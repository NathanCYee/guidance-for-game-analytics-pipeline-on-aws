/**
 * Copyright 2023 Amazon.com, Inc. and its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the 'License').
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *   http://aws.amazon.com/asl/
 *
 * or in the 'license' file accompanying this file. This file is distributed
 * on an 'AS IS' BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
import { GameAnalyticsPipelineConfig } from "../helpers/config-types";
import * as cdk from "aws-cdk-lib";
import { Construct } from "constructs";
import * as s3 from "aws-cdk-lib/aws-s3";
import { aws_glue as glue } from "aws-cdk-lib";
import * as iam from "aws-cdk-lib/aws-iam";

import * as glueCfn from "aws-cdk-lib/aws-glue";
import * as s3tables from "aws-cdk-lib/aws-s3tables";
import * as sns from "aws-cdk-lib/aws-sns";
import * as athena from "aws-cdk-lib/aws-athena";

/**
 * S3 Tables sub-construct
 *
 * Creates an S3 Tables table bucket, namespace, and Iceberg table for game events.
 * Used as an alternative to Glue catalog resources when ENABLE_S3_TABLES is true.
 */
export interface S3TablesConstructProps {
  config: GameAnalyticsPipelineConfig;
}

export class S3TablesConstruct extends Construct {
  public readonly tableBucket: s3tables.CfnTableBucket;
  public readonly namespace: s3tables.CfnNamespace;
  public readonly eventDataTable: s3tables.CfnTable;
  public readonly catalogArn: string;

  constructor(parent: Construct, name: string, props: S3TablesConstructProps) {
    super(parent, name);

    // S3 Tables table bucket
    this.tableBucket = new s3tables.CfnTableBucket(this, "GameAnalyticsBucket", {
      tableBucketName: props.config.WORKLOAD_NAME,
      encryptionConfiguration: {
        sseAlgorithm: "AES256",
      },
    });

    // Namespace (equivalent to a database)
    this.namespace = new s3tables.CfnNamespace(this, "GameAnalyticsNamespace", {
      namespace: props.config.EVENTS_DATABASE,
      tableBucketArn: this.tableBucket.attrTableBucketArn,
    });

    // Iceberg table for raw game events
    this.eventDataTable = new s3tables.CfnTable(this, "EventDataTable", {
      tableName: props.config.RAW_EVENTS_TABLE,
      namespace: props.config.EVENTS_DATABASE,
      tableBucketArn: this.tableBucket.attrTableBucketArn,
      openTableFormat: "ICEBERG",
      icebergMetadata: {
        icebergSchema: {
          schemaFieldList: [
            { id: 0, name: "event_id", type: "string", required: true },
            { id: 1, name: "event_type", type: "string", required: true },
            { id: 2, name: "event_name", type: "string", required: false },
            { id: 3, name: "event_version", type: "string", required: true },
            { id: 4, name: "event_timestamp", type: "timestamp", required: true },
            { id: 5, name: "app_version", type: "string", required: true },
            { id: 6, name: "application_id", type: "string", required: true },
            { id: 7, name: "application_name", type: "string", required: false },
            { id: 8, name: "event_data", type: "string", required: false },
            { id: 9, name: "metadata", type: "string", required: true },
          ],
        },
        icebergPartitionSpec: {
          fields: [
            { name: "application_id", sourceId: 6, transform: "identity" },
            { name: "event_timestamp", sourceId: 4, transform: "day" },
          ],
        },
      },
    });
    this.eventDataTable.addDependency(this.namespace);

    // Federated Glue catalog ARN for S3 Tables
    this.catalogArn = `arn:aws:glue:${cdk.Aws.REGION}:${cdk.Aws.ACCOUNT_ID}:catalog/s3tablescatalog/${props.config.WORKLOAD_NAME}`;
  }
}

/* eslint-disable @typescript-eslint/no-empty-interface */
export interface DataLakeConstructProps extends cdk.StackProps {
  analyticsBucket: s3.Bucket;
  config: GameAnalyticsPipelineConfig;
  notificationsTopic: sns.Topic;
}

const defaultProps: Partial<DataLakeConstructProps> = {};

/**
 * Deploys the DataLake construct
 *
 * Creates Glue to turn analytics s3 bucket into Datalake. Creates Jobs that can be used to process s3 data for Athena.
 */
export class DataLakeConstruct extends Construct {
  public readonly gameEventsDatabase?: glueCfn.CfnDatabase;
  public readonly rawEventsTable?: glueCfn.CfnTable;
  public readonly gameAnalyticsWorkgroup: athena.CfnWorkGroup;
  public readonly s3TablesConstruct?: S3TablesConstruct;

  constructor(parent: Construct, name: string, props: DataLakeConstructProps) {
    super(parent, name);

    /* eslint-disable @typescript-eslint/no-unused-vars */
    props = { ...defaultProps, ...props };

    // ---- S3 Tables ---- //
    // When S3 Tables support is enabled, create S3 Tables resources instead of Glue catalog
    if (props.config.ENABLE_S3_TABLES) {
      this.s3TablesConstruct = new S3TablesConstruct(this, "S3Tables", {
        config: props.config,
      });
    }

    // Glue Database (only when S3 Tables is disabled)
    const gameEventsDatabase = props.config.ENABLE_S3_TABLES
      ? undefined
      : new glueCfn.CfnDatabase(
      this,
      "GameEventDatabase",
      {
        catalogId: cdk.Aws.ACCOUNT_ID,
        databaseInput: {
          description: `Database for game analytics events for workload: ${props.config.WORKLOAD_NAME}`,
          locationUri: props.analyticsBucket.s3UrlForObject(),
          name: props.config.EVENTS_DATABASE
        },
      }
    );


    // ---- Athena ---- //
    // Define the resources for the `GameAnalyticsWorkgroup` Athena workgroup
    const gameAnalyticsWorkgroup = new athena.CfnWorkGroup(
      this,
      "GameAnalyticsWorkgroup",
      {
        name: `${props.config.WORKLOAD_NAME}-Workgroup`,
        description: "Default workgroup for the solution workload",
        recursiveDeleteOption: true, // delete the associated queries when stack is deleted
        state: "ENABLED",
        workGroupConfiguration: {
          publishCloudWatchMetricsEnabled: true,
          resultConfiguration: {
            encryptionConfiguration: {
              encryptionOption: "SSE_S3",
            },
            outputLocation: `s3://${props.analyticsBucket.bucketName}/athena_query_results/`,
          },
        },
      }
    );

    /* 
    // Enables the recommended encryption settings for the account Glue Data Catalog
    // Applies to all databases and tables in the account; uncomment to apply
    // Do not apply this setting if the account already has data encryption enabled to avoid conflicts
    const cfnDataCatalogEncryptionSettings =
      new glue.CfnDataCatalogEncryptionSettings(
        this,
        "DataCatalogEncryptionSettings",
        {
          catalogId: cdk.Aws.ACCOUNT_ID,
          dataCatalogEncryptionSettings: {
            connectionPasswordEncryption: {
              returnConnectionPasswordEncrypted: true,
            },
            encryptionAtRest: {
              catalogEncryptionMode: "SSE-KMS",
            },
          },
        }
      );
    */

    // Glue table for raw events that come in from stream (only when S3 Tables is disabled)
    const rawEventsTable = !gameEventsDatabase
      ? undefined
      : new glueCfn.CfnTable(this, "GameRawEventsTable", {
      catalogId: cdk.Aws.ACCOUNT_ID,
      databaseName: gameEventsDatabase.ref,
      ...(props.config.ENABLE_APACHE_ICEBERG_SUPPORT
        ? {
          tableInput: {
            name: props.config.RAW_EVENTS_TABLE.toLowerCase(),
            description: `Stores raw event data from the game analytics pipeline for stack ${cdk.Aws.STACK_NAME}`,
            storageDescriptor: {
              columns: [
                { name: "event_id", type: "string" },
                { name: "event_type", type: "string" },
                { name: "event_name", type: "string" },
                { name: "event_version", type: "string" },
                { name: "event_timestamp", type: "timestamp" },
                { name: "app_version", type: "string" },
                { name: "application_id", type: "string" },
                { name: "application_name", type: "string" },
                { name: "event_data", type: "string" },
                { name: "metadata", type: "string" },
              ],
              location: props.analyticsBucket.s3UrlForObject(props.config.RAW_EVENTS_TABLE.toLowerCase()),
              storedAsSubDirectories: false,
              parameters: {
                classification: "parquet",
                compressionType: "none",
                typeOfData: "file",
              }
            },
            tableType: "EXTERNAL_TABLE",
          },
          openTableFormatInput: {
            icebergInput: {
              metadataOperation: "CREATE",
              version: "2",
            },
          },
        }
        : {
          tableInput: {
            name: props.config.RAW_EVENTS_TABLE,
            description: `Stores raw event data from the game analytics pipeline for stack ${cdk.Aws.STACK_NAME}`,
            tableType: "EXTERNAL_TABLE",
            partitionKeys: [
              { name: "year", type: "string" },
              { name: "month", type: "string" },
              { name: "day", type: "string" },
            ],
            parameters: {
              classification: "parquet",
              compressionType: "none",
              typeOfData: "file",
              enablePartitionFiltering: "true",
            },
            storageDescriptor: {
              outputFormat:
                "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
              inputFormat:
                "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
              compressed: false,
              numberOfBuckets: -1,
              serdeInfo: {
                serializationLibrary:
                  "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                parameters: {
                  "serialization.format": "1",
                },
              },
              bucketColumns: [],
              sortColumns: [],
              storedAsSubDirectories: false,
              location: props.analyticsBucket.s3UrlForObject(props.config.RAW_EVENTS_PREFIX),
              columns: [
                { name: "event_id", type: "string" },
                { name: "event_type", type: "string" },
                { name: "event_name", type: "string" },
                { name: "event_version", type: "string" },
                { name: "event_timestamp", type: "bigint" },
                { name: "app_version", type: "string" },
                { name: "application_id", type: "string" },
                { name: "application_name", type: "string" },
                { name: "event_data", type: "string" },
                { name: "metadata", type: "string" },
              ],
            }
          },
        }),
    });
    if (rawEventsTable && gameEventsDatabase) {
      rawEventsTable.addDependency(gameEventsDatabase);
    }

    /* The following sets up automatic Glue table optimization for Apache Iceberg */
    if (props.config.ENABLE_APACHE_ICEBERG_SUPPORT && !props.config.ENABLE_S3_TABLES && gameEventsDatabase && rawEventsTable) {
      const glueOptimizationServiceRole = new iam.Role(this, "GlueOptimizationServiceRole", {
        assumedBy: new iam.ServicePrincipal("glue.amazonaws.com"),
        path: "/",
        inlinePolicies: {
          'glue_optimization_service_role_policy': new iam.PolicyDocument({
            statements: [
              new iam.PolicyStatement({
                effect: iam.Effect.ALLOW,
                actions: [
                  "s3:PutObject",
                  "s3:GetObject",
                  "s3:DeleteObject"
                ],
                resources: [
                  `${props.analyticsBucket.bucketArn}/*`,
                ],
              }),
              new iam.PolicyStatement({
                effect: iam.Effect.ALLOW,
                actions: ["s3:ListBucket"],
                resources: [
                  props.analyticsBucket.bucketArn
                ],
              }),
              new iam.PolicyStatement({
                effect: iam.Effect.ALLOW,
                actions: [
                  "glue:UpdateTable",
                  "glue:GetTable"
                ],
                resources: [
                  `arn:aws:glue:${cdk.Aws.REGION}:${cdk.Aws.ACCOUNT_ID}:table/${gameEventsDatabase.ref}/${rawEventsTable.ref}`,
                  `arn:aws:glue:${cdk.Aws.REGION}:${cdk.Aws.ACCOUNT_ID}:database/${gameEventsDatabase.ref}`,
                  `arn:aws:glue:${cdk.Aws.REGION}:${cdk.Aws.ACCOUNT_ID}:catalog`
                ],
              }),
              new iam.PolicyStatement({
                effect: iam.Effect.ALLOW,
                actions: [
                  "logs:CreateLogGroup",
                  "logs:CreateLogStream",
                  "logs:PutLogEvents"
                ],
                resources: [
                  `arn:aws:logs:${cdk.Aws.REGION}:${cdk.Aws.ACCOUNT_ID}:log-group:/aws-glue/iceberg-compaction/logs:*`,
                  `arn:aws:logs:${cdk.Aws.REGION}:${cdk.Aws.ACCOUNT_ID}:log-group:/aws-glue/iceberg-retention/logs:*`,
                  `arn:aws:logs:${cdk.Aws.REGION}:${cdk.Aws.ACCOUNT_ID}:log-group:/aws-glue/iceberg-orphan-file-deletion/logs:*`
                ],
              })
            ]
          })
        }
      });

      glueOptimizationServiceRole.addToPolicy(
        new iam.PolicyStatement({
          sid: "NetworkandS3Access",
          effect: iam.Effect.ALLOW,
          actions: [
            "s3:ListAllMyBuckets",
            "s3:GetBucketAcl",
            "ec2:DescribeVpcEndpoints",
            "ec2:DescribeRouteTables",
            "ec2:CreateNetworkInterface",
            "ec2:DeleteNetworkInterface",
            "ec2:DescribeNetworkInterfaces",
            "ec2:DescribeSecurityGroups",
            "ec2:DescribeSubnets",
            "ec2:DescribeVpcAttribute",
            "iam:ListRolePolicies",
            "iam:GetRole",
            "iam:GetRolePolicy",
            "cloudwatch:PutMetricData"
          ],
          resources: ["*"],
        })
      );
      glueOptimizationServiceRole.addToPolicy(
        new iam.PolicyStatement({
          sid: "CreateBucketAccess",
          effect: iam.Effect.ALLOW,
          actions: [
            "s3:CreateBucket"
          ],
          resources: ["arn:aws:s3:::aws-glue-*"],
        })
      );
      glueOptimizationServiceRole.addToPolicy(
        new iam.PolicyStatement({
          sid: "S3GlueAccess",
          effect: iam.Effect.ALLOW,
          actions: [
            "s3:GetObject",
            "s3:PutObject",
            "s3:DeleteObject"
          ],
          resources : [
            "arn:aws:s3:::aws-glue-*/*",
            "arn:aws:s3:::*/*aws-glue-*/*"
          ]
        })
      );
      glueOptimizationServiceRole.addToPolicy(
        new iam.PolicyStatement({
          sid: "S3GetObjectAccess",
          effect: iam.Effect.ALLOW,
          actions: [
            "s3:GetObject"
          ],
          resources : [
            "arn:aws:s3:::crawler-public*",
            "arn:aws:s3:::aws-glue-*"
          ]
        })
      );
      glueOptimizationServiceRole.addToPolicy(
        new iam.PolicyStatement({
          sid: "LogGroupAccess",
          effect: iam.Effect.ALLOW,
          actions: [
            "logs:CreateLogGroup",
            "logs:CreateLogStream",
            "logs:PutLogEvents"
          ],
          resources : [
            "arn:aws:logs:*:*:*:/aws-glue/*"
          ]
        })
      );
      glueOptimizationServiceRole.addToPolicy(
        new iam.PolicyStatement({
          sid: "TagAccess",
          effect: iam.Effect.ALLOW,
          actions: [
            "ec2:CreateTags",
            "ec2:DeleteTags"
          ],
          conditions: {
            StringEquals: {
              "aws:TagKeys": "aws-glue-service-resource",
            }
          },
          resources : [
            "arn:aws:ec2:*:*:network-interface/*",
            "arn:aws:ec2:*:*:security-group/*",
            "arn:aws:ec2:*:*:instance/*"
          ]
        })
      );

      const rawEventsCompactionOptimizer = new glue.CfnTableOptimizer(this, "RawEventsCompactionOptimizer", {
        catalogId: cdk.Aws.ACCOUNT_ID,
        databaseName: gameEventsDatabase.ref,
        tableName: rawEventsTable.ref,
        tableOptimizerConfiguration: {
          enabled: true,
          roleArn: glueOptimizationServiceRole.roleArn,
        },
        type: 'compaction'
      })

      const rawEventsRetentionOptimizer = new glue.CfnTableOptimizer(this, "RawEventsRetentionOptimizer", {
        catalogId: cdk.Aws.ACCOUNT_ID,
        databaseName: gameEventsDatabase.ref,
        tableName: rawEventsTable.ref,
        tableOptimizerConfiguration: {
          enabled: true,
          roleArn: glueOptimizationServiceRole.roleArn,
        },
        type: 'retention'
      })
      const rawEventsOrphanOptimizer = new glue.CfnTableOptimizer(this, "RawEventsOrphanOptimizer", {
        catalogId: cdk.Aws.ACCOUNT_ID,
        databaseName: gameEventsDatabase.ref,
        tableName: rawEventsTable.ref,
        tableOptimizerConfiguration: {
          enabled: true,
          roleArn: glueOptimizationServiceRole.roleArn
        },
        type: 'orphan_file_deletion'
      })
    }

    this.gameEventsDatabase = gameEventsDatabase;
    this.rawEventsTable = rawEventsTable;
    this.gameAnalyticsWorkgroup = gameAnalyticsWorkgroup;
  }
}
