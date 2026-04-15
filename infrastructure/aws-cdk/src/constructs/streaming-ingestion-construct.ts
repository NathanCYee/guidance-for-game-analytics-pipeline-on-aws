/**
 * Copyright 2023 Amazon.com, Inc. and its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *   http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

import * as cdk from "aws-cdk-lib";
import * as iam from "aws-cdk-lib/aws-iam";
import * as kms from "aws-cdk-lib/aws-kms";
import * as logs from "aws-cdk-lib/aws-logs";
import * as kinesisFirehose from "aws-cdk-lib/aws-kinesisfirehose";
import { Construct } from "constructs";
import { GameAnalyticsPipelineConfig } from "../helpers/config-types";
import { MSKConstruct } from "./msk-construct";

/* eslint-disable @typescript-eslint/no-empty-interface */
export interface StreamingIngestionConstructProps extends cdk.StackProps {
  applicationsTable: cdk.aws_dynamodb.TableV2;
  gamesEventsStream?: cdk.aws_kinesis.Stream;
  analyticsBucket: cdk.aws_s3.Bucket;
  rawEventsTable?: cdk.aws_glue.CfnTable;
  gameEventsDatabase?: cdk.aws_glue.CfnDatabase;
  eventsProcessingFunction: cdk.aws_lambda.Function;
  config: GameAnalyticsPipelineConfig;
  /** MSK construct — required when INGEST_MODE is KAFKA */
  mskConstruct?: MSKConstruct;
  /** Federated Glue catalog ARN for S3 Tables (from S3TablesConstruct.catalogArn) */
  s3TablesCatalogArn?: string;
}

const defaultProps: Partial<StreamingIngestionConstructProps> = {};

/**
 * Deploys the StreamingIngestion construct
 */
export class StreamingIngestionConstruct extends Construct {
  public readonly gameEventsFirehose: kinesisFirehose.CfnDeliveryStream;

  constructor(
    parent: Construct,
    name: string,
    props: StreamingIngestionConstructProps
  ) {
    super(parent, name);

    /* eslint-disable @typescript-eslint/no-unused-vars */
    props = { ...defaultProps, ...props };

    const enableS3Tables = props.config.ENABLE_S3_TABLES;

    // Create firehose log groups and streams
    const firehoseLogGroup = new logs.LogGroup(this, "firehose-log-group", {
      retention: props.config.CLOUDWATCH_RETENTION_DAYS,
    });

    const kmsKey = new kms.Key(this, "FirehoseKMSKey", {
      enableKeyRotation: true,
    });

    const firehoseS3DeliveryLogStream = new logs.LogStream(
      this,
      "firehose-s3-delivery-log-stream",
      { logGroup: firehoseLogGroup }
    );

    const firehoseBackupDeliveryLogStream = new logs.LogStream(
      this,
      "firehose-backup-delivery-log-stream",
      { logGroup: firehoseLogGroup }
    );

    // --- IAM policies ---
    // Base statements: S3, Lambda, Logs, KMS
    const baseStatements: iam.PolicyStatement[] = [
      new iam.PolicyStatement({
        actions: [
          "s3:AbortMultipartUpload",
          "s3:GetBucketLocation",
          "s3:GetObject",
          "s3:ListBucket",
          "s3:ListBucketMultipartUploads",
          "s3:PutObject",
        ],
        effect: iam.Effect.ALLOW,
        resources: [
          props.analyticsBucket.arnForObjects("*"),
          props.analyticsBucket.bucketArn,
        ],
      }),
      new iam.PolicyStatement({
        actions: [
          "lambda:InvokeFunction",
          "lambda:GetFunctionConfiguration",
        ],
        effect: iam.Effect.ALLOW,
        resources: [props.eventsProcessingFunction.functionArn],
      }),
      new iam.PolicyStatement({
        actions: ["logs:PutLogEvents"],
        effect: iam.Effect.ALLOW,
        resources: [firehoseLogGroup.logGroupArn],
      }),
      new iam.PolicyStatement({
        sid: "KMSPermission",
        actions: ["kms:GenerateDataKey", "kms:Decrypt"],
        effect: iam.Effect.ALLOW,
        resources: [kmsKey.keyArn],
      }),
    ];

    // Conditional catalog permissions: Glue standard catalog vs S3 Tables
    const catalogStatements: iam.PolicyStatement[] = enableS3Tables
      ? [
          new iam.PolicyStatement({
            sid: "S3TablesAccessPermission",
            actions: [
              "s3tables:ListTables",
              "s3tables:GetNamespace",
              "s3tables:ListNamespaces",
              "s3tables:GetTable",
              "s3tables:GetTableData",
              "s3tables:GetTableMetadataLocation",
              "s3tables:UpdateTableMetadataLocation",
              "s3tables:PutTableData",
            ],
            effect: iam.Effect.ALLOW,
            resources: [
              `arn:${cdk.Aws.PARTITION}:s3tables:${cdk.Aws.REGION}:${cdk.Aws.ACCOUNT_ID}:bucket/*`,
              `arn:${cdk.Aws.PARTITION}:s3tables:${cdk.Aws.REGION}:${cdk.Aws.ACCOUNT_ID}:bucket/*/table/*`,
            ],
          }),
          new iam.PolicyStatement({
            sid: "S3TableBucketAccessPermission",
            actions: ["s3tables:GetTableBucket"],
            effect: iam.Effect.ALLOW,
            resources: [
              `arn:${cdk.Aws.PARTITION}:s3tables:${cdk.Aws.REGION}:${cdk.Aws.ACCOUNT_ID}:bucket/*`,
            ],
          }),
          new iam.PolicyStatement({
            sid: "GlueCatalogAccessForS3Tables",
            actions: [
              "glue:GetDatabase",
              "glue:GetDatabases",
              "glue:GetTable",
              "glue:GetTables",
              "glue:UpdateTable",
            ],
            effect: iam.Effect.ALLOW,
            resources: [
              `arn:${cdk.Aws.PARTITION}:glue:${cdk.Aws.REGION}:${cdk.Aws.ACCOUNT_ID}:catalog`,
              `arn:${cdk.Aws.PARTITION}:glue:${cdk.Aws.REGION}:${cdk.Aws.ACCOUNT_ID}:catalog/s3tablescatalog`,
              `arn:${cdk.Aws.PARTITION}:glue:${cdk.Aws.REGION}:${cdk.Aws.ACCOUNT_ID}:catalog/s3tablescatalog/*`,
              `arn:${cdk.Aws.PARTITION}:glue:${cdk.Aws.REGION}:${cdk.Aws.ACCOUNT_ID}:database/*`,
              `arn:${cdk.Aws.PARTITION}:glue:${cdk.Aws.REGION}:${cdk.Aws.ACCOUNT_ID}:table/*/*`,
            ],
          }),
        ]
      : props.gameEventsDatabase
        ? [
            new iam.PolicyStatement({
              actions: [
                "glue:GetTable",
                "glue:GetTableVersion",
                "glue:GetTableVersions",
                "glue:GetSchema",
                "glue:GetSchemaVersion",
                "glue:CreateTable",
                "glue:UpdateTable",
                "glue:StartTransaction",
                "glue:CommitTransaction",
                "glue:GetDatabase",
              ],
              effect: iam.Effect.ALLOW,
              resources: [
                `arn:${cdk.Aws.PARTITION}:glue:${cdk.Aws.REGION}:${cdk.Aws.ACCOUNT_ID}:table/${props.gameEventsDatabase.ref}/*`,
                `arn:${cdk.Aws.PARTITION}:glue:${cdk.Aws.REGION}:${cdk.Aws.ACCOUNT_ID}:database/${props.gameEventsDatabase.ref}`,
                `arn:${cdk.Aws.PARTITION}:glue:${cdk.Aws.REGION}:${cdk.Aws.ACCOUNT_ID}:catalog`,
                `arn:${cdk.Aws.PARTITION}:glue:${cdk.Aws.REGION}:${cdk.Aws.ACCOUNT_ID}:registry/*`,
                `arn:${cdk.Aws.PARTITION}:glue:${cdk.Aws.REGION}:${cdk.Aws.ACCOUNT_ID}:schema/*`,
              ],
            }),
          ]
        : [];

    // Kinesis Data Streams source permissions
    const kinesisStatements: iam.PolicyStatement[] =
      props.config.INGEST_MODE === "KINESIS_DATA_STREAMS" && props.gamesEventsStream
        ? [
            new iam.PolicyStatement({
              actions: [
                "kinesis:DescribeStream",
                "kinesis:GetShardIterator",
                "kinesis:GetRecords",
                "kinesis:ListShards",
              ],
              effect: iam.Effect.ALLOW,
              resources: [props.gamesEventsStream.streamArn],
            }),
          ]
        : [];

    // MSK (Kafka) source permissions
    const mskStatements: iam.PolicyStatement[] =
      props.config.INGEST_MODE === "KAFKA" && props.mskConstruct
        ? [
            new iam.PolicyStatement({
              sid: "MSKClusterPermission",
              actions: [
                "kafka:GetBootstrapBrokers",
                "kafka:DescribeCluster",
                "kafka:DescribeClusterV2",
                "kafka-cluster:Connect",
              ],
              effect: iam.Effect.ALLOW,
              resources: [props.mskConstruct.cluster.attrArn],
            }),
            new iam.PolicyStatement({
              sid: "MSKTopicPermission",
              actions: [
                "kafka-cluster:DescribeTopic",
                "kafka-cluster:DescribeTopicDynamicConfiguration",
                "kafka-cluster:ReadData",
              ],
              effect: iam.Effect.ALLOW,
              resources: [props.mskConstruct.topic.attrTopicArn],
            }),
            new iam.PolicyStatement({
              sid: "MSKGroupPermission",
              actions: ["kafka-cluster:DescribeGroup"],
              effect: iam.Effect.ALLOW,
              // Group ARN uses wildcard for consumer group names
              resources: [
                `arn:${cdk.Aws.PARTITION}:kafka:${cdk.Aws.REGION}:${cdk.Aws.ACCOUNT_ID}:group/${props.mskConstruct.cluster.clusterName}/*`,
              ],
            }),
          ]
        : [];

    // Role for firehose
    const gamesEventsFirehoseRole = new iam.Role(
      this,
      "games-events-firehose-role",
      {
        assumedBy: new iam.CompositePrincipal(
          new iam.ServicePrincipal("firehose.amazonaws.com"),
          new iam.ServicePrincipal("glue.amazonaws.com")
        ),
        inlinePolicies: {
          firehose_delivery_policy: new iam.PolicyDocument({
            statements: [
              ...baseStatements,
              ...catalogStatements,
              ...kinesisStatements,
              ...mskStatements,
            ],
          }),
        },
      }
    );

    // --- Source configuration ---
    const s3TimestampPrefix =
      "year=!{timestamp:YYYY}/month=!{timestamp:MM}/day=!{timestamp:dd}";

    var firehoseIngestDeliveryStreamType: string;
    var firehoseSourceConfiguration: Record<string, unknown> = {};

    if (props.config.INGEST_MODE === "KINESIS_DATA_STREAMS" && props.gamesEventsStream) {
      firehoseIngestDeliveryStreamType = "KinesisStreamAsSource";
      firehoseSourceConfiguration = {
        kinesisStreamSourceConfiguration: {
          kinesisStreamArn: props.gamesEventsStream.streamArn,
          roleArn: gamesEventsFirehoseRole.roleArn,
        },
      };
    } else if (props.config.INGEST_MODE === "KAFKA" && props.mskConstruct) {
      firehoseIngestDeliveryStreamType = "MSKAsSource";
      firehoseSourceConfiguration = {
        mskSourceConfiguration: {
          mskClusterArn: props.mskConstruct.cluster.attrArn,
          topicName: props.mskConstruct.topic.topicName,
          authenticationConfiguration: {
            connectivity: "PRIVATE",
            roleArn: gamesEventsFirehoseRole.roleArn,
          },
        },
      };
    } else {
      firehoseIngestDeliveryStreamType = "DirectPut";
      firehoseSourceConfiguration = {
        directPutSourceConfiguration: {
          throughputHintInMBs: 1,
        },
      };
    }

    // --- Destination configuration ---
    // Resolve catalog ARN: S3 Tables federated catalog or standard Glue catalog
    const catalogArn = enableS3Tables && props.s3TablesCatalogArn
      ? props.s3TablesCatalogArn
      : `arn:aws:glue:${cdk.Aws.REGION}:${cdk.Aws.ACCOUNT_ID}:catalog`;

    var firehoseDestinationConfiguration = props.config.ENABLE_APACHE_ICEBERG_SUPPORT
      ? {
          icebergDestinationConfiguration: {
            catalogConfiguration: { catalogArn },
            roleArn: gamesEventsFirehoseRole.roleArn,
            appendOnly: true,
            s3Configuration: {
              bucketArn: props.analyticsBucket.bucketArn,
              roleArn: gamesEventsFirehoseRole.roleArn,
              kmsKeyArn: kmsKey.keyArn,
              bufferingHints: {
                intervalInSeconds: props.config.DEV_MODE ? 60 : 900,
                sizeInMBs: 128,
              },
            },
            destinationTableConfigurationList: [
              {
                destinationDatabaseName: props.config.EVENTS_DATABASE,
                destinationTableName: props.config.RAW_EVENTS_TABLE,
                s3ErrorOutputPrefix: "firehose-errors/!{firehose:error-output-type}/",
                uniqueKeys: ["event_id"],
              },
            ],
            cloudWatchLoggingOptions: {
              enabled: true,
              logGroupName: firehoseLogGroup.logGroupName,
              logStreamName: firehoseS3DeliveryLogStream.logStreamName,
            },
            processingConfiguration: {
              enabled: true,
              processors: [
                {
                  type: "Lambda",
                  parameters: [
                    { parameterName: "LambdaArn", parameterValue: props.eventsProcessingFunction.functionArn },
                    { parameterName: "BufferIntervalInSeconds", parameterValue: "60" },
                    { parameterName: "BufferSizeInMBs", parameterValue: "3" },
                    { parameterName: "NumberOfRetries", parameterValue: "3" },
                  ],
                },
              ],
            },
            s3BackupMode: "FailedDataOnly",
          },
        }
      : {
          extendedS3DestinationConfiguration: {
            bucketArn: props.analyticsBucket.bucketArn,
            bufferingHints: {
              intervalInSeconds: props.config.DEV_MODE ? 60 : 900,
              sizeInMBs: 128,
            },
            prefix: `${props.config.RAW_EVENTS_PREFIX}/year=!{partitionKeyFromQuery:year}/month=!{partitionKeyFromQuery:month}/day=!{partitionKeyFromQuery:day}/`,
            errorOutputPrefix: "firehose-errors/!{firehose:error-output-type}/",
            compressionFormat: "UNCOMPRESSED",
            roleArn: gamesEventsFirehoseRole.roleArn,
            kmsKeyArn: kmsKey.keyArn,
            dynamicPartitioningConfiguration: { enabled: true },
            processingConfiguration: {
              enabled: true,
              processors: [
                {
                  type: "Lambda",
                  parameters: [
                    { parameterName: "LambdaArn", parameterValue: props.eventsProcessingFunction.functionArn },
                    { parameterName: "BufferIntervalInSeconds", parameterValue: "60" },
                    { parameterName: "BufferSizeInMBs", parameterValue: "3" },
                    { parameterName: "NumberOfRetries", parameterValue: "3" },
                  ],
                },
                {
                  type: "MetadataExtraction",
                  parameters: [
                    {
                      parameterName: "MetadataExtractionQuery",
                      parameterValue:
                        '{year: .event_timestamp| strftime("%Y"), month: .event_timestamp| strftime("%m"), day: .event_timestamp| strftime("%d")}',
                    },
                    { parameterName: "JsonParsingEngine", parameterValue: "JQ-1.6" },
                  ],
                },
              ],
            },
            cloudWatchLoggingOptions: {
              enabled: true,
              logGroupName: firehoseLogGroup.logGroupName,
              logStreamName: firehoseS3DeliveryLogStream.logStreamName,
            },
            s3BackupMode: props.config.S3_BACKUP_MODE ? "Enabled" : "Disabled",
            s3BackupConfiguration: {
              bucketArn: props.analyticsBucket.bucketArn,
              cloudWatchLoggingOptions: {
                enabled: true,
                logGroupName: firehoseLogGroup.logGroupName,
                logStreamName: firehoseBackupDeliveryLogStream.logStreamName,
              },
              compressionFormat: "GZIP",
              bufferingHints: { intervalInSeconds: 900, sizeInMBs: 128 },
              prefix: `FirehoseS3SourceRecordBackup/${s3TimestampPrefix}/`,
              errorOutputPrefix: `FirehoseS3SourceRecordBackup/firehose-errors/${s3TimestampPrefix}/!{firehose:error-output-type}/`,
              roleArn: gamesEventsFirehoseRole.roleArn,
            },
            dataFormatConversionConfiguration: {
              enabled: true,
              inputFormatConfiguration: {
                deserializer: {
                  openXJsonSerDe: {
                    caseInsensitive: true,
                    convertDotsInJsonKeysToUnderscores: false,
                  },
                },
              },
              outputFormatConfiguration: {
                serializer: {
                  parquetSerDe: { compression: "SNAPPY" },
                },
              },
              schemaConfiguration: {
                catalogId: cdk.Aws.ACCOUNT_ID,
                roleArn: gamesEventsFirehoseRole.roleArn,
                databaseName: props.gameEventsDatabase?.ref ?? props.config.EVENTS_DATABASE,
                tableName: props.rawEventsTable?.ref ?? props.config.RAW_EVENTS_TABLE,
                region: cdk.Aws.REGION,
                versionId: "LATEST",
              },
            },
          },
        };

    var firehoseSettings: kinesisFirehose.CfnDeliveryStreamProps = {
      deliveryStreamType: firehoseIngestDeliveryStreamType,
      ...firehoseSourceConfiguration,
      ...firehoseDestinationConfiguration,
    };

    // Firehose to manage stream input, process data with Lambda, and send it to s3
    const gameEventsFirehose = new kinesisFirehose.CfnDeliveryStream(
      this,
      "game-events-firehose",
      firehoseSettings
    );

    gameEventsFirehose.node.addDependency(gamesEventsFirehoseRole);

    this.gameEventsFirehose = gameEventsFirehose;
  }
}
