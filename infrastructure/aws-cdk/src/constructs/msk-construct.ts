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

import * as cdk from "aws-cdk-lib";
import * as ec2 from "aws-cdk-lib/aws-ec2";
import * as iam from "aws-cdk-lib/aws-iam";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as logs from "aws-cdk-lib/aws-logs";
import * as msk from "aws-cdk-lib/aws-msk";

import * as path from "path";
import { Construct } from "constructs";
import { NodejsFunction } from "aws-cdk-lib/aws-lambda-nodejs";
import { GameAnalyticsPipelineConfig } from "../helpers/config-types";
import { VpcConstruct } from "./vpc-construct";

/* eslint-disable @typescript-eslint/no-empty-interface */
export interface MSKConstructProps extends cdk.StackProps {
  config: GameAnalyticsPipelineConfig;
  vpcConstruct: VpcConstruct;
  clusterName?: string;
  topicName?: string;
  partitionCount?: number;
}

const defaultProps = {
  topicName: "game_events",
  partitionCount: 100,
};

/**
 * Deploys the MSK construct
 *
 * Creates an MSK cluster with IAM SASL authentication, a Kafka topic for game events,
 * a CloudWatch log group for cluster logs, a cluster resource policy enabling Firehose access,
 * and a VPC-attached Lambda function for producing events into the topic.
 */
export class MSKConstruct extends Construct {
  public readonly cluster: msk.CfnCluster;
  public readonly topic: msk.CfnTopic;
  public readonly securityGroup: ec2.SecurityGroup;
  public readonly clusterLogGroup: logs.LogGroup;
  public readonly eventIngestionFunction: lambda.Function;

  constructor(parent: Construct, name: string, props: MSKConstructProps) {
    super(parent, name);

    /* eslint-disable @typescript-eslint/no-unused-vars */
    const mergedProps = { ...defaultProps, ...props };
    const vpc = mergedProps.vpcConstruct.vpc;

    const workloadName = mergedProps.config.WORKLOAD_NAME;
    const resolvedClusterName =
      mergedProps.clusterName ?? `${workloadName}-cluster`.replace(/_/g, "-");

    // Security group allowing all inbound/outbound within VPC
    this.securityGroup = new ec2.SecurityGroup(this, "MskSecurityGroup", {
      vpc,
      description: "Allow inbound from the VPC",
      allowAllOutbound: true,
    });
    this.securityGroup.addIngressRule(
      ec2.Peer.anyIpv4(),
      ec2.Port.allTraffic(),
      "Allow all inbound"
    );

    // CloudWatch log group for MSK cluster logs
    this.clusterLogGroup = new logs.LogGroup(this, "ClusterLogs", {
      logGroupName: `${workloadName}-cluster-logs`.replace(/_/g, "-"),
      retention: mergedProps.config.CLOUDWATCH_RETENTION_DAYS,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    // MSK cluster with IAM SASL authentication
    this.cluster = new msk.CfnCluster(this, "GameAnalyticsCluster", {
      clusterName: resolvedClusterName,
      kafkaVersion: "3.8.x",
      numberOfBrokerNodes: 3,
      enhancedMonitoring: "PER_TOPIC_PER_PARTITION",
      brokerNodeGroupInfo: {
        instanceType: mergedProps.config.MSK_CLUSTER_INSTANCE_TYPE,
        clientSubnets: vpc.privateSubnets.map((s: ec2.ISubnet) => s.subnetId),
        securityGroups: [this.securityGroup.securityGroupId],
        connectivityInfo: {
          vpcConnectivity: {
            clientAuthentication: {
              sasl: {
                iam: { enabled: true },
              },
            },
          },
        },
      },
      clientAuthentication: {
        sasl: {
          iam: { enabled: true },
        },
      },
    });

    // Cluster resource policy enabling Firehose and account-level access
    new msk.CfnClusterPolicy(this, "EnableFirehosePolicy", {
      clusterArn: this.cluster.attrArn,
      policy: new iam.PolicyDocument({
        statements: [
          new iam.PolicyStatement({
            effect: iam.Effect.ALLOW,
            principals: [
              new iam.ServicePrincipal("firehose.amazonaws.com"),
            ],
            actions: [
              "kafka:CreateVpcConnection",
              "kafka:GetBootstrapBrokers",
              "kafka:DescribeCluster",
              "kafka:DescribeClusterV2",
            ],
            resources: [this.cluster.attrArn],
          }),
          new iam.PolicyStatement({
            effect: iam.Effect.ALLOW,
            principals: [new iam.AccountRootPrincipal()],
            actions: [
              "kafka:CreateVpcConnection",
              "kafka:GetBootstrapBrokers",
              "kafka:DescribeCluster",
              "kafka:DescribeClusterV2",
            ],
            resources: [this.cluster.attrArn],
          }),
        ],
      }),
    });

    // MSK topic for game events
    this.topic = new msk.CfnTopic(this, "GameEventTopic", {
      clusterArn: this.cluster.attrArn,
      topicName: mergedProps.topicName!,
      partitionCount: mergedProps.partitionCount!,
      replicationFactor: 3,
    });

    const codePath = "../../../../business-logic";

    // IAM role for the event ingestion Lambda
    const eventIngestionRole = new iam.Role(
      this,
      "KafkaEventIngestionFunctionRole",
      {
        assumedBy: new iam.ServicePrincipal("lambda.amazonaws.com"),
        managedPolicies: [
          iam.ManagedPolicy.fromAwsManagedPolicyName(
            "service-role/AWSLambdaBasicExecutionRole"
          ),
          iam.ManagedPolicy.fromAwsManagedPolicyName(
            "AWSXrayWriteOnlyAccess"
          ),
          iam.ManagedPolicy.fromAwsManagedPolicyName(
            "service-role/AWSLambdaVPCAccessExecutionRole"
          ),
        ],
      }
    );

    eventIngestionRole.addToPolicy(
      new iam.PolicyStatement({
        sid: "MskClusterAccess",
        effect: iam.Effect.ALLOW,
        actions: [
          "kafka-cluster:Connect",
          "kafka-cluster:DescribeCluster",
        ],
        resources: [this.cluster.attrArn],
      })
    );

    eventIngestionRole.addToPolicy(
      new iam.PolicyStatement({
        sid: "MskTopicAccess",
        effect: iam.Effect.ALLOW,
        actions: [
          "kafka-cluster:DescribeTopic",
          "kafka-cluster:WriteData",
        ],
        resources: [this.topic.attrTopicArn],
      })
    );

    // VPC-attached Lambda for producing events into the MSK topic
    this.eventIngestionFunction = new NodejsFunction(
      this,
      "KafkaEventIngestionFunction",
      {
        description:
          "Kafka producer used to send events into a deployed game analytics pipeline MSK topic",
        entry: path.join(
          __dirname,
          `${codePath}/kafka-event-ingestion-lambda/index.js`
        ),
        memorySize: 256,
        timeout: cdk.Duration.minutes(5),
        runtime: lambda.Runtime.NODEJS_22_X,
        tracing: lambda.Tracing.PASS_THROUGH,
        architecture: lambda.Architecture.ARM_64,
        role: eventIngestionRole,
        vpc,
        vpcSubnets: { subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS },
        securityGroups: [this.securityGroup],
        environment: {
          BROKERS: this.cluster.attrBootstrapBrokersSaslIam,
          TOPIC: mergedProps.topicName!,
        },
      }
    );
  }
}
