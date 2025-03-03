from aws_cdk import (
    aws_secretsmanager as secretsmanager,
    aws_iam as iam,
    aws_ec2 as ec2,
    aws_lambda as lambda_,
    aws_dynamodb as dynamodb,
    aws_events as events,
    aws_events_targets as targets,
    aws_ecr_assets as ecr_assets,
    aws_apigateway as apigateway,
    RemovalPolicy,
    Stack,
    Duration
)
from aws_cdk.aws_lambda_python_alpha import PythonFunction
from aws_cdk.aws_s3 import Bucket
from constructs import Construct


class PolyMarketStack(Stack):
    def __init__(self, scope: Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        # groq_api_secret = secretsmanager.Secret.from_secret_name_v2(
        #     self,
        #     "GroqApiKeySecret",
        #     "IRWorkflow/GroqApiKey"
        # )

        # # Twitter API credentials secret
        # twitter_api_secret = secretsmanager.Secret(
        #     self,
        #     "TwitterApiSecret",
        #     secret_name="PolymarketWatcher/TwitterApiCredentials",
        #     description="Twitter API credentials for Polymarket Watcher"
        # )

        # DynamoDB Tables
        markets_table = dynamodb.Table(
            self,
            "MarketsTable",
            table_name="PolymarketMarkets",
            partition_key=dynamodb.Attribute(name="id", type=dynamodb.AttributeType.STRING),
            removal_policy=RemovalPolicy.DESTROY,
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            time_to_live_attribute="ttl"
        )

        historical_table = dynamodb.Table(
            self,
            "HistoricalTable",
            table_name="PolymarketHistorical",
            partition_key=dynamodb.Attribute(name="market_id", type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name="timestamp", type=dynamodb.AttributeType.STRING),
            removal_policy=RemovalPolicy.DESTROY,
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            time_to_live_attribute="ttl"
        )

        # posts_table = dynamodb.Table(
        #     self,
        #     "PostsTable",
        #     table_name="PolymarketPosts",
        #     partition_key=dynamodb.Attribute(name="market_id", type=dynamodb.AttributeType.NUMBER),
        #     sort_key=dynamodb.Attribute(name="posted_at", type=dynamodb.AttributeType.STRING),
        #     removal_policy=RemovalPolicy.DESTROY,
        #     billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
        #     time_to_live_attribute="ttl"
        # )

        # manager_function = PythonFunction(
        #     self,
        #     "ManagerFunction",
        #     entry="../serverless/manager",
        #     index="manager.py",
        #     handler="lambda_handler",
        #     runtime=lambda_.Runtime.PYTHON_3_9,
        #     timeout=Duration.seconds(60 * 15),
        #     environment={
        #         "TABLE_NAME": market_table.table_name,
        #         "WORKER_IMAGE_URI": "worker_image_asset.image_uri",
        #         "WORKER_EXECUTION_ROLE": "worker_lambda_execution_role.role_arn",
        #         "HISTORICAL_TABLE": historical_table.table_name,
        #         "CONFIG_TABLE": "config_table.table_name",
        #         "MESSAGES_TABLE": "messages_table.table_name",
        #         "AWS_ACCOUNT_ID": self.account,
        #         "GROQ_API_SECRET_ARN": groq_api_secret.secret_arn, 
        #         "DISCORD_WEBHOOK_SECRET_ARN": "discord_webhook_url.secret_arn",
        #         "INSTANCE_PROFILE": "instance_profile.ref",
        #         "SUBNET_ID": "vpc.public_subnets[0].subnet_id",
        #         "INSTANCE_SECURITY_GROUP": "instance_sg.security_group_id",
        #         "ARTIFACT_BUCKET": "artifact_bucket.bucket_name",
        #     },
        # )

        # market_table.grant_read_data(manager_function)
        # market_table.grant_write_data(manager_function)

        # Lambda execution role with permissions
        lambda_execution_role = iam.Role(
            self,
            "PolymarketWatcherLambdaRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole")
            ]
        )

        # Add DynamoDB permissions
        markets_table.grant_read_write_data(lambda_execution_role)
        historical_table.grant_read_write_data(lambda_execution_role)
        # posts_table.grant_read_write_data(lambda_execution_role)

        # Add Secrets Manager permissions
        # twitter_api_secret.grant_read(lambda_execution_role)

        # Collector Lambda
        collector_lambda = PythonFunction(
            self,
            "CollectorFunction",
            entry="../serverless",
            index="collector/collector.py",
            handler="lambda_handler",
            runtime=lambda_.Runtime.PYTHON_3_9,
            timeout=Duration.minutes(5),
            role=lambda_execution_role,
            environment={
                "MARKETS_TABLE": markets_table.table_name,
                "HISTORICAL_TABLE": historical_table.table_name
            }
        )

        # Analyzer Lambda
        # analyzer_lambda = PythonFunction(
        #     self,
        #     "AnalyzerFunction",
        #     entry="../serverless",
        #     index="analyzer/analyzer.py",
        #     handler="lambda_handler",
        #     runtime=lambda_.Runtime.PYTHON_3_9,
        #     timeout=Duration.seconds(60),
        #     memory_size=256,
        #     role=lambda_execution_role,
        #     environment={
        #         "MARKETS_TABLE": markets_table.table_name,
        #         "HISTORICAL_TABLE": historical_table.table_name,
        #         "POSTS_TABLE": posts_table.table_name
        #     }
        # )

        # # Publisher Lambda
        # publisher_lambda = PythonFunction(
        #     self,
        #     "PublisherFunction",
        #     entry="../serverless",
        #     index="publisher/publisher.py",
        #     handler="lambda_handler",
        #     runtime=lambda_.Runtime.PYTHON_3_9,
        #     timeout=Duration.seconds(60),
        #     memory_size=256,
        #     role=lambda_execution_role,
        #     environment={
        #         "POSTS_TABLE": posts_table.table_name,
        #         "TWITTER_SECRET_ARN": twitter_api_secret.secret_arn
        #     }
        # )


        # Schedule for Collector Lambda (every 5 minutes)
        collector_schedule = events.Rule(
            self,
            "CollectorSchedule",
            schedule=events.Schedule.rate(Duration.minutes(5)),
            targets=[targets.LambdaFunction(collector_lambda)]
        )

        # # Schedule for Analyzer Lambda (every 5 minutes, slightly offset)
        # analyzer_schedule = events.Rule(
        #     self,
        #     "AnalyzerSchedule",
        #     schedule=events.Schedule.rate(Duration.minutes(5)),
        #     targets=[targets.LambdaFunction(analyzer_lambda)]
        # )

        # API Gateway for manual triggering
        # api = apigateway.RestApi(
        #     self,
        #     "PolymarketWatcherApi",
        #     rest_api_name="Polymarket Watcher API",
        #     description="API for Polymarket Watcher",
        #     deploy_options=apigateway.StageOptions(
        #         stage_name="prod"
        #     )
        # )

        # # API Gateway endpoints
        # collect_resource = api.root.add_resource("collect")
        # collect_integration = apigateway.LambdaIntegration(collector_lambda)
        # collect_resource.add_method("POST", collect_integration)

        # analyze_resource = api.root.add_resource("analyze")
        # analyze_integration = apigateway.LambdaIntegration(analyzer_lambda)
        # analyze_resource.add_method("POST", analyze_integration)

        # publish_resource = api.root.add_resource("publish")
        # publish_integration = apigateway.LambdaIntegration(publisher_lambda)
        # publish_resource.add_method("POST", publish_integration)