from aws_cdk import (
    aws_secretsmanager as secretsmanager,
    aws_iam as iam,
    aws_lambda as lambda_,
    aws_dynamodb as dynamodb,
    aws_events as events,
    aws_events_targets as targets,
    aws_apigateway as apigateway,
    aws_sns as sns,
    aws_sns_subscriptions as sns_subscriptions,
    RemovalPolicy,
    Stack,
    Duration
)
from aws_cdk.aws_lambda_python_alpha import PythonFunction
from constructs import Construct


class PolyMarketStack(Stack):
    def __init__(self, scope: Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        # x API credentials secrets
        x_access_token_secret = secretsmanager.Secret.from_secret_name_v2(
            self,
            "XAccessTokenSecret",
            secret_name="polymarket/x-access-token",
        )
        
        x_access_token_secret_secret = secretsmanager.Secret.from_secret_name_v2(
            self,
            "XAccessTokenSecretSecret",
            secret_name="polymarket/x-access-token-secret",
        )
        
        x_consumer_key_secret = secretsmanager.Secret.from_secret_name_v2(
            self,
            "XConsumerKeySecret",
            secret_name="polymarket/x-consumer-key",
        )
        
        x_consumer_secret_secret = secretsmanager.Secret.from_secret_name_v2(
            self,
            "XConsumerSecretSecret",
            secret_name="polymarket/x-consumer-secret",
        )

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

        posts_table = dynamodb.Table(
            self,
            "PostsTable",
            table_name="PolymarketPosts",
            partition_key=dynamodb.Attribute(name="market_id", type=dynamodb.AttributeType.NUMBER),
            sort_key=dynamodb.Attribute(name="posted_at", type=dynamodb.AttributeType.STRING),
            removal_policy=RemovalPolicy.DESTROY,
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            time_to_live_attribute="ttl"
        )

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
        posts_table.grant_read_write_data(lambda_execution_role)

        # Add Secrets Manager permissions
        x_access_token_secret.grant_read(lambda_execution_role)
        x_access_token_secret_secret.grant_read(lambda_execution_role)
        x_consumer_key_secret.grant_read(lambda_execution_role)
        x_consumer_secret_secret.grant_read(lambda_execution_role)

        # Collector Lambda
        collector_lambda = PythonFunction(
            self,
            "CollectorFunction",
            entry="../serverless",
            index="collector/collector.py",
            handler="lambda_handler",
            runtime=lambda_.Runtime.PYTHON_3_9,
            timeout=Duration.minutes(10),
            role=lambda_execution_role,
            memory_size=256,
            environment={
                "MARKETS_TABLE": markets_table.table_name,
                "HISTORICAL_TABLE": historical_table.table_name
            }
        )

        # Create SNS Topic for market movement events
        market_movements_topic = sns.Topic(
            self,
            "MarketMovementsTopic",
            display_name="Market Movements Topic",
            topic_name="PolymarketMovements"
        )

        # Analyzer Lambda
        analyzer_lambda = PythonFunction(
            self,
            "AnalyzerFunction",
            entry="../serverless",
            index="analyzer/analyzer.py",
            handler="lambda_handler",
            runtime=lambda_.Runtime.PYTHON_3_9,
            timeout=Duration.minutes(5),
            memory_size=256,
            role=lambda_execution_role,
            environment={
                "MARKETS_TABLE": markets_table.table_name,
                "HISTORICAL_TABLE": historical_table.table_name,
                "POSTS_TABLE": posts_table.table_name,
                "MARKET_MOVEMENTS_TOPIC_ARN": market_movements_topic.topic_arn
            }
        )
        market_movements_topic.grant_publish(analyzer_lambda)

        publisher_lambda = PythonFunction(
            self,
            "PublisherFunction",
            entry="../serverless",
            index="publisher/publisher.py",
            handler="lambda_handler",
            runtime=lambda_.Runtime.PYTHON_3_9,
            timeout=Duration.minutes(2),
            memory_size=256,
            role=lambda_execution_role,
            environment={
                "MARKETS_TABLE": markets_table.table_name,
                "HISTORICAL_TABLE": historical_table.table_name,
                "POSTS_TABLE": posts_table.table_name,
                "X_ACCESS_TOKEN_SECRET_ARN": x_access_token_secret.secret_arn,
                "X_ACCESS_TOKEN_SECRET_SECRET_ARN": x_access_token_secret_secret.secret_arn,
                "X_CONSUMER_KEY_SECRET_ARN": x_consumer_key_secret.secret_arn,
                "X_CONSUMER_SECRET_SECRET_ARN": x_consumer_secret_secret.secret_arn
            }
        )
        
        # Subscribe publisher to SNS topic
        market_movements_topic.add_subscription(
            sns_subscriptions.LambdaSubscription(publisher_lambda)
        )

        # Schedule for Collector Lambda (every 5 minutes)
        collector_schedule = events.Rule(
            self,
            "CollectorSchedule",
            schedule=events.Schedule.rate(Duration.minutes(5)),
            targets=[targets.LambdaFunction(collector_lambda)]
        )

        # Schedule for Analyzer Lambda (every 15 minutes)
        analyzer_schedule = events.Rule(
            self,
            "AnalyzerSchedule",
            schedule=events.Schedule.rate(Duration.minutes(15)),
            targets=[targets.LambdaFunction(analyzer_lambda)]
        )

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