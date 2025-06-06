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
            time_to_live_attribute="ttl"
        )

        historical_table = dynamodb.Table(
            self,
            "HistoricalTable",
            table_name="PolymarketHistorical",
            partition_key=dynamodb.Attribute(name="market_id", type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name="timestamp", type=dynamodb.AttributeType.STRING),
            removal_policy=RemovalPolicy.DESTROY,
            time_to_live_attribute="ttl"
        )

        posts_table = dynamodb.Table(
            self,
            "PostsTable",
            table_name="PolymarketPosts",
            partition_key=dynamodb.Attribute(name="market_id", type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name="posted_at", type=dynamodb.AttributeType.STRING),
            removal_policy=RemovalPolicy.DESTROY,
            time_to_live_attribute="ttl"
        )
        
        # New tables for enhanced signal detection and resolution tracking
        signals_table = dynamodb.Table(
            self,
            "SignalsTable",
            table_name="PolymarketSignals",
            partition_key=dynamodb.Attribute(name="market_id", type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name="signal_id", type=dynamodb.AttributeType.STRING),
            removal_policy=RemovalPolicy.DESTROY,
            time_to_live_attribute="ttl"
        )
        
        resolutions_table = dynamodb.Table(
            self,
            "ResolutionsTable",
            table_name="PolymarketResolutions",
            partition_key=dynamodb.Attribute(name="market_id", type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name="resolution_timestamp", type=dynamodb.AttributeType.STRING),
            removal_policy=RemovalPolicy.DESTROY,
            time_to_live_attribute="ttl"
        )
        
        thresholds_table = dynamodb.Table(
            self,
            "ThresholdsTable",
            table_name="PolymarketThresholds",
            partition_key=dynamodb.Attribute(name="threshold_id", type=dynamodb.AttributeType.STRING),
            removal_policy=RemovalPolicy.DESTROY,
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
        
        # Add explicit permissions for Secrets Manager
        lambda_execution_role.add_to_policy(
            iam.PolicyStatement(
                actions=["secretsmanager:GetSecretValue"],
                resources=[
                    f"arn:aws:secretsmanager:{self.region}:{self.account}:secret:polymarket/x-access-token*",
                    f"arn:aws:secretsmanager:{self.region}:{self.account}:secret:polymarket/x-access-token-secret*",
                    f"arn:aws:secretsmanager:{self.region}:{self.account}:secret:polymarket/x-consumer-key*",
                    f"arn:aws:secretsmanager:{self.region}:{self.account}:secret:polymarket/x-consumer-secret*"
                ]
            )
        )
        
        # Add DynamoDB permissions
        markets_table.grant_read_write_data(lambda_execution_role)
        historical_table.grant_read_write_data(lambda_execution_role)
        posts_table.grant_read_write_data(lambda_execution_role)
        signals_table.grant_read_write_data(lambda_execution_role)
        resolutions_table.grant_read_write_data(lambda_execution_role)
        thresholds_table.grant_read_write_data(lambda_execution_role)

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
                "SIGNALS_TABLE": signals_table.table_name,
                "MARKET_MOVEMENTS_TOPIC_ARN": market_movements_topic.topic_arn
            }
        )
        market_movements_topic.grant_publish(analyzer_lambda)
        
        # Signal Analyzer Lambda
        signal_analyzer_lambda = PythonFunction(
            self,
            "SignalAnalyzerFunction",
            entry="../serverless",
            index="signal_analyzer/signal_analyzer.py",
            handler="lambda_handler",
            runtime=lambda_.Runtime.PYTHON_3_9,
            timeout=Duration.minutes(5),
            memory_size=256,
            role=lambda_execution_role,
            environment={
                "MARKETS_TABLE": markets_table.table_name,
                "HISTORICAL_TABLE": historical_table.table_name,
                "SIGNALS_TABLE": signals_table.table_name,
                "THRESHOLDS_TABLE": thresholds_table.table_name
            }
        )
        
        # Resolution Tracker Lambda
        resolution_tracker_lambda = PythonFunction(
            self,
            "ResolutionTrackerFunction",
            entry="../serverless",
            index="resolution_tracker/resolution_tracker.py",
            handler="lambda_handler",
            runtime=lambda_.Runtime.PYTHON_3_9,
            timeout=Duration.minutes(5),
            memory_size=256,
            role=lambda_execution_role,
            environment={
                "MARKETS_TABLE": markets_table.table_name,
                "SIGNALS_TABLE": signals_table.table_name,
                "RESOLUTIONS_TABLE": resolutions_table.table_name,
                "THRESHOLDS_TABLE": thresholds_table.table_name
            }
        )

        # Publisher Lambda
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
                "X_ACCESS_TOKEN_SECRET_NAME": "polymarket/x-access-token",
                "X_ACCESS_TOKEN_SECRET_SECRET_NAME": "polymarket/x-access-token-secret",
                "X_CONSUMER_KEY_SECRET_NAME": "polymarket/x-consumer-key",
                "X_CONSUMER_SECRET_SECRET_NAME": "polymarket/x-consumer-secret"
            }
        )
        
        # Subscribe publisher to SNS topic
        market_movements_topic.add_subscription(
            sns_subscriptions.LambdaSubscription(publisher_lambda)
        )

        # Schedule for Collector Lambda (every 20 minutes)
        collector_schedule = events.Rule(
            self,
            "CollectorSchedule",
            schedule=events.Schedule.rate(Duration.minutes(20)),
            targets=[targets.LambdaFunction(collector_lambda)]
        )

        # Schedule for Analyzer Lambda (every 36 minutes)
        analyzer_schedule = events.Rule(
            self,
            "AnalyzerSchedule",
            schedule=events.Schedule.rate(Duration.minutes(36)),
            targets=[targets.LambdaFunction(analyzer_lambda)]
        )
        
        # Schedule for Signal Analyzer Lambda (every 15 minutes)
        signal_analyzer_schedule = events.Rule(
            self,
            "SignalAnalyzerSchedule",
            schedule=events.Schedule.rate(Duration.minutes(15)),
            targets=[targets.LambdaFunction(signal_analyzer_lambda)]
        )
        
        # Schedule for Resolution Tracker Lambda (every 60 minutes)
        resolution_tracker_schedule = events.Rule(
            self,
            "ResolutionTrackerSchedule",
            schedule=events.Schedule.rate(Duration.minutes(60)),
            targets=[targets.LambdaFunction(resolution_tracker_lambda)]
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