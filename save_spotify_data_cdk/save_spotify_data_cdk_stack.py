from constructs import Construct
from aws_cdk import (
    Stack,
    aws_lambda,
    aws_apigateway,
    aws_events,
    aws_events_targets,
    aws_s3,
    aws_dynamodb,
    Duration,
    RemovalPolicy
)
from os import name, path, remove

from settings.spotify_settings import CLIENT_ID, CLIENT_SECRET

class SaveSpotifyDataCdkStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        my_bucket = aws_s3.Bucket(
            self, "spotify-data",
            versioned=False,
            removal_policy=RemovalPolicy.DESTROY)

        my_table = aws_dynamodb.Table(
            self,"SpotifyArtists",
            table_name='spotify_artists',
            partition_key=aws_dynamodb.Attribute(name="artist_name", type=aws_dynamodb.AttributeType.STRING),
            sort_key=aws_dynamodb.Attribute(name="date", type=aws_dynamodb.AttributeType.STRING),
            billing_mode=aws_dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY
        )
        
        my_lambda = aws_lambda.Function(
                self, "SaveSpotifyData",
                runtime=aws_lambda.Runtime.PYTHON_3_8,
                # https://stackoverflow.com/a/69276116/1561441
                # https://docs.aws.amazon.com/cdk/api/v1/python/aws_cdk.aws_lambda_python/README.html
                # https://docs.aws.amazon.com/cdk/api/v1/python/aws_cdk.core/BundlingOptions.html#aws_cdk.core.BundlingOptions
                code=aws_lambda.Code.from_asset("lambda_save_spotify_data"),
                handler="lambda_function.lambda_handler",
                function_name="save-spotify-data",
                timeout=Duration.seconds(15),
                environment={
                    'TARGET_BUCKET_NAME': my_bucket.bucket_name,
                    'TARGET_BUCKET_NAME_DATA_FOLDER': 'history/',
                    'DYNAMODB_TABLE_NAME': my_table.table_name,
                    'CLIENT_ID': CLIENT_ID,
                    'CLIENT_SECRET': CLIENT_SECRET,
                    'AUTH_URL': 'https://accounts.spotify.com/api/token',
                    'BASE_URL': 'https://api.spotify.com/v1/'
                }
        )
        my_lambda.add_layers(
            aws_lambda.LayerVersion.from_layer_version_arn(
                self, 'AWSDataWrangler',
                layer_version_arn='arn:aws:lambda:us-east-1:336392948345:layer:AWSDataWrangler-Python38:1')
)
        my_bucket.grant_write(my_lambda)
        my_table.grant_read_write_data(my_lambda)

        aws_apigateway.LambdaRestApi(
            self, "SpotifyLambdaEndpoint",
            handler=my_lambda,
            rest_api_name="Spotify-Lambda-Endpoint",
            # cache results (requires EDGE endpoint)
            # changes in event["path"] won't be reflected in result for caching period
            endpoint_configuration=aws_apigateway.EndpointConfiguration(
                types=[aws_apigateway.EndpointType.EDGE]
            ),
            deploy_options=aws_apigateway.StageOptions(
                caching_enabled=True,
                cache_ttl=Duration.minutes(5),
                cache_cluster_size='0.5'
            )
        )
        
        # schedule execution to once a day (at 8 AM)
        rule_daily = aws_events.Rule(
            self, "DailyUTC8AM",
            # schedule=aws_events.Schedule.rate(Duration.days(1)),
            schedule=aws_events.Schedule.cron(hour="8", minute="0"),
            rule_name="Daily-UTC-8-AM"
        )
        rule_daily.add_target(aws_events_targets.LambdaFunction(my_lambda))