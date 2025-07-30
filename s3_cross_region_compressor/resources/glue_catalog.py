"""
AWS Glue resources for catalog metadata querying.
"""

from aws_cdk import (
    aws_glue as glue,
    aws_iam as iam,
    aws_s3 as s3,
    aws_events as events,
    aws_events_targets as targets,
)
from constructs import Construct


def create_glue_database(scope: Construct, database_name: str) -> glue.CfnDatabase:
    """
    Create Glue database for catalog metadata.
    
    Args:
        scope: CDK construct scope
        database_name: Name of the Glue database
        
    Returns:
        glue.CfnDatabase: The created Glue database
    """
    return glue.CfnDatabase(
        scope,
        'GlueDatabase',
        catalog_id=scope.account,
        database_input=glue.CfnDatabase.DatabaseInputProperty(
            name=database_name,
            description='Database for S3 backup catalog metadata'
        )
    )


def create_glue_crawler_role(scope: Construct) -> iam.Role:
    """
    Create IAM role for Glue crawler.
    
    Args:
        scope: CDK construct scope
        
    Returns:
        iam.Role: The created IAM role
    """
    role = iam.Role(
        scope,
        'GlueCrawlerRole',
        assumed_by=iam.ServicePrincipal('glue.amazonaws.com'),
        managed_policies=[
            iam.ManagedPolicy.from_aws_managed_policy_name('service-role/AWSGlueServiceRole')
        ]
    )
    
    # Add CloudWatch Logs permissions for KMS key association
    role.add_to_policy(
        iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=[
                'logs:AssociateKmsKey',
                'logs:CreateLogGroup',
                'logs:CreateLogStream',
                'logs:PutLogEvents'
            ],
            resources=['*']
        )
    )
    
    return role


def create_glue_crawler(
    scope: Construct, 
    database: glue.CfnDatabase, 
    catalog_bucket: s3.Bucket,
    crawler_role: iam.Role,
    stack_name: str
) -> glue.CfnCrawler:
    """
    Create Glue crawler for catalog metadata.
    
    Args:
        scope: CDK construct scope
        database: Glue database
        catalog_bucket: S3 catalog bucket
        crawler_role: IAM role for crawler
        stack_name: Stack name for naming
        
    Returns:
        glue.CfnCrawler: The created Glue crawler
    """
    # Grant S3 permissions to crawler role
    catalog_bucket.grant_read(crawler_role)
    
    crawler = glue.CfnCrawler(
        scope,
        'CatalogCrawlerV2',
        name=f'{stack_name}-catalog-crawler-v2',
        role=crawler_role.role_arn,
        database_name=database.ref,
        description='S3 catalog crawler for backup metadata v2',
        targets=glue.CfnCrawler.TargetsProperty(
            s3_targets=[
                glue.CfnCrawler.S3TargetProperty(
                    path=f's3://{catalog_bucket.bucket_name}/'
                )
            ]
        ),
        configuration='{"Version":1.0,"CrawlerOutput":{"Partitions":{"AddOrUpdateBehavior":"InheritFromTable"}},"Grouping":{"TableGroupingPolicy":"CombineCompatibleSchemas"}}',
        schema_change_policy=glue.CfnCrawler.SchemaChangePolicyProperty(
            update_behavior='UPDATE_IN_DATABASE',
            delete_behavior='LOG'
        )
    )
    
    return crawler


def create_crawler_schedule(
    scope: Construct,
    crawler: glue.CfnCrawler
) -> events.Rule:
    """
    Create EventBridge rule to run crawler daily.
    
    Args:
        scope: CDK construct scope
        crawler: Glue crawler
        
    Returns:
        events.Rule: The created EventBridge rule
    """
    rule = events.Rule(
        scope,
        'CrawlerSchedule',
        description='Run Glue crawler daily for catalog metadata',
        schedule=events.Schedule.cron(
            minute='0',
            hour='2',  # Run at 2 AM UTC daily
            day='*',
            month='*',
            year='*'
        )
    )
    
    # Add Glue crawler as target
    rule.add_target(
        targets.AwsApi(
            service='glue',
            action='startCrawler',
            parameters={
                'Name': crawler.name
            }
        )
    )
    
    return rule