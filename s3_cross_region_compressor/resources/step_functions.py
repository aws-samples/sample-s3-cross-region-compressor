from constructs import Construct
from aws_cdk import (
    Duration,
    aws_stepfunctions as sfn,
    aws_lambda as _lambda
)
from cdk_nag import NagSuppressions

def create_migration_state_machine(scope: Construct, migration_lambda: _lambda.Function) -> sfn.StateMachine:
    """
    Create a Step Functions state machine that processes S3 objects using a Distributed Map state.
    
    This implementation matches the provided JSON definition exactly:
    - Uses JSONata query language for data transformation
    - Implements S3 ItemReader with dynamic bucket and prefix
    - Configures distributed processing with batching
    - Includes proper Lambda retry configuration
    
    Args:
        scope: The CDK construct scope
        migration_lambda: Lambda function to used by StepFunctions to facilitate data migration
        
    Returns:
        The Step Functions state machine
    """

    # Create ItemReader for S3 objects with JSONata expressions
    item_reader = sfn.S3ObjectsItemReader(
        bucket_name_path="{% $states.input.bucket %}",
        prefix="{% $exists($states.input.prefix_filter) ? $states.input.prefix_filter : \"\" %}"
    )
    
    # Create Lambda task using CustomState to generate proper JSONata ASL
    # This bypasses CDK construct limitations and provides exact ASL structure
    lambda_task = sfn.CustomState(
        scope, "Lambda Invoke",
        state_json={
            "Type": "Task",
            "Resource": "arn:aws:states:::lambda:invoke",
            "Arguments": {
                "FunctionName": migration_lambda.function_arn,
                "Payload": "{% $states.input %}"
            },
            "Output": "{% $states.result.Payload %}",
            "Retry": [
                {
                    "ErrorEquals": [
                        "Lambda.ServiceException",
                        "Lambda.AWSLambdaException",
                        "Lambda.SdkClientException", 
                        "Lambda.TooManyRequestsException"
                    ],
                    "IntervalSeconds": 1,
                    "MaxAttempts": 3,
                    "BackoffRate": 2.0,
                    "JitterStrategy": "FULL"
                }
            ],
            "End": True
        }
    )
    
    # Create DistributedMap with JSONata support
    distributed_map = sfn.DistributedMap(
        scope, "Map",
        query_language=sfn.QueryLanguage.JSONATA,
        map_execution_type=sfn.StateMachineType.STANDARD,
        max_concurrency=1000,
        label="Map",
        item_reader=item_reader,
        item_batcher=sfn.ItemBatcher(
            max_items_per_batch=100,
            batch_input={
                "execution": "{% $states.context.Execution.Input %}"
            }
        )
    )
    
    # Set the Lambda task as the item processor
    distributed_map.item_processor(lambda_task)
    
    # Create the state machine with JSONata query language
    state_machine = sfn.StateMachine(
        scope,
        'MigrationStateMachine',
        state_machine_name='migration-state-machine',
        definition_body=sfn.DefinitionBody.from_chainable(distributed_map),
        query_language=sfn.QueryLanguage.JSONATA,
        state_machine_type=sfn.StateMachineType.STANDARD
    )
    
    # Suppress CDK Nag warnings for logging and tracing
    NagSuppressions.add_resource_suppressions(
        state_machine,
        [
            {
                "id": "AwsSolutions-SF1",
                "reason": "CloudWatch logging not required for this migration state machine - suppressed by user request"
            },
            {
                "id": "AwsSolutions-SF2", 
                "reason": "X-Ray tracing not required for this migration state machine - suppressed by user request"
            }
        ]
    )
    
    return state_machine
