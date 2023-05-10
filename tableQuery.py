import boto3
from botocore.exceptions import ClientError

ERROR_HELP_STRINGS = {
    # Operation specific errors
    'ConditionalCheckFailedException': 'Condition check specified in the operation failed, review and update the condition check before retrying',
    'TransactionConflictException': 'Operation was rejected because there is an ongoing transaction for the item, generally safe to retry with exponential back-off',
    'ItemCollectionSizeLimitExceededException': 'An item collection is too large, you\'re using Local Secondary Index and exceeded size limit of items per partition key.' +
                                                ' Consider using Global Secondary Index instead',
    # Common Errors
    'InternalServerError': 'Internal Server Error, generally safe to retry with exponential back-off',
    'ProvisionedThroughputExceededException': 'Request rate is too high. If you\'re using a custom retry strategy make sure to retry with exponential back-off.' +
                                              'Otherwise consider reducing frequency of requests or increasing provisioned capacity for your table or secondary index',
    'ResourceNotFoundException': 'One of the tables was not found, verify table exists before retrying',
    'ServiceUnavailable': 'Had trouble reaching DynamoDB. generally safe to retry with exponential back-off',
    'ThrottlingException': 'Request denied due to throttling, generally safe to retry with exponential back-off',
    'UnrecognizedClientException': 'The request signature is incorrect most likely due to an invalid AWS access key ID or secret key, fix before retrying',
    'ValidationException': 'The input fails to satisfy the constraints specified by DynamoDB, fix input before retrying',
    'RequestLimitExceeded': 'Throughput exceeds the current throughput limit for your account, increase account level throughput before retrying',
}

def create_dynamodb_client(region="us-east-1"):
    return boto3.client("dynamodb", region_name=region)


def create_execute_statement_input():
    #What are all the cases that a particular doctor has worked on?
    # input = {"Statement": "SELECT * FROM SynapseAIDB WHERE PK = 'DOCTOR#1' AND begins_with(SK, 'CASE#1')"
    # }

    #What are all the doctors associated with a particular case?
    # input = {"Statement": "SELECT * FROM SynapseAIDB WHERE PK = 'CASE#1' AND begins_with(SK, 'DOCTOR#1')"
    # }

    #What are all the scans associated with a particular case?
    # input = {"Statement": "SELECT * FROM SynapseAIDB WHERE PK = 'CASE#1' AND begins_with(SK, 'SCAN#1')"

    return input


def execute_statement(dynamodb_client, input):
    try:
        response = dynamodb_client.execute_statement(**input)
        print(response['Items'])
        print("ExecuteStatement executed successfully.")
        # Handle response
    except ClientError as error:
        handle_error(error)
    except BaseException as error:
        print("Unknown error while executing executeStatement operation: " +
              error.response['Error']['Message'])


def handle_error(error):
    error_code = error.response['Error']['Code']
    error_message = error.response['Error']['Message']

    error_help_string = ERROR_HELP_STRINGS[error_code]

    print('[{error_code}] {help_string}. Error message: {error_message}'
          .format(error_code=error_code,
                  help_string=error_help_string,
                  error_message=error_message))


def main():
    # Create the DynamoDB Client with the region you want
    dynamodb_client = create_dynamodb_client()

    # Create the dictionary containing arguments for execute_statement call
    partiql_statement_input = create_execute_statement_input()

    # Call DynamoDB's execute_statement API
    execute_statement(dynamodb_client, partiql_statement_input)


if __name__ == "__main__":
    main()
