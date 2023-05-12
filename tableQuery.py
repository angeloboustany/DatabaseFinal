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

# Here are all the 20 queries that we implemented

def create_execute_statement_input():

    #What are all the doctors associated with a particular case?
    # input = {"Statement": "SELECT * FROM SynapseAIDB WHERE PK = 'C#1' AND begins_with(SK, 'D#1')"
    # }

    #What are all the scans associated with a particular case?
    # input = {"Statement": "SELECT * FROM SynapseAIDB WHERE PK = 'C#1' AND begins_with(SK, 'S#1')"
    # }

    #What are all the cases for a particular patient?
    # select form gsi
    #input = {"Statement": "select * from \"SynapseAIDB\".\"Patient\" where PID = 'P#1' and begins_with(PK,'C#')"}

    #What are all the scans associated with a particular case?
    # input = {"Statement": "SELECT * FROM SynapseAIDB WHERE PK = 'C#1' AND begins_with(SK, 'S#1')"

    #What are all the cases that a particular doctor has worked on?
    #input = {"Statement": "SELECT * FROM SynapseAIDB WHERE PK = 'D#1' AND begins_with(SK, 'C#')"}

    # What are all the doctors associated with a particular case?
    #input = {"Statement": "SELECT * FROM SynapseAIDB WHERE PK = 'C#2 ' AND begins_with(SK, 'D#')"}

    #What are all the cases associated with a particular doctor and patient?
    #input = {"Statement": "SELECT * FROM SynapseAIDB WHERE PID = 'P#1' AND SK = 'D#1'"}

    # What are all the scans associated with a particular radiologist and case?
    #input = {"Statement": * FROM SynapseAIDB WHERE RID = 'R#1' AND SK = 'C#1'"}

    #What are all the scans for a particular patient and case?
    #input = {"Statement": "SELECT * FROM SynapseAIDB WHERE PID = 'P#1' AND SK = 'C#1'"}

    # What are all the cases associated with a particular patient and radiologist?
    #input = {"Statement": "SELECT * FROM SynapseAIDB WHERE PID = 'P#1' AND RID = 'R#1' AND begins_with(SK, 'C#')"}

    # What are all the cases for a particular patient with a severity level of "HIGH"?
    # input = {"Statement": "SELECT * FROM SynapseAIDB WHERE PID = 'P#1' AND CaseSeverity = 'HIGH' AND begins_with(PK, 'C#')"}

    # Sort cases by severity
    #input = {"Statement": "SELECT * FROM SynapseAIDB WHERE begins_with(PK, 'C#') ORDER BY SeverityLevel DESC"}

    #How many cases there are for a particular doctor?
    #input = {"Statement": "SELECT * FROM SynapseAIDB WHERE PK = 'D#1' AND begins_with(SK, 'C#')"}

    # What medications is a particular patient currently taking?
    #input = {"Statement": "SELECT MedicationName FROM SynapseAIDB WHERE PK = 'P#1'"}

    # What is the latest blood pressure reading for a particular patient?
    #input = {"Statement": "SELECT BloodPressure FROM \"SynapseAIDB\".\"Diagnosis\" WHERE PK = 'P#1' ORDER BY DiagnosisTime DESC"}

    #What is the most recent diagnosis for a particular patient?
    #input = {"Statement": "SELECT * FROM \"SynapseAIDB\".\"Diagnosis\" WHERE PK = 'P#1' ORDER BY DiagnosisTime DESC"}

    #What is the total cost of medical procedures for a particular patient?
    #input = {"Statement": "SELECT ProcedureCost FROM SynapseAIDB WHERE PK = 'C#1' AND SK = 'C#1'"}

    #How many patient have this diagnosis X
    #input = {"Statement": "SELECT * FROM SynapseAIDB WHERE Diagnosis = 'Patient Diagnosised with Chain Pain could be hear failure'"}

    # List all patients who have a specific medication prescribed:
    #input = {"Statement": "SELECT * FROM SynapseAIDB WHERE MedicationName = 'Panadol' AND begins_with(PK, 'P#')"}

    # Get all the scans for a particular case, sorted by date: Attribute ScanDate in ORDER BY clause must be part of the primary key
    #input = {"Statement": "SELECT * FROM \"SynapseAIDB\".\"PK-ScanDate-index\" WHERE PK = 'C#1' AND begins_with(SK, 'S#') ORDER BY ScanDate DESC"}

    #Get the age of all patients in the system:
    #input = {"Statement": "SELECT Age FROM \"SynapseAIDB\".\"AgeAVG\" where begins_with(PK,'P#')"}
    return input

def execute_statement(dynamodb_client, input):
    try:
        response = dynamodb_client.execute_statement(**input)
        #count
        # count = 0
        # for item in response['Items']:
        #     print(item['Age'])
        #     count += 1
        # print(count)
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
