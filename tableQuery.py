import boto3
from botocore.exceptions import ClientError


''' "DataModel": [
    {
      "TableName": "SynapseAIDB",
      "KeyAttributes": {
        "PartitionKey": {
          "AttributeName": "PK",
          "AttributeType": "S"
        },
        "SortKey": {
          "AttributeName": "SK",
          "AttributeType": "S"
        }
      },
      "NonKeyAttributes": [
        {
          "AttributeName": "Fname",
          "AttributeType": "S"
        },
        {
          "AttributeName": "Lname",
          "AttributeType": "S"
        },
        {
          "AttributeName": "Email",
          "AttributeType": "S"
        },
        {
          "AttributeName": "Phone",
          "AttributeType": "N"
        },
        {
          "AttributeName": "Certification",
          "AttributeType": "S"
        },
        {
          "AttributeName": "Specialty",
          "AttributeType": "S"
        },
        {
          "AttributeName": "Age",
          "AttributeType": "N"
        },
        {
          "AttributeName": "Gender",
          "AttributeType": "S"
        },
        {
          "AttributeName": "Time",
          "AttributeType": "N"
        },
        {
          "AttributeName": "Date",
          "AttributeType": "S"
        },
        {
          "AttributeName": "Location",
          "AttributeType": "S"
        },
        {
          "AttributeName": "Reason",
          "AttributeType": "S"
        },
        {
          "AttributeName": "CaseSeverity",
          "AttributeType": "S"
        },
        {
          "AttributeName": "ScanDate",
          "AttributeType": "S"
        },
        {
          "AttributeName": "MedicationName",
          "AttributeType": "S"
        },
        {
          "AttributeName": "BloodPressure",
          "AttributeType": "S"
        },
        {
          "AttributeName": "Diagnosis",
          "AttributeType": "S"
        },
        {
          "AttributeName": "VisitTimestamp",
          "AttributeType": "S"
        },
        {
          "AttributeName": "ProcedureCost",
          "AttributeType": "S"
        },
        {
          "AttributeName": "EntityType",
          "AttributeType": "S"
        },
        {
          "AttributeName": "Info",
          "AttributeType": "S"
        },
        {
          "AttributeName": "URL",
          "AttributeType": "S"
        },
        {
          "AttributeName": "Notes ",
          "AttributeType": "S"
        },
        {
          "AttributeName": "Appointment_time ",
          "AttributeType": "S"
        },
        {
          "AttributeName": "#of assignment",
          "AttributeType": "N"
        },
        {
          "AttributeName": "PID",
          "AttributeType": "S"
        },
        {
          "AttributeName": "DID",
          "AttributeType": "S"
        },
        {
          "AttributeName": "RID",
          "AttributeType": "S"
        },
        {
          "AttributeName": "Severity Level",
          "AttributeType": "N"
        },
        {
          "AttributeName": "DiagnosisTime",
          "AttributeType": "N"
        }
      ],
      "GlobalSecondaryIndexes": [
        {
          "IndexName": "SK_PK",
          "KeyAttributes": {
            "PartitionKey": {
              "AttributeName": "SK",
              "AttributeType": "S"
            },
            "SortKey": {
              "AttributeName": "PK",
              "AttributeType": "S"
            }
          },
          "Projection": {
            "ProjectionType": "ALL"
          }
        },
        {
          "IndexName": "Case_Patient",
          "KeyAttributes": {
            "PartitionKey": {
              "AttributeName": "SK",
              "AttributeType": "S"
            },
            "SortKey": {
              "AttributeName": "PID",
              "AttributeType": "S"
            }
          },
          "Projection": {
            "ProjectionType": "INCLUDE",
            "NonKeyAttributes": [
              "PK"
            ]
          }
        },
        {
          "IndexName": "Case_Radiologist",
          "KeyAttributes": {
            "PartitionKey": {
              "AttributeName": "SK",
              "AttributeType": "S"
            },
            "SortKey": {
              "AttributeName": "RID",
              "AttributeType": "S"
            }
          },
          "Projection": {
            "ProjectionType": "INCLUDE",
            "NonKeyAttributes": [
              "PK"
            ]
          }
        },
        {
          "IndexName": "Diag_Pat_Count",
          "KeyAttributes": {
            "PartitionKey": {
              "AttributeName": "Diagnosis",
              "AttributeType": "S"
            },
            "SortKey": {
              "AttributeName": "PID",
              "AttributeType": "S"
            }
          },
          "Projection": {
            "ProjectionType": "KEYS_ONLY"
          }
        },
        {
          "IndexName": "Medication",
          "KeyAttributes": {
            "PartitionKey": {
              "AttributeName": "MedicationName",
              "AttributeType": "S"
            },
            "SortKey": {
              "AttributeName": "PID",
              "AttributeType": "S"
            }
          },
          "Projection": {
            "ProjectionType": "KEYS_ONLY"
          }
        },
        {
          "IndexName": "AgeAVG",
          "KeyAttributes": {
            "PartitionKey": {
              "AttributeName": "PK",
              "AttributeType": "S"
            }
          },
          "Projection": {
            "ProjectionType": "INCLUDE",
            "NonKeyAttributes": [
              "Age"
            ]
          }
        }
      ],
      "TableData": [
        {
          "PK": {
            "S": "D#1"
          },
          "SK": {
            "S": "I#2023"
          },
          "Fname": {
            "S": "Angelo"
          },
          "Lname": {
            "S": "Boustany"
          },
          "Email": {
            "S": "angelo@gmail.com"
          },
          "Phone": {
            "N": "71589456"
          },
          "Certification": {
            "S": "Board Certified in Cardiology"
          },
          "Specialty": {
            "S": "Cardiologist"
          },
          "Age": {
            "N": "30"
          },
          "Gender": {
            "S": "Male"
          },
          "EntityType": {
            "S": "DOCTOR"
          }
        },
        {
          "PK": {
            "S": "D#1"
          },
          "SK": {
            "S": "P#1"
          },
          "Age": {
            "N": "25"
          },
          "Gender": {
            "S": "Male"
          },
          "EntityType": {
            "S": "PATIENT"
          },
          "Notes ": {
            "S": "Patient need an MRI"
          },
          "Appointment_time ": {
            "S": "2023-5-15"
          },
          "#of assignment": {
            "N": "2"
          }
        },
        {
          "PK": {
            "S": "D#1"
          },
          "SK": {
            "S": "C#2"
          },
          "Time": {
            "N": "1686221070000"
          },
          "Location": {
            "S": "Beirut, Hopital Saint Josef"
          },
          "Reason": {
            "S": "Chess Pain"
          },
          "EntityType": {
            "S": "CASE"
          }
        },
        {
          "PK": {
            "S": "C#2"
          },
          "SK": {
            "S": "D#1"
          },
          "Fname": {
            "S": "Angelo"
          },
          "Lname": {
            "S": "Boustany"
          },
          "Email": {
            "S": "angelo@gmail.com"
          },
          "EntityType": {
            "S": "DOCTOR"
          }
        },
        {
          "PK": {
            "S": "C#2"
          },
          "SK": {
            "S": "D#2"
          },
          "Fname": {
            "S": "Fred"
          },
          "Lname": {
            "S": "Lavita"
          },
          "Email": {
            "S": "lavita@gmail.com"
          },
          "EntityType": {
            "S": "CASE"
          }
        },
        {
          "PK": {
            "S": "C#2"
          },
          "SK": {
            "S": "I#2023"
          },
          "Time": {
            "N": "1683735462"
          },
          "Date": {
            "S": " 2023-5-10"
          },
          "CaseSeverity": {
            "S": "HIGH"
          },
          "Diagnosis": {
            "S": "Patient Diagnosised with Chain Pain could be hear failure"
          },
          "ProcedureCost": {
            "S": "1200$"
          },
          "PID": {
            "S": "P#1"
          },
          "Severity Level": {
            "N": "3"
          }
        },
        {
          "PK": {
            "S": "P#1"
          },
          "SK": {
            "S": "I#2023"
          },
          "Fname": {
            "S": "Alfred"
          },
          "Lname": {
            "S": "Vastovo"
          },
          "Email": {
            "S": "alfred@gmail.com"
          },
          "Phone": {
            "N": "75894632"
          },
          "Age": {
            "N": "21"
          },
          "Gender": {
            "S": "Male"
          },
          "Location": {
            "S": "Debbieh, Chouf"
          },
          "EntityType": {
            "S": "PATIENT"
          }
        },
        {
          "PK": {
            "S": "S#1"
          },
          "SK": {
            "S": "I#2023"
          },
          "Info": {
            "S": "{'Type':'Xray'}"
          }
        },
        {
          "PK": {
            "S": "S#1"
          },
          "SK": {
            "S": "C#1"
          },
          "PID": {
            "S": "P#1"
          },
          "RID": {
            "S": "R#1"
          }
        },
        {
          "PK": {
            "S": "C#1"
          },
          "SK": {
            "S": "I#1"
          },
          "Time": {
            "N": "1683737378"
          },
          "Date": {
            "S": "2022-5-10"
          },
          "CaseSeverity": {
            "S": "LOW"
          },
          "ScanDate": {
            "S": "1683737378"
          },
          "Diagnosis": {
            "S": "Patient Diagnosed with broken bones"
          },
          "ProcedureCost": {
            "S": "500$"
          },
          "PID": {
            "S": "P#2"
          },
          "Severity Level": {
            "N": "2"
          }
        },
        {
          "PK": {
            "S": "D#1"
          },
          "SK": {
            "S": "C#1"
          },
          "Time": {
            "N": "1683737378"
          },
          "Location": {
            "S": "Beirut, Hopital Saint Josef"
          },
          "Reason": {
            "S": "Broken bones"
          },
          "EntityType": {
            "S": "CASE"
          }
        },
        {
          "PK": {
            "S": "P#1"
          },
          "SK": {
            "S": "EHR"
          },
          "Time": {
            "N": "1546344000"
          },
          "MedicationName": {
            "S": "Panadol"
          },
          "BloodPressure": {
            "S": "140"
          },
          "PID": {
            "S": "P#1"
          },
          "DiagnosisTime": {
            "N": "1683738942"
          }
        },
        {
          "PK": {
            "S": "C#1"
          },
          "SK": {
            "S": "S#1"
          },
          "ScanDate": {
            "S": "1683735462"
          }
        }
      ],'''


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
    #input = {"Statement": "SELECT ProcedureCost FROM SynapseAIDB WHERE PK = 'C#1' AND SK = 'I#2023'"}

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
