import logging
import boto3
from botocore.exceptions import ClientError
logger = logging.getLogger(__name__)

class SynapseAI:
    def __init__(self, dyn_resource):

        self.dyn_resource = dyn_resource
        self.table = None

    def create_table(self, table_name, partitiom_key, att_type1, sort_key=None, att_type2=None):
        try:
            if sort_key:
                self.table = self.dyn_resource.create_table(
                    TableName=table_name,
                    KeySchema=[
                        {
                            'AttributeName': partitiom_key,
                            'KeyType': 'HASH'  # Partition key
                        },
                        {
                            'AttributeName': sort_key,
                            'KeyType': 'RANGE'  # Sort key
                        }
                    ],
                    AttributeDefinitions=[
                        {
                            'AttributeName': partitiom_key,
                            'AttributeType': att_type1  # String data type
                        },
                        {
                            'AttributeName': sort_key,
                            'AttributeType': att_type2  # String data type
                        },
                    ],
                    BillingMode='PAY_PER_REQUEST'
                )
            else:
                self.table = self.dyn_resource.create_table(
                    TableName=table_name,
                    KeySchema=[
                        {
                            'AttributeName': partitiom_key,
                            'KeyType': 'HASH'  # Partition key
                        }
                    ],
                    AttributeDefinitions=[
                        {
                            'AttributeName': partitiom_key,
                            'AttributeType': 'N'  # String data type
                        }
                    ],
                    BillingMode='PAY_PER_REQUEST'
                )
            self.table.wait_until_exists()
        except ClientError as err:
            logger.error(
                "Couldn't create table %s. Here's why: %s: %s", table_name,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise            

    def create_table_EHR(self, table_name):
        try:
            self.table = self.dyn_resource.create_table(
                TableName=table_name,
                KeySchema=[
                    {
                        'AttributeName': 'Patient_id',
                        'KeyType': 'HASH'  # Partition key
                    },
                    {
                        'AttributeName': 'Record_date',
                        'KeyType': 'RANGE'  # Sort key
                    }
                ],
                AttributeDefinitions=[
                    {
                        'AttributeName': 'Patient_id',
                        'AttributeType': 'N'  # String data type
                    },
                    {
                        'AttributeName': 'Record_date',
                        'AttributeType': 'S'  # String data type
                    },
                ],
                BillingMode='PAY_PER_REQUEST'
            )
            self.table.wait_until_exists()
        except ClientError as err:
            logger.error(
                "Couldn't create table %s. Here's why: %s: %s", table_name,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise
        else:
            return self.table

    def create_table_Radiologist(self, table_name2):
        try:
            self.table = self.dyn_resource.create_table(
                TableName=table_name2,
                KeySchema=[
                    {
                        'AttributeName': 'Radiologist_id',
                        'KeyType': 'HASH'  # Partition key
                    },
                    {
                        'AttributeName': 'Scan_id',
                        'KeyType': 'RANGE'  # Sort key
                    }
                ],
                AttributeDefinitions=[
                    {
                        'AttributeName': 'Radiologist_id',
                        'AttributeType': 'N'  # String data type
                    },
                    {
                        'AttributeName': 'Scan_id',
                        'AttributeType': 'N'  # String data type
                    },
                ],
                BillingMode='PAY_PER_REQUEST'
            )
            self.table.wait_until_exists()
        except ClientError as err:
            logger.error(
                "Couldn't create table %s. Here's why: %s: %s", table_name2,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise
        else:
            return self.table

    def gsi(self, table_name):
        try:
            self.table = self.dyn_resource.Table(table_name)
            self.table.update(
                AttributeDefinitions=[
                    {
                        'AttributeName': 'BLood_Type',
                        'AttributeType': 'S'  # data type of the partition key attribute
                    }
                ],
                TableName='Radiologist',
                BillingMode='PAY_PER_REQUEST',
                GlobalSecondaryIndexUpdates=[
                    {
                        'Create': {
                            'IndexName': 'BLood_Type_Index',  # specify the name of the index
                            'KeySchema': [
                                {
                                    'AttributeName': 'BLood_Type',
                                    'KeyType': 'HASH'  # specify the partition key as HASH type
                                }
                                # add more attributes for the index's sort key if necessary
                            ],
                            'Projection': {
                                'ProjectionType': 'ALL'  # specify which attributes should be projected to the index
                            },
                        }
                    }
                ]
            )


            self.table.wait_until_exists()
        except ClientError as err:
            logger.error(
                "Couldn't create table %s. Here's why: %s: %s", table_name,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise
        else:
            return self.table

    def create_table_Doctors(self, table_name3):
        try:
            self.table = self.dyn_resource.create_table(
                TableName=table_name3,
                KeySchema=[
                    {
                        'AttributeName': 'Doctor_id',
                        'KeyType': 'HASH'  # Partition key
                    }
                ],
                AttributeDefinitions=[
                    {
                        'AttributeName': 'Doctor_id',
                        'AttributeType': 'N'  # String data type
                    }
                ],
                BillingMode='PAY_PER_REQUEST'
            )
            self.table.wait_until_exists()
        except ClientError as err:
            logger.error(
                "Couldn't create table %s. Here's why: %s: %s", table_name3,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise
        else:
            return self.table
    
    def create_table_Scans(self, table_name4):
        try:
            self.table = self.dyn_resource.create_table(
                TableName=table_name4,
                KeySchema=[
                    {
                        'AttributeName': 'Scan_id',
                        'KeyType': 'HASH'  # Partition key

                    },
                    {
                        'AttributeName': 'Patient_id',
                        'KeyType': 'RANGE'  # Sort key
                    }
                ],
                AttributeDefinitions=[
                    {
                        'AttributeName': 'Scan_id',
                        'AttributeType': 'N'  # String data type
                    },
                    {
                        'AttributeName': 'Patient_id',
                        'AttributeType': 'N'  # String data type
                    },
                    
                ],
                BillingMode='PAY_PER_REQUEST'
            )
            self.table.wait_until_exists()
        except ClientError as err:
            logger.error(
                "Couldn't create table %s. Here's why: %s: %s", table_name4,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise
        else:
            return self.table

    def create_table_Appointment(self, table_name5):
        try:
            self.table = self.dyn_resource.create_table(
                TableName=table_name5,
                KeySchema=[
                    {
                        'AttributeName': 'Appointment_id',
                        'KeyType': 'HASH'  # Partition key

                    }
                ],
                AttributeDefinitions=[
                    {
                        'AttributeName': 'Appointment_id',
                        'AttributeType': 'N'  # String data type
                    }
                ],
                BillingMode='PAY_PER_REQUEST'
            )
            self.table.wait_until_exists()
        except ClientError as err:
            logger.error(
                "Couldn't create table %s. Here's why: %s: %s", table_name5,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise
        else:
            return self.table

    def additem_Radiologist(self,rid,scid,fn,ln,email,phone,availability,expertise,created_at):
        try:
            self.table = self.dyn_resource.Table('Radiologist')
            response = self.table.put_item(
                Item={
                    'Radiologist_id': rid,
                    'Scan_id': scid,
                    'First_Name': fn,
                    'Last_Name': ln,
                    'Email': email,
                    'Phone': phone,
                    'Availability': availability,
                    'Expertise': expertise,
                    'Created_At': created_at
                }
            )
        except ClientError as err:
            logger.error(
                "Couldn't add item to table %s. Here's why: %s: %s", 'Radiologist',
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise
        else:
            return response

    def delete_table(self, table_name):
        try:
            self.table = self.dyn_resource.Table(table_name)
            self.table.delete()
        except ClientError as err:
            logger.error(
                "Couldn't delete table %s. Here's why: %s: %s", table_name,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise
        else:
            return self.table
        
    def getitem(self, table_name, key):
        try:
            self.table = self.dyn_resource.Table(table_name)
            response = self.table.get_item(Key=key)
        except ClientError as err:
            logger.error(
                "Couldn't get item from table %s. Here's why: %s: %s", table_name,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise
        else:
            return response['Item']
        
if __name__ == '__main__':
    try:
        dyn_res = boto3.resource('dynamodb')
        ehr = SynapseAI(dyn_res)
        # ehr.create_table_EHR('EHR')
        #ehr.create_table_Radiologist('Radiologist')
        #ehr.create_table_Doctors('Doctors')
        #ehr.create_table_Scans('Scans')
        #ehr.create_table_Appointment('Appointment')
        #ehr.additem_Radiologist(1,1,'John','Doe','example@gmail.com,',1234567890,'Yes','Cardiovascular','2021-05-19 14:30:59.000000')
        #ehr.delete_table('Radiologist')
        #ehr.gsi('Radiologist')

    except Exception as e:
        print(f"Something went wrong with the demo! Here's what: {e}")