from datetime import datetime
from decimal import Decimal
import logging

import boto3
from botocore.exceptions import ClientError


logger = logging.getLogger(__name__)


class PartiQLWrapper:

    def __init__(self, dyn_resource):

        self.dyn_resource = dyn_resource

    def run_partiql(self, statement, params):
        """
        Runs a PartiQL statement. A Boto3 resource is used even though
        `execute_statement` is called on the underlying `client` object because the
        resource transforms input and output from plain old Python objects (POPOs) to
        the DynamoDB format. If you create the client directly, you must do these
        transforms yourself.

        :param statement: The PartiQL statement.
        :param params: The list of PartiQL parameters. These are applied to the
                       statement in the order they are listed.
        :return: The items returned from the statement, if any.
        """
        try:
            output = self.dyn_resource.meta.client.execute_statement(
                Statement=statement, Parameters=params)
        except ClientError as err:
            if err.response['Error']['Code'] == 'ResourceNotFoundException':
                logger.error(
                    "Couldn't execute PartiQL '%s' because the table does not exist.",
                    statement)
            else:
                logger.error(
                    "Couldn't execute PartiQL '%s'. Here's why: %s: %s", statement,
                    err.response['Error']['Code'], err.response['Error']['Message'])
            raise
        else:
            return output


def run_scenario(wrapper, table_name):
    logging.basicConfig(level=logging.INFO,
                        format='%(levelname)s: %(message)s')


if __name__ == '__main__':
    try:
        dyn_res = boto3.resource('dynamodb')
        synapseDB = PartiQLWrapper(dyn_res)
        run_scenario(synapseDB, 'testDB')
    except Exception as e:
        print(f"Something went wrong with the demo! Here's what: {e}")
