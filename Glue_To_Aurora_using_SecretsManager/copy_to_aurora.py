from pg import DB, connect
import json

import boto3
from botocore.exceptions import ClientError

import sys
from awsglue.utils import getResolvedOptions

###
# Params

args = getResolvedOptions(sys.argv, ['table_name', 'imp_path', 'imp_file'])

table_name = args['table_name']  
imp_path = args['imp_path']  
imp_file = args['imp_file']  

#
###


def get_secret(p_secret_name):
    region_name = "us-east-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=region_name)
    try:
        get_secret_value_response = client.get_secret_value(SecretId=p_secret_name)
    except ClientError as e:
        raise e
    else:
        secret = get_secret_value_response['SecretString']
        return json.loads(secret)


json_secr = get_secret("AuroraDBCreds")
v_dbname = 'db_test'
db = DB(dbname=v_dbname, host=json_secr["host"], port=json_secr["port"],
        user=json_secr["username"], passwd=json_secr["password"])

sqll = "delete from {0}".format(table_name)
db.query(sqll)

imp_region = 'us-east-1'

sqll = """
SELECT  aws_s3.table_import_from_s3(
   '{0}', '', '(format csv, NULL ''NULL'')',
   aws_commons.create_s3_uri('{1}','{2}','us-east-1')
)
""".format(table_name, imp_path, imp_file)

print(sqll)
db.query(sqll)

db.close()
