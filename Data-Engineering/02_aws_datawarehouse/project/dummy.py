import boto3
import configparser
import json

config = configparser.ConfigParser()
config.read('dwh.cfg')
KEY, SECRET = config['AWS']['KEY'], config['AWS']['SECRET']
s3 = boto3.resource('s3',
                    region_name='us-west-2',
                    aws_access_key_id=KEY,
                    aws_secret_access_key=SECRET
                    )

content_object = s3.Object('udacity-dend', 'log_json_path.json')
file_content = content_object.get()['Body'].read().decode('utf-8')
# json_content = json.loads(file_content)
print(file_content)