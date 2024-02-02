import configparser
import psycopg2
import boto3
import threading
import json
import pandas as pd
import time

config = configparser.ConfigParser()
config.read('dwh.cfg')

KEY = config['AWS']['KEY']
SECRET = config['AWS']['SECRET']
DWH_IAM_ROLE_NAME = config['CLUSTER']['DWH_IAM_ROLE_NAME']
DWH_CLUSTER_IDENTIFIER = config['CLUSTER']['DWH_CLUSTER_IDENTIFIER']
DWH_CLUSTER_TYPE = config['CLUSTER']['DWH_CLUSTER_TYPE']
DWH_NODE_TYPE = config['CLUSTER']['DWH_NODE_TYPE']
DWH_NUM_NODES = config['CLUSTER']['DWH_NUM_NODES']
DB_NAME = config['CLUSTER']['DB_NAME']
DB_USER = config['CLUSTER']['DB_USER']
DB_PASSWORD = config['CLUSTER']['DB_PASSWORD']

def create_IAM_role() -> None:
    """create IAM role in redshift
    """    

    iam = boto3.client('iam',aws_access_key_id=KEY,
                     aws_secret_access_key=SECRET,
                     region_name='us-west-2'
                  )
    try:
        print('1.1 Creating a new IAM Role')
        dwhRole = iam.create_role(
            Path='/',
            RoleName=DWH_IAM_ROLE_NAME,
            Description="Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {
                    'Statement': [{'Action': 'sts:AssumeRole',
                    'Effect': 'Allow',
                    'Principal': {'Service': 'redshift.amazonaws.com'}}],
                    'Version': '2012-10-17'
                }
            )
        )
    except Exception as e:
        print(e)

def get_IAM_role() -> None:
    """get IAM role in redshift after creation
    """   
    iam = boto3.client('iam',aws_access_key_id=KEY,
                     aws_secret_access_key=SECRET,
                     region_name='us-west-2'
                  )
    try:
        iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                            PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                            )['ResponseMetadata']['HTTPStatusCode']

        roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']

        return roleArn
    
    except Exception as e:
        print(e)

def delete_IAM_role() -> None:
    """delete IAM role in redshift after creation
    """   
    iam = boto3.client('iam',aws_access_key_id=KEY,
                     aws_secret_access_key=SECRET,
                     region_name='us-west-2'
                  )
    try:
        iam.detach_role_policy(RoleName=DWH_IAM_ROLE_NAME, 
            PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
            )
        iam.delete_role(RoleName=DWH_IAM_ROLE_NAME)
    except Exception as e:
        print(e)

def create_cluster(arn):
    """create cluster in redshift after creation
    """   
    print('2.1 Creating a new cluster')
    redshift = boto3.client('redshift',
                       region_name="us-west-2",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                       )
    if get_cluster_status() == 'non-exist':
        pass 

    while get_cluster_status() == 'deleting':
        print('cluster is being deleted and will create a new cluster until the deletion is completed')
        time.sleep(10)
    try:
        response = redshift.create_cluster(        
            # TODO: add parameters for hardware
            ClusterType=DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,
            NumberOfNodes=int(DWH_NUM_NODES),

            # TODO: add parameters for identifiers & credentials
            DBName=DB_NAME,
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            MasterUsername=DB_USER,
            MasterUserPassword=DB_PASSWORD,
            
            #Roles (for s3 access)
            IamRoles=[arn]  
            
        )
    except Exception as e:
        print(e)

def prettyRedshiftProps(props:dict) -> pd.DataFrame:
        """output redshift properties to dataframe

        Args:
            props (redshiftProperty): properties of redshift cluster

        Returns:
            status: return properties of cluster. 
        """        
        keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
        x = [(k, v) for k,v in props.items() if k in keysToShow]

        return pd.DataFrame(data=x, columns=["Key", "Value"])

def get_cluster_status() -> str:
    """get cluster status in redshift after creation
    """  

    try:
        redshift = boto3.client('redshift',
                        region_name="us-west-2",
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET
                        )

        myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
        redshiftProperty = prettyRedshiftProps(myClusterProps)
        status = redshiftProperty[redshiftProperty['Key']=='ClusterStatus']['Value'].values[0]
        return status
    except Exception as e:
        return 'non-exist'



def get_cluster_name() -> str:
    """get endpoint of cluster

    Returns:
        str: endpoint of cluster
    """     
    try:
        redshift = boto3.client('redshift',
                        region_name="us-west-2",
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET
                        )

        myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
        redshiftProperty = prettyRedshiftProps(myClusterProps)
        retries_count = 1
        while (redshiftProperty[redshiftProperty['Key']=='ClusterStatus']['Value'].values[0] != 'available'):

            print(f'{retries_count}: tries to get EndPoint')
            myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
            redshiftProperty = prettyRedshiftProps(myClusterProps)
            time.sleep(10)
            retries_count += 1

        return redshiftProperty[redshiftProperty['Key']=='Endpoint']['Value'].values[0]['Address']  
    except Exception as e:
        print(e)

def delete_cluster() -> None:
    """Delete cluster
    """    
    try:
        redshift = boto3.client('redshift',
                        region_name="us-west-2",
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET
                        )
    
        redshift.delete_cluster(
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER, 
            SkipFinalClusterSnapshot=True
            )
    except Exception as e:
        print(e)