from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 redshift_conn_id:str='',
                 aws_credentials_id:str='',
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.qa_checks = kwargs["params"]["qa_check"]

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        for qa_check in self.qa_checks:
            records = redshift.get_records(qa_check['check_sql'])
            num_records = records[0][0]
            if num_records != qa_check['expected_result']:
                raise ValueError(f"Data quality check failed. {qa_check['check_sql']} is not equal to the expected result {qa_check['expected_result']}")
            
            self.log.info(f"Data quality on query {qa_check['check_sql']} check passed with {num_records} records") 