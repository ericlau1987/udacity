from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                redshift_conn_id:str='',
                aws_credentials_id:str='',
                schema:str='',
                table:str='',
                sql:str='',
                insert_type:str='delete-load',
                *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.schema = schema
        self.table = table
        self.sql = sql
        self.insert_type = insert_type

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info(f'{self.schema}.{self.table} is being inserted')
        if self.insert_type == 'delete-load':
            redshift.run(f'delete from {self.schema}.{self.table}')
        elif self.insert_type == 'append':
            pass
        redshift.run(self.sql.format(self.schema, self.table))