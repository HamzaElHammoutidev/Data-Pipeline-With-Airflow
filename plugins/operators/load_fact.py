from airflow.models import BaseOperator

from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook


from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):

    @apply_defaults
    def __init__(self,redshift_connection_id,sql_query,*args,**kwargs):
        self.redshift_connection_id = redshift_connection_id
        self.sql_query = sql_query

    def execute(self, context):
        redshift = PostgresHook(self.redshift_connection_id)
        redshift.run(self.sql_query)