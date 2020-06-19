from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults






class LoadDimensionOperator(BaseOperator):

    @apply_defaults
    def __init__(self,redshift_conn_id,table,sql_query,truncate,*args,**kwargs):
        super(LoadDimensionOperator,self).__init__(*args,**kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query = sql_query
        self.truncate = truncate


    def execute(self,context):
        redshift = PostgresHook(self.redshift_conn_id)
        if self.truncate:
            redshift.run("DELETE FROM {} ".format(self.table))

        redshift.run(self.sql_query)
        self.log.info(f"Success: {self.task_id}")
