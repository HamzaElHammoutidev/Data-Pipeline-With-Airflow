from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.postgres_hook import PostgresHook



class DataQualityOperator(BaseOperator):


    @apply_defaults
    def __init__(self,redshift_conn_id,tables,*args,**kwargs):
        super(DataQualityOperator,self).__init__(*args,**kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self, context):
        redshift = PostgresHook(self.redshift_conn_id)
        for table in self.tables:
            records = redshift.get_records("SELECT COUNT(*) FROM {}".format(table))
            if len(records) < 1 or len(records[0]) < 1 or len(records[0][0]) < 1:
                raise ValueError("Data quality Check Failed form {}".format(table))
            self.log.info("Data Quality Check Succeded for {}, width {} rows".format(table,records[0][0]))