from airflow.models import BaseOperator
from airflow.utils.decorators import  apply_defaults

from airflow.hooks.postgres_hook import PostgresHook





class CreateRedshiftTables(BaseOperator):

    @apply_defaults
    def __init__(self,redshift_conn_id,script_path,*args,**kwargs):
        super(CreateRedshiftTables,self).__init__(*args,**kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.script_path = script_path

    def execute(self, context):
        redshift = PostgresHook(self.redshift_conn_id)

        file_script = open(self.script_path,'r')
        creation_script = file_script.read()
        file_script.close()


        sql_queries = creation_script.split(';')
        for sql_query in sql_queries:
            if sql_query.rstrip() != '':
                redshift.run(sql_query)