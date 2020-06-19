from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
# from airflow.operators.postgres_operator import PostgresOperator
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageS3ToRedshiftOperator(BaseOperator):
    template_fields = ("s3_key",)
    copy_sql= """COPY {} FROM {} ACCESS_KEY '{}' SECRET_KEY '{}' REGION '{}' TIMEFORMAT as 'epochmillisecs' 
                TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL {} 'auto' {} 
    """

    @apply_defaults
    def __init__(self,aws_connection,table,redshift_conn_id,s3_bucket,s3_key,region,file_format="JSON",*args,**kwargs):
        super(StageS3ToRedshiftOperator,self).__init__(*args,**kwargs)
        self.aws_connection = aws_connection
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region = region
        self.file_format = file_format
        self.execution_date = kwargs.get("execution_date")

    def execute(self,context):
        aws_hook = AwsHook(self.aws_connection)
        redshift = PostgresHook(self.redshift_conn_id)
        credentials = aws_hook.get_credentials()
        redshift.run("DELETE FROM {}".format(self.table))

        s3_path = "s3://{}".format(self.s3_bucket,self.s3_key)

        if self.execution_date: # Backfilling
            day = self.execution_date.strftime('%d')
            month = self.execution_date.strftime('%M')
            year = self.execution_date.strftime('%Y')
            s3_path = s3_path+'/'.join([s3_path,year,month,day])

        s3_path = s3_path + '/' + self.s3_key

        if self.file_format == "CSV":
            additinal_csv_format = " DELIMETER ',' IGNOREHEADER 1 "

        sql_query = StageS3ToRedshiftOperator.copy_sql.format(
            self.table,s3_path,credentials.acces_key,credentials.secret_key,
            self.region,self.file_format,additinal_csv_format
        )

        redshift.run(sql_query)


