from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowSkipException


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    template_fields = ("s3_key",)
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        CSV
        IGNOREHEADER 1
    """

    delete_sql = 'DELETE FROM {}'

    @apply_defaults
    def __init__(self,
                 aws_credentials_id="",
                 redshift_conn_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 append=True,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.aws_credentials_id = aws_credentials_id
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.append = append

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id, client_type="s3")
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.append == False:
            self.log.info("Deleting rows from {} ...".format(self.table))
            formatted_delete_sql = StageToRedshiftOperator.delete_sql.format(
                self.table,
            )
            redshift.run(formatted_delete_sql)

        rendered_key = self.s3_key.format(**context)
        s3 = S3Hook(aws_conn_id=self.aws_credentials_id)
        s3_keys = s3.list_keys(
            prefix=rendered_key, bucket_name=self.s3_bucket)

        if len(s3_keys) > 0:
            self.log.info("Copying data from S3 to Redshift")
            s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
            formatted_copy_sql = StageToRedshiftOperator.copy_sql.format(
                self.table,
                s3_path,
                credentials.access_key,
                credentials.secret_key,
            )
            redshift.run(formatted_copy_sql)
        else:
            self.log.info("No file with this key")
            raise AirflowSkipException
