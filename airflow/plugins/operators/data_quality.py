from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tables=[],
                 columns='*',
                 queries=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables
        self.columns = columns
        self.queries = queries

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info('Do Data Quality Check')
        for t in self.tables:
            for q in self.queries:
                formatted_sql = q.format(t, self.columns)
                result = redshift.get_records(formatted_sql)[0][0]
                if result == True:
                    self.log.info("Data Check unsucessfull")
                    raise ValueError(
                        f"Data quality check on table {t} failed.")
            self.log.info(f"Data quality on table {t} check passed.")
