from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):

    template_fields = ("query",)
    ui_color = '#F98866'

    insert_sql = 'INSERT INTO {} {}'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 query="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.query = query
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Loading data from staging into {}".format(self.table))
        rendered_query = self.query.format(**context)
        formatted_sql = LoadFactOperator.insert_sql.format(
            self.table,
            rendered_query
        )
        redshift.run(formatted_sql)
