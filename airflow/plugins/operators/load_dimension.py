from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    insert_sql = 'INSERT INTO {} {}'
    delete_sql = 'DELETE FROM {}'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 query="",
                 append=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.query = query
        self.append = append
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.append == False:
            self.log.info("Deleting rows from {} ...".format(self.table))
            formatted_delete_sql = LoadDimensionOperator.delete_sql.format(
                self.table
            )
            redshift.run(formatted_delete_sql)

        self.log.info("Loading data from staging into {}".format(self.table))
        formatted_insert_sql = LoadDimensionOperator.insert_sql.format(
            self.table,
            self.query
        )
        redshift.run(formatted_insert_sql)
