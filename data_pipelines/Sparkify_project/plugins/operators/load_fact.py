from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 destination_table = "",
                 redshift_conn_id = "",
                 sql_query = "",
                provide_context = True,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.destination_table=destination_table
        self.sql_query=sql_query

    def execute(self, context):
        self.log.info('Connecting to Redshift')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info('Loading data to fact table')
        insert_query = 'INSERT INTO {} ({})'.format(self.destination_table, self.sql_query)
        redshift.run(insert_query)