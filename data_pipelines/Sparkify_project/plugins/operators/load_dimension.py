from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 destination_table = "",
                 sql_query = "",
                 update_mode = "overwrite",
                 provide_context=True,
                 aws_credentials_id="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.destination_table=destination_table
        self.sql_query=sql_query
        self.update_mode=update_mode

    def execute(self, context):
        self.log.info('Connecting to Redshift')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info('Loading data to dimension table {}'.format(self.destination_table))
        if self.update_mode == 'overwrite':
            update_query = 'TRUNCATE {}; INSERT INTO {} ({})'.format(self.destination_table, self.destination_table, self.sql_statement)
        elif self.update_mode == 'insert':
            update_query = 'INSERT INTO {} ({})'.format(self.destination_table, self.sql_query)
        redshift.run(update_query)