from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self, redshift_conn_id='redshift', table='', sql_query='', mode='', *args, **kwargs):
        
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query = sql_query
        self.mode = mode
        

    def execute(self, context):
        if self.mode.lower() == 'truncate':
            query = f"""
                TRUNCATE {self.table}; INSEET INTO {self.table} {self.sql_query}
                """
        if self.mode.lower() == 'insert':
            query = f"""
                INSEET INTO {self.table} {self.sql_query}
                """
        redshift_hook = PostgresHook(self.redshift_conn_id)
        redshift_hook.run(query)
        self.log.info(f'{self.mode} into {self.table}')
