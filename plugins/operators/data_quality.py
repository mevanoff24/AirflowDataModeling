from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow import AirflowException


class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self, test_queries, test_conditions, redshift_conn_id='redshift',
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.test_queries = test_queries
        self.test_conditions = test_conditions
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        self.log.info('DataQualityOperator')
        redshift_hook = PostgresHook(self.redshift_conn_id)

        for i, query in enumerate(self.test_queries):
            rows = redshift_hook.get_records(query)

            first_row = rows[0]
            actual_row_count = first_row[0]
            expected_row_count = self.test_conditions[i]
            if actual_row_count < expected_row_count:
                raise AirflowException(f"Failed min row count; \
                actual:{actual_row_count}, expected: {expected_row_count}")