from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

"""
     - this module represents the operator to execute queries
       that allow apply data quality rules to the tables defined as dimensional and fact.
"""
class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='redshift',
                 operations=[],
                 *args, **kwargs):
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.operations = operations

    def execute(self, context):
        self.log.info('Running DataQualityOperator')
        redshift_hook = PostgresHook("redshift")

        self.log.info(f"Starting check ")

        for stmt in self.operations:
            result = int(redshift_hook.get_first(sql=stmt['sql'])[0])

            # check if equal
            if stmt['operation'] == 'error':
                if result != stmt['target']:
                    raise AssertionError(f"Check failed: {result} {stmt['operation']} {stmt['target']}")
            
            # check if greater than
            elif stmt['operation'] == 'validation':
                if result <= stmt['target']:
                    raise AssertionError(f"Check failed: {result} {stmt['operation']} {stmt['target']}")
                if result > stmt['target']:
                    raise AssertionError(f"Check passed: {result} {stmt['operation']} {stmt['target']}")

            self.log.info(f"Passed check: {result} {stmt['operation']} {stmt['target']}")