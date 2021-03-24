from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

"""
     - this module represents the operator to execute queries that allow loading 
       the data from the staging tables to the dimensions tables, in the redshift cluster
"""
class LoadDimensionOperator(BaseOperator):
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 table,
                 redshift_conn_id='redshift',
                 select_sql='',
                 mode='APPEND',
                 *args, **kwargs):
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.select_sql = select_sql
        self.mode = mode

    def execute(self, context):
        self.log.info('Running LoadDimensionOperator for {} - {}'.format(self.select_sql, self.mode))
            
        redshift_hook = PostgresHook("redshift")

        ## delete data from dimension table
        if self.mode == 'DELETE':
            self.log.info(f'Deleting data from {self.table} dimension table...')
            redshift_hook.run(f'DELETE FROM {self.table};')
            self.log.info("Delete complete.")

        self.log.info(f'Inserting data to {self.table} dimension table...')
        sql = f"""
            INSERT INTO {self.table}
            {self.select_sql};
        """
        self.log.info("Insert complete.")


        self.log.info(f'Loading data into {self.table} dimension table...')
        redshift_hook.run(sql)
        self.log.info("Loading complete.")