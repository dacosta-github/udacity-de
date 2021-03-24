from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

"""
     - this module represents the operator to execute queries that allow 
       loading the data from the staging tables to the fact tables, in the redshift cluster
"""
class LoadFactOperator(BaseOperator):
    
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 table,
                 redshift_conn_id='redshift',
                 select_sql='',
                 *args, **kwargs):
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.select_sql = select_sql

    def execute(self, context):
        self.log.info('Running LoadFactOperator for {}'.format(self.table))

        redshift_hook = PostgresHook("redshift")
        self.log.info(f'Loading data into {self.table} fact table...')

        sql = f"""
            INSERT INTO {self.table}
            {self.select_sql};
        """
        
        redshift_hook.run(sql)
        self.log.info("Loading complete.")