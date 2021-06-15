from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    fact_table_template = """
    INSERT INTO {table}
    {sql_source} 
    """
 
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql_source = "",                 
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_source = sql_source        
        
    def execute(self, context):
        self.log.info('Load fact started')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        fact_sql = LoadFactOperator.fact_table_template.format(
            table=self.table,
            sql_source = self.sql_source)
        redshift.run(fact_sql)
        self.log.info('Load fact Ok')    

        