from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    dimesion_table_template = """
    INSERT INTO {table}
    {sql_source}     
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql_source = "",
                 append_table = "",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_source = sql_source
        self.append_table = append_table 
        
    def execute(self, context):
        self.log.info('Load dimension started')
        if self.append_table == False:
              sql_statement = 'TRUNCATE TABLE %s' % self.table
              redshift.run(sql_statement)
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        dimension_sql = LoadDimensionOperator.dimesion_table_template.format(
            table=self.table,
            sql_source = self.sql_source)
        redshift.run(dimension_sql)
        self.log.info('Load dimension Ok')        
