from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 sql ="",
                 table="",
                 truncate = False,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.sql = sql
        self.table=table
        self.truncate = truncate
        
    def execute(self, context):
        self.log.info('LoadFactOperator EXECUTING')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.truncate:
            self.log.info(f"Truncate {self.table} table")
            redshift.run(f"TRUNCATE {self.table}" )
            
        insert_sql = f"INSERT INTO {self.table} {self.sql}"
        self.log.info(f"Loading {self.table} table")
        redshift.run(insert_sql)
