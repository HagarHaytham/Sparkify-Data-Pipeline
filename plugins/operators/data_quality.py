from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id = "",
                 tests=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.tests = tests

    def execute(self, context):
        self.log.info('DataQualityOperator EXECUTING')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)        
        for test in self.tests:
            sql = test['sql']
            expected_result = test['expected_result']
            self.log.info(f"Testing {sql} Against {expected_result}")
            actual_result = redshift.get_records(sql)[0]
            
            if actual_result[0] != expected_result :
                self.log.info(f"Data quality {sql} test failed with result {actual_result}.") 
                raise ValueError("Data quality check failed.")
            else:
                self.log.info(f"Data quality {sql} test passed with result {actual_result}.") 
        self.log.info("All Data quality tests finished")
        