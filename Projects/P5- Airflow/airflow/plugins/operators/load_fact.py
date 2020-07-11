from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 load_sql_stmt="",
                 append_data=False,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.append_data = append_data
        self.load_sql_stmt = load_sql_stmt

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Loading fact table into redshift")

        if not self.append_data:
            sql_statement = 'DELETE FROM %s' % self.table_name
            redshift.run(sql_statement)

        insert_sql = """
        INSERT INTO {}
        {};
        """
        formatted_sql = insert_sql.format(
            self.table,
            self.load_sql_stmt
        )
        redshift.run(formatted_sql)