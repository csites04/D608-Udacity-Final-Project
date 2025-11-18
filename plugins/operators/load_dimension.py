from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.postgres.hooks.postgres import PostgresHook

class LoadDimensionOperator(BaseOperator):

    ui_color = "#80BD9E"

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id="",
        table="",
        sql_query="",
        append_only=False,
        *args,
        **kwargs
    ):
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query = sql_query
        self.append_only = append_only

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f"Starting load for dimension table {self.table}")

        # DELETE-LOAD MODE (default)
        if not self.append_only:
            self.log.info(f"Clearing existing data from {self.table}")
            redshift.run(f"DELETE FROM {self.table};")

        # Always INSERT
        insert_sql = f"""
            INSERT INTO {self.table}
            {self.sql_query}
        """

        self.log.info(f"Running INSERT for {self.table}")
        redshift.run(insert_sql)

        self.log.info(f"Dimension table {self.table} load complete.")
