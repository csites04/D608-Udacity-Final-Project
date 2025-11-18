from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.postgres.hooks.postgres import PostgresHook

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id="",
        dq_checks=None,
        *args,
        **kwargs
    ):
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.dq_checks = dq_checks or []

    def execute(self, context):

        if not self.dq_checks:
            raise ValueError("No data quality checks provided.")

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Starting data quality checks...")

        for idx, check in enumerate(self.dq_checks, start=1):

            sql = check.get("check_sql")
            expected = check.get("expected_result")

            if sql is None or expected is None:
                raise ValueError(f"DQ check #{idx} is missing required fields: {check}")

            self.log.info(f"DQ Check {idx}: Running SQL → {sql}")

            records = redshift.get_records(sql)

            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check #{idx} FAILED: No results returned.")

            actual = records[0][0]

            if actual != expected:
                raise ValueError(
                    f"Data quality check #{idx} FAILED\n"
                    f"SQL: {sql}\n"
                    f"Expected: {expected}\n"
                    f"Got: {actual}"
                )

            self.log.info(
                f"✔️ Data quality check #{idx} PASSED: Expected {expected}, got {actual}"
            )

        self.log.info("All data quality checks passed successfully!")
