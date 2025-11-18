from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook

class StageToRedshiftOperator(BaseOperator):
    """
    Loads JSON data from S3 into a Redshift staging table using a COPY command.
    Uses parameters dynamically and validates connection via hooks.
    """

    ui_color = '#358140'
    template_fields = ("s3_key",)

    @apply_defaults
    def __init__(self,
                 redshift_conn_id: str = "",
                 aws_conn_id: str = "",    
                 table: str = "",
                 s3_bucket: str = "",
                 s3_key: str = "",
                 json_path: str = "auto",
                 region: str = "us-west-2",
                 *args, **kwargs):

        super().__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_conn_id = aws_conn_id   
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_path = json_path
        self.region = region

    def execute(self, context):
        self.log.info(f"Starting StageToRedshiftOperator for table: {self.table}")

        aws_hook = AwsBaseHook(self.aws_conn_id, client_type="s3")
        credentials = aws_hook.get_credentials()

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f"Clearing existing data from Redshift table {self.table}")
        redshift.run(f"DELETE FROM {self.table}")

        rendered_key = self.s3_key.format(**context)
        s3_path = f"s3://{self.s3_bucket}/{rendered_key}"

        copy_sql = f"""
            COPY {self.table}
            FROM '{s3_path}'
            ACCESS_KEY_ID '{credentials.access_key}'
            SECRET_ACCESS_KEY '{credentials.secret_key}'
            REGION '{self.region}'
            FORMAT AS JSON '{self.json_path}';
        """

        self.log.info(f"Running COPY command for {self.table}: {copy_sql}")
        redshift.run(copy_sql)

        self.log.info(f"Successfully staged {self.table} from S3 → Redshift")
