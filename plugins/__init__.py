from __future__ import annotations
from airflow.plugins_manager import AirflowPlugin

from operators.stage_redshift import StageToRedshiftOperator
from operators.load_fact import LoadFactOperator
from operators.load_dimension import LoadDimensionOperator
from operators.data_quality import DataQualityOperator

from helpers.sql_queries import SqlQueries


class SparkifyPlugin(AirflowPlugin):
    name = "sparkify_plugin"
    operators = [
        StageToRedshiftOperator,
        LoadFactOperator,
        LoadDimensionOperator,
        DataQualityOperator
    ]
    helpers = [
        SqlQueries
    ]
