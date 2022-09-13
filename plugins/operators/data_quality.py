from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers.sql_queries import  SqlQueries

class DataQualityOperator(BaseOperator):

    """Check data quality
    
    Checks to confirm that each table returns at least one row of data. 
    Also checks to confirm that each table does not contain NULL values. 

    Raises:
        ValueError: Raised if no data was returned
    """
    ui_color = '#89DA59'
    insert_sql = None
    
    @apply_defaults
    def __init__(self,
                    redshift_conn_id="",
                    target_table="",
                    *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.target_table = target_table


    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Preparing SQL query for {} table".format(self.target_table))
        tables = self.target_table
        check_nulls_queries = []
        check_count_queries = []
        if "songplays" in tables:
            check_nulls_queries.append(SqlQueries.songplays_check_nulls)
            check_count_queries.append(SqlQueries.songplays_check_count)
        if "users" in tables:
            check_nulls_queries.append(SqlQueries.users_check_nulls)
            check_count_queries.append(SqlQueries.users_check_count)
        if "songs" in tables:
            check_nulls_queries.append(SqlQueries.songs_check_nulls)
            check_count_queries.append(SqlQueries.songs_check_count)
        if "artists" in tables:
            check_nulls_queries.append(SqlQueries.artists_check_nulls)
            check_count_queries.append(SqlQueries.artists_check_count)
        if "time" in tables:
            check_nulls_queries.append(SqlQueries.time_check_nulls)
            check_count_queries.append(SqlQueries.time_check_count)

        # Executing quality checks
        self.log.info(f"Executing Redshift table quality checks for tables: {tables} ...")
        self.log.info(f"Executing Redshift table quality checks for queries: {check_nulls_queries} ...")
        for query in check_nulls_queries:
            records = redshift.get_records(query)
            self.log.info(f"RESULTS: {records}")
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {query} returned results.")
            num_records = records[0][0]
            if num_records > 0:
                raise ValueError(f"Data quality check failed. {query} contained > 0 rows")
            self.log.info(f"Data quality on table {query} check passed with {records[0][0]} records")

        for query in check_count_queries:
            records = redshift.get_records(query)
            self.log.info(f"RESULTS: {records}")
            self.log.info(f"RESULTS: {query} had {records[0][0]} records.")

        self.log.info("Redshift table quality checks DONE.")