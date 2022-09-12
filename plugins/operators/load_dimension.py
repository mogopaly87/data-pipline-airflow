from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers.sql_queries import  SqlQueries


class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    insert_sql = None
    select_statment = None
    
    users_insert_sql = """
        INSERT INTO {}(userid, first_name, last_name, gender, level)
        {}
    """
    
    song_insert_sql = """
        INSERT INTO {}(songid, title, artistid, year, duration)
        {}
    """
    
    artist_insert_sql = """
        INSERT INTO {}(artistid, name, location, lattitude, longitude)
        {}
    """
    
    time_insert_sql = """
        INSERT INTO {}(start_time, hour, day, week, month, year, weekday)
        {}
    """
    
    
    @apply_defaults
    def __init__(self,
                    redshift_conn_id="",
                    table="",
                    *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table

    
    
    def execute(self, context):
        
        if self.table == "users":
            LoadDimensionOperator.insert_sql = LoadDimensionOperator.users_insert_sql
            LoadDimensionOperator.select_statment = SqlQueries.user_table_insert
        elif self.table == "songs":
            LoadDimensionOperator.insert_sql = LoadDimensionOperator.song_insert_sql
            LoadDimensionOperator.select_statment = SqlQueries.song_table_insert
        elif self.table == "artists":
            LoadDimensionOperator.insert_sql = LoadDimensionOperator.artist_insert_sql
            LoadDimensionOperator.select_statment = SqlQueries.artist_table_insert
        elif self.table == "time":
            LoadDimensionOperator.insert_sql = LoadDimensionOperator.time_insert_sql
            LoadDimensionOperator.select_statment = SqlQueries.time_table_insert
            
            
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Deleting all rows from the {} table".format(self.table))
        redshift.run("DELETE FROM {}".format(self.table))
        
        self.log.info("Running this users query: \n", LoadDimensionOperator.select_statment)
        formatted_sql = LoadDimensionOperator.insert_sql.format(
            self.table,
            LoadDimensionOperator.select_statment
        )
        redshift.run(formatted_sql)
