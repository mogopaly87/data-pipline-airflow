from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers.sql_queries import  SqlQueries

class LoadFactOperator(BaseOperator):
    """_summary_\n
    Operator to perform an INSERT operation into the songplays table 
    from a SELECT/JOIN operation between the staging_events and staging_songs
    table.

    Args:
        BaseOperator (_type_): _description_
    """

    ui_color = '#F98866'
    insert_sql = """
        INSERT INTO {}(playId, start_time, userId, level, 
                        songId, artistId, sessionId, location, user_agent)
        {}
    """
    @apply_defaults
    def __init__(self,
                    redshift_conn_id="",
                    table="",
                    *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table

    
    
    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        print("Deleting all rows from the {} table".format(self.table))
        redshift.run("DELETE FROM {}".format(self.table))
        print("Running this songplays query: \n", SqlQueries.song_table_insert)
        formatted_sql = LoadFactOperator.insert_sql.format(
                        self.table,
                        SqlQueries.songplay_table_insert)
        print("Insert query >> ", formatted_sql)
        redshift.run(formatted_sql)
        # self.log.info('LoadFactOperator not implemented yet')
