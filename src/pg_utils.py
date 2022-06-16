import os
import psycopg2
import logging
import time
from common.logging import LoggingUtil


class PGUtils:
    def __init__(self, dbname, username, password):
        # get the log level and directory from the environment.
        # level comes from the container dockerfile, path comes from the k8s secrets
        log_level: int = int(os.getenv('LOG_LEVEL', logging.INFO))
        log_path: str = os.getenv('LOG_PATH', os.path.dirname(__file__))

        # create the dir if it does not exist
        if not os.path.exists(log_path):
            os.mkdir(log_path)

        # create a logger
        self.logger = LoggingUtil.init_logging("APSVIZ.Settings.pg_utils", level=log_level, line_format='medium', log_file_path=log_path)

        # get configuration params from the pods secrets
        host = os.environ.get('ASGS_DB_HOST')
        port = os.environ.get('ASGS_DB_PORT')

        # create a connection string
        self.conn_str = f"host={host} port={port} dbname={dbname} user={username} password={password}"

        # init the DB connection objects
        self.conn = None
        self.cursor = None

        # get a db connection and cursor
        self.get_db_connection()

    def get_db_connection(self):
        """
        Gets a connection to the DB. performs a check to continue trying until
        a connection is made

        :return:
        """
        # init the connection status indicator
        good_conn = False

        # until forever
        while not good_conn:
            # check the DB connection
            good_conn = self.check_db_connection()

            try:
                # do we have a good connection
                if not good_conn:
                    # connect to the DB
                    self.conn = psycopg2.connect(self.conn_str)

                    # insure records are updated immediately
                    self.conn.autocommit = True

                    # create the connection cursor
                    self.cursor = self.conn.cursor()

                    # check the DB connection
                    good_conn = self.check_db_connection()

                    # is the connection ok now?
                    if good_conn:
                        # ok to continue
                        return
                else:
                    # ok to continue
                    return
            except (Exception, psycopg2.DatabaseError):
                good_conn = False

            self.logger.error(f'DB Connection failed. Retrying...')
            time.sleep(5)

    def check_db_connection(self) -> bool:
        """
        checks to see if there is a good connection to the DB

        :return: boolean
        """
        # init the return value
        ret_val = None

        try:
            # is there a connection
            if not self.conn or not self.cursor:
                ret_val = False
            else:
                # get the DB version
                self.cursor.execute("SELECT version()")

                # get the value
                db_version = self.cursor.fetchone()

                # did we get a value
                if db_version:
                    # update the return flag
                    ret_val = True

        except (Exception, psycopg2.DatabaseError):
            # connect failed
            ret_val = False

        # return to the caller
        return ret_val

    def __del__(self):
        """
        close up the DB

        :return:
        """
        try:
            if self.cursor is not None:
                self.cursor.close()

            if self.conn is not None:
                self.conn.close()
        except Exception as e:
            self.logger.error(f'Error detected closing cursor or connection. {e}')

    def exec_sql(self, sql_stmt):
        """
        executes a sql statement

        :param sql_stmt:
        :return:
        """
        # init the return
        ret_val = None

        # insure we have a valid DB connection
        self.get_db_connection()

        try:
            # execute the sql
            self.cursor.execute(sql_stmt)

            # get the returned value
            ret_val = self.cursor.fetchone()

            # trap the return
            if ret_val is None or ret_val[0] is None:
                # specify a return code on an empty result
                ret_val = -1
            else:
                # get the one and only record of json
                ret_val = ret_val[0]

        except Exception as e:
            self.logger.error(f'Error detected executing SQL: {sql_stmt}. {e}')

        # return to the caller
        return ret_val

    def get_job_defs(self):
        """
        gets the supervisor job definitions

        :return:
        """

        # create the sql
        sql: str = 'SELECT public.get_supervisor_job_defs_json()'

        # get the data
        return self.exec_sql(sql)

    def get_terria_map_catalog_data(self):
        """
        gets the catalog data for the terria map UI

        :return:
        """

        # create the sql
        sql: str = 'SELECT public.get_terria_data_json()'

        # get the data
        return self.exec_sql(sql)

    def get_run_list(self):
        """
        gets the last 100 job runs

        :return:
        """

        # create the sql
        sql: str = """
                        SELECT json_agg(runs)
                        FROM
                        (
                            SELECT DISTINCT 
                                id, instance_id, uid, value AS status
                            FROM 
                                public."ASGS_Mon_config_item"
                            WHERE 
                                KEY IN ('supervisor_job_status')
                                AND instance_id IN (SELECT id FROM public."ASGS_Mon_instance" ORDER BY id DESC)
                            ORDER BY 
                                instance_id DESC, id DESC 
                            LIMIT 100
                        ) runs;"""

        data = self.exec_sql(sql)

        # get the data
        return data

    def update_job_image(self, job_name: str, image: str):
        """
        Updates the image version

        :param job_name:
        :param image:
        :return: nothing
        """

        # create the sql
        sql = f"SELECT public.update_job_image('{job_name}', '{image}')"

        # run the SQL
        self.exec_sql(sql)

    def update_run_status(self, instance_id: int, uid: str, status: str):
        """
        Updates the run properties run status to 'new'.

        :param instance_id:
        :param uid:
        :param status
        :return:
        """

        # create the sql
        sql = f"SELECT public.set_config_item({instance_id}, '{uid}', 'supervisor_job_status', '{status}')"

        # run the SQL
        self.exec_sql(sql)
