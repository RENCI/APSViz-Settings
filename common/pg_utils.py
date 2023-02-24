# SPDX-FileCopyrightText: 2022 Renaissance Computing Institute. All rights reserved.
# SPDX-FileCopyrightText: 2023 Renaissance Computing Institute. All rights reserved.
#
# SPDX-License-Identifier: GPL-3.0-or-later
# SPDX-License-Identifier: LicenseRef-RENCI
# SPDX-License-Identifier: MIT

"""
    Class to encapsulate database activities
"""

import os
import time
import psycopg2
from common.logger import LoggingUtil


class PGUtils:
    """
    Methods to perform database activities
    """

    def __init__(self, dbname, username, password, auto_commit=True):
        # create a logger
        self.logger = LoggingUtil.init_logging("APSVIZ.Settings.pg_utils", line_format='medium')

        # get configuration params from the pods secrets
        host = os.environ.get('ASGS_DB_HOST')
        port = os.environ.get('ASGS_DB_PORT')

        # create a connection string
        self.conn_str = f"host={host} port={port} dbname={dbname} user={username} password={password}"

        # init the DB connection objects
        self.conn = None
        self.cursor = None
        self.auto_commit = auto_commit

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

                    # set the manner of commit
                    self.conn.autocommit = self.auto_commit

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

            self.logger.error('DB Connection failed. Retrying...')
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

        # check/terminate the DB connection and cursor
        try:
            if self.cursor is not None:
                self.cursor.close()

            if self.conn is not None:
                self.conn.close()
        except Exception:
            self.logger.exception('Error detected closing cursor or connection.')

    def exec_sql(self, sql_stmt, is_select=True):
        """
        executes a sql statement

        :param sql_stmt:
        :param is_select:
        :return:
        """
        # init the return
        ret_val = None

        # insure we have a valid DB connection
        self.get_db_connection()

        try:
            # execute the sql
            ret_val = self.cursor.execute(sql_stmt)

            # get the data
            if is_select:
                # get the returned value
                ret_val = self.cursor.fetchall()

                # trap the return
                if len(ret_val) == 0:
                    # specify a return code on an empty result
                    ret_val = -1

        except Exception:
            self.logger.exception('Error detected executing SQL: %s.', sql_stmt)
            ret_val = -2

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
        return self.exec_sql(sql)[0][0]

    def get_job_order(self, workflow_type: str):
        """
        gets the supervisor job order

        :return:
        """
        # create the sql
        sql: str = f"SELECT public.get_supervisor_job_order('{workflow_type}')"

        # get the data
        return self.exec_sql(sql)[0][0]

    def reset_job_order(self, workflow_type_name: str) -> bool:
        """
        resets the supervisor job order to the default

        :return:
        """

        # declare an array of the job id and next job type id in sequence
        workflow_job_types: dict = {
            'ASGS': [
                # record id, next job type
                # -------------------------
                '1, 12',   # staging step
                '13, 25',  # hazus step
                '17, 23',  # obs-mod ast step
                '15, 26',  # adcirc to cog step
                '18, 24',  # adcirc time to cog step
                '16, 19',  # geotiff to cog step
                '11, 20',  # load geo server step
                '14, 21'   # final staging step
                ],
            'ECFLOW': [
                # job id, next job type
                # -------------------------
                '101, 25',  # staging step
                '106, 23',  # obs-mod ast step
                '104, 26',  # adcirc to cog step
                '108, 24',  # adcirc time to cog step
                '105, 19',  # geotiff to cog step
                '102, 20',  # load geo server step
                '103, 21'  # final staging step
                ]
         }

        # init the failed flag
        failed: bool = False

        # for each job entry
        for item in workflow_job_types[workflow_type_name]:
            # build the update sql
            sql = f"SELECT public.update_next_job_for_job({item}, '{workflow_type_name}')"

            # and execute it
            ret_val = self.exec_sql(sql)

            # anything other than a list returned is an error
            if not isinstance(ret_val, list):
                failed = True
                break

        # if there were no errors, commit the updates
        if not failed:
            self.conn.commit()

        # return to the caller
        return failed

    def get_terria_map_catalog_data(self, **kwargs):
        """
        gets the catalog data for the terria map UI

        :return:
        """
        # create the sql
        sql: str = f"SELECT public.get_terria_data_json(_grid_type:={kwargs['grid_type']}, _event_type:={kwargs['event_type']}, " \
                   f"_instance_name:={kwargs['instance_name']}, _run_date:={kwargs['run_date']}, _end_date:={kwargs['end_date']}, " \
                   f"_limit:={kwargs['limit']}, _met_class:={kwargs['met_class']})"

        # get the data
        return self.exec_sql(sql)[0][0]

    def get_run_list(self):
        """
        gets the last 100 job runs

        :return:
        """

        # create the sql
        sql: str = 'SELECT public.get_supervisor_run_list()'

        # return the data
        return self.exec_sql(sql)[0][0]

    def update_next_job_for_job(self, job_name: str, next_process_id: int, workflow_type_name: str):
        """
        Updates the next job process id for a job

        :param job_name:
        :param next_process_id:
        :param workflow_type_name:
        :return: nothing
        """

        # create the sql
        sql = f"SELECT public.update_next_job_for_job('{job_name}', {next_process_id}, '{workflow_type_name}')"

        # run the SQL
        self.exec_sql(sql)

    def update_job_image_version(self, job_name: str, image: str):
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
