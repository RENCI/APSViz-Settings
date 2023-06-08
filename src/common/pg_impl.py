# SPDX-FileCopyrightText: 2022 Renaissance Computing Institute. All rights reserved.
# SPDX-FileCopyrightText: 2023 Renaissance Computing Institute. All rights reserved.
#
# SPDX-License-Identifier: GPL-3.0-or-later
# SPDX-License-Identifier: LicenseRef-RENCI
# SPDX-License-Identifier: MIT

"""
    Class for database functionalities

    Author: Phil Owen, RENCI.org
"""
from src.common.pg_utils_multi import PGUtilsMultiConnect
from src.common.logger import LoggingUtil


class PGImplementation(PGUtilsMultiConnect):
    """
        Class that contains DB calls for the Settings app.

        Note this class inherits from the PGUtilsMultiConnect class
        which has all the connection and cursor handling.
    """

    def __init__(self, db_names: tuple, _logger=None, _auto_commit=True):
        # if a reference to a logger passed in use it
        if _logger is not None:
            # get a handle to a logger
            self.logger = _logger
        else:
            # get the log level and directory from the environment.
            log_level, log_path = LoggingUtil.prep_for_logging()

            # create a logger
            self.logger = LoggingUtil.init_logging("APSViz.Settings.PGImplementation", level=log_level, line_format='medium', log_file_path=log_path)

        # init the base class
        PGUtilsMultiConnect.__init__(self, 'APSViz.Settings', db_names, _logger=self.logger, _auto_commit=_auto_commit)

    def __del__(self):
        """
        Calls super base class to clean up DB connections and cursors.

        :return:
        """
        # clean up connections and cursors
        PGUtilsMultiConnect.__del__(self)

    def get_job_defs(self):
        """
        gets the supervisor job definitions

        :return:
        """

        # create the sql
        sql: str = 'SELECT public.get_supervisor_job_defs_json()'

        # get the data
        ret_val = self.exec_sql('asgs', sql)

        # return the data
        return ret_val

    def get_job_order(self, workflow_type: str):
        """
        gets the supervisor job order

        :return:
        """
        # create the sql
        sql: str = f"SELECT public.get_supervisor_job_order('{workflow_type}')"

        # get the data
        ret_val = self.exec_sql('asgs', sql)

        # return the data
        return ret_val

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
                '1, 23',  # staging
                '15, 30',  # adcirc2cog-tiff
                '21, 27',  # adcirc-to-kalpana-cog
                '19, 25',  # ast-run-harvester
                '17, 24',  # obs-mod-ast
                # '22, 21',  # timeseries_ingest
                '16, 19',  # geotiff2cog
                '11, 20',  # load-geo-server
                '14, 21'  # final-staging
                ],
            'ECFLOW': [
                # record id, next job type
                # -------------------------
                '101, 23',  # staging
                '104, 30',  # adcirc2cog-tiff
                '111, 25',  # adcirc-to-kalpana-cog
                '106, 24',  # obs-mod-ast
                # '112, 21',  # timeseries_ingest
                '105, 19',  # geotiff2cog
                '102, 29',  # load-geo-server
                '110, 20',  # collab-data-sync
                '103, 21'  # final-staging
                ],
            'HECRAS': [
                '201, 21',  # load geo server step
                ]
         }

        # init the failed flag
        failed: bool = False

        # for each job entry
        for item in workflow_job_types[workflow_type_name]:
            # build the update sql
            sql = f"SELECT public.update_next_job_for_job({item}, '{workflow_type_name}')"

            # and execute it
            ret_val = self.exec_sql('asgs', sql)

            # anything other than a list returned is an error
            if ret_val != 0:
                failed = True
                break

        # if there were no errors, commit the updates
        if not failed:
            self.commit('asgs')

        # return to the caller
        return failed

    def get_run_list(self):
        """
        gets the last 100 job runs

        :return:
        """

        # create the sql
        sql: str = 'SELECT public.get_supervisor_run_list()'

        # return the data
        return self.exec_sql('asgs', sql)

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
        ret_val = self.exec_sql('asgs', sql)

        # if there were no errors, commit the updates
        if ret_val > -1:
            self.commit('asgs')

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
        ret_val = self.exec_sql('asgs', sql)

        # if there were no errors, commit the updates
        if ret_val > -1:
            self.commit('asgs')

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
        ret_val = self.exec_sql('asgs', sql)

        # if there were no errors, commit the updates
        if ret_val > -1:
            self.commit('asgs')
