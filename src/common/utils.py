# SPDX-FileCopyrightText: 2022 Renaissance Computing Institute. All rights reserved.
# SPDX-FileCopyrightText: 2023 Renaissance Computing Institute. All rights reserved.
#
# SPDX-License-Identifier: GPL-3.0-or-later
# SPDX-License-Identifier: LicenseRef-RENCI
# SPDX-License-Identifier: MIT

"""
    General utilities.

    Author: Phil Owen, 6/27/2023
"""

import os

from pathlib import Path
from enum import Enum
from src.common.logger import LoggingUtil


class GenUtils:
    """
    General utilities

    """
    # declare the two potential image repos
    image_repo_to_repo_name: dict = {'renciorg': 'renciorg', 'containers.renci.org': 'containers.renci.org/eds'}

    # declare the component job type image name
    job_type_to_image_name: dict = {'adcirc2cog-tiff-job': '/adcirc2cog:', 'adcirctime-to-cog-job': '/adcirctime2cogs:',
                                    'adcirc-to-kalpana-cog-job': '/adcirc-to-kalpana-cog-job:', 'ast-run-harvester-job': '/ast_run_harvester:',
                                    'collab-data-sync-job': '/apsviz-collab-sync:', 'final-staging-job': '/stagedata:',
                                    'geotiff2cog-job': '/adcirc2cog: ', 'hazus': '/adras:', 'load-geo-server-job': '/load_geoserver:',
                                    'load-geo-server-s3-job': '/load_geoserver:', 'obs-mod-ast-job': '/ast_supp:', 'staging': '/stagedata:',
                                    'timeseriesdb-ingest-job': '/apsviz-timeseriesdb-ingest:'}

    # declare job name to id
    job_type_name_to_id: dict = {'adcirc2cog-tiff-job': 23, 'adcirctime-to-cog-job': 26, 'adcirc-to-kalpana-cog-job': 30, 'ast-run-harvester-job': 27,
                                 'collab-data-sync-job': 29, 'complete': 21, 'final-staging-job': 20, 'geotiff2cog-job': 24, 'hazus': 12,
                                 'load-geo-server-job': 19, 'load-geo-server-s3-job': 28, 'obs-mod-ast-job': 25, 'staging': 11,
                                 'timeseriesdb-ingest-job': 31}

    @staticmethod
    def get_log_file_list(hostname):
        """
        Gets all the log file path/names

        :return:
        """
        # init the return
        ret_val = {}

        # init a file counter
        counter = 0

        # get the log path
        log_file_path: str = LoggingUtil.get_log_path().replace('\\', '/')

        # go through all the directories
        for file_path in Path(log_file_path).rglob('*log*'):
            # increment the counter
            counter += 1

            # save the absolute file path, endpoint URL, and file size in a dict
            ret_val.update({f"{file_path.name}_{counter}": {'file_name': file_path.name, 'url': f'{hostname}/get_log_file/?log_file={file_path}',
                                                            'file_size': f'{file_path.stat().st_size} bytes'}})

        # return the list to the caller
        return ret_val

    @staticmethod
    def check_freeze_status() -> bool:
        """
        checks to see if we are in image freeze mode.

        """
        # get the flag that indicates we are freezing the updating of image versions
        freeze_mode: bool = os.path.exists(os.path.join(os.path.dirname(__file__), '../', str('freeze')))

        # return to the caller
        return freeze_mode


class WorkflowTypeName(str, Enum):
    """
    Class enums for the supervisor workflow names
    """
    ASGS = 'ASGS'
    ECFLOW = 'ECFLOW'
    HECRAS = 'HECRAS'


class JobTypeName(str, Enum):
    """
    Class enum for k8s job type names
    """
    ADCIRC2COG_TIFF_JOB = 'adcirc2cog-tiff-job'
    ADCIRCTIME_TO_COG_JOB = 'adcirctime-to-cog-job'
    ADCIRC_TO_KALPANA_COG_JOB = 'adcirc-to-kalpana-cog-job'
    AST_RUN_HARVESTER_JOB = 'ast-run-harvester-job'
    COLLAB_DATA_SYNC = 'collab-data-sync-job'
    FINAL_STAGING_JOB = 'final-staging-job'
    GEOTIFF2COG_JOB = 'geotiff2cog-job'
    HAZUS = 'hazus'
    LOAD_GEO_SERVER_JOB = 'load-geo-server-job'
    LOAD_GEO_SERVER_S3_JOB = 'load-geo-server-s3-job'
    OBS_MOD_AST_JOB = 'obs-mod-ast-job'
    STAGING = 'staging'
    TIMESERIESDB_INGEST_JOB = 'timeseriesdb-ingest-job'


class NextJobTypeName(str, Enum):
    """
    Class enum for k8s job type names
    """
    ADCIRC2COG_TIFF_JOB = 'adcirc2cog-tiff-job'
    ADCIRCTIME_TO_COG_JOB = 'adcirctime-to-cog-job'
    ADCIRC_TO_KALPANA_COG_JOB = 'adcirc-to-kalpana-cog-job'
    AST_RUN_HARVESTER_JOB = 'ast-run-harvester-job'
    COLLAB_DATA_SYNC = 'collab-data-sync-job'
    COMPLETE = 'complete'
    FINAL_STAGING_JOB = 'final-staging-job'
    GEOTIFF2COG_JOB = 'geotiff2cog-job'
    HAZUS = 'hazus'
    LOAD_GEO_SERVER_JOB = 'load-geo-server-job'
    LOAD_GEO_SERVER_S3_JOB = 'load-geo-server-s3-job'
    OBS_MOD_AST_JOB = 'obs-mod-ast-job'
    STAGING = 'staging'
    TIMESERIESDB_INGEST_JOB = 'timeseriesdb-ingest-job'


class RunStatus(str, Enum):
    """
    Class enum for job run status types
    """
    NEW = 'new'
    DEBUG = 'debug'
    DO_NOT_RERUN = 'do not rerun'


class ImageRepo(str, Enum):
    """
    Class enum for image repo registries
    """
    CONTAINERS = 'containers.renci.org'
    RENCIORG = 'renciorg'
