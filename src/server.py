# SPDX-FileCopyrightText: 2022 Renaissance Computing Institute. All rights reserved.
# SPDX-FileCopyrightText: 2023 Renaissance Computing Institute. All rights reserved.
#
# SPDX-License-Identifier: GPL-3.0-or-later
# SPDX-License-Identifier: LicenseRef-RENCI
# SPDX-License-Identifier: MIT

"""
    APSVIZ settings server.
"""

import json
import re
from pathlib import Path

from enum import Enum

from fastapi import FastAPI, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse

from src.common.logger import LoggingUtil
from src.common.pg_impl import PGImplementation

# set the app version
APP_VERSION = 'v0.3.6'

# declare the FastAPI details
APP = FastAPI(title='APSVIZ Settings', version=APP_VERSION)

# get the log level and directory from the environment.
log_level, log_path = LoggingUtil.prep_for_logging()

# create a logger
logger = LoggingUtil.init_logging("APSVIZ.Settings", level=log_level, line_format='medium', log_file_path=log_path)

# declare app access details
APP.add_middleware(CORSMiddleware, allow_origins=['*'], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

# specify the DB to get a connection
# note the extra comma makes this single item a singleton tuple
db_names: tuple = ('apsviz', 'asgs')

# create a DB connection object
db_info: PGImplementation = PGImplementation(db_names, _logger=logger)

# create a DB connection object with auto-commit turned off
db_info_no_auto_commit: PGImplementation = PGImplementation(db_names, _logger=logger, _auto_commit=False)


# declare the supervisor workflow types
class WorkflowTypeName(str, Enum):
    """
    Class enums for the supervisor workflow types
    """
    ASGS = 'ASGS'
    ECFLOW = 'ECFLOW'
    HECRAS = 'HECRAS'


# declare the job type names
class JobTypeName(str, Enum):
    """
    Class enum for k8s job names
    """
    ADCIRC2COG_TIFF_JOB = 'adcirc2cog-tiff-job'
    ADCIRCTIME_TO_COG_JOB = 'adcirctime-to-cog-job'
    AST_RUN_HARVESTER_JOB = 'ast-run-harvester-job'
    FINAL_STAGING_JOB = 'final-staging-job'
    GEOTIFF2COG_JOB = 'geotiff2cog-job'
    HAZUS = 'hazus'
    LOAD_GEO_SERVER_JOB = 'load-geo-server-job'
    LOAD_GEO_SERVER_S3_JOB = 'load-geo-server-s3-job'
    OBS_MOD_AST_JOB = 'obs-mod-ast-job'
    STAGING = 'staging'


# declare the job type names
class NextJobTypeName(str, Enum):
    """
    Class enum for k8s job names
    """
    ADCIRC2COG_TIFF_JOB = 'adcirc2cog-tiff-job'
    ADCIRCTIME_TO_COG_JOB = 'adcirctime-to-cog-job'
    AST_RUN_HARVESTER_JOB = 'ast-run-harvester-job'
    COMPLETE = 'complete'
    FINAL_STAGING_JOB = 'final-staging-job'
    GEOTIFF2COG_JOB = 'geotiff2cog-job'
    HAZUS = 'hazus'
    LOAD_GEO_SERVER_JOB = 'load-geo-server-job'
    LOAD_GEO_SERVER_S3_JOB = 'load-geo-server-s3-job'
    OBS_MOD_AST_JOB = 'obs-mod-ast-job'
    STAGING = 'staging'


# declare the run status types
class RunStatus(str, Enum):
    """
    Class enum for job run statuses
    """
    NEW = 'new'
    DEBUG = 'debug'
    DO_NOT_RERUN = 'do not rerun'


# declare the possible image repos
class ImageRepo(str, Enum):
    """
    Class enum for image registries
    """
    CONTAINERS = 'containers.renci.org'
    RENCIORG = 'renciorg'


# declare the two potential image repos
image_repo_to_repo_name: dict = {'renciorg': 'renciorg', 'containers.renci.org': 'containers.renci.org/eds'}

# declare the component job type image name
job_type_to_image_name: dict = {'adcirc2cog-tiff-job': '/adcirc2cog:', 'adcirctime-to-cog-job': '/adcirctime2cogs:',
                                'ast-run-harvester-job': '/ast_run_harvester', 'final-staging-job': '/stagedata:', 'geotiff2cog-job': '/adcirc2cog:',
                                'hazus': '/adras:', 'load-geo-server-job': '/load_geoserver:', 'load-geo-server-s3-job': '/load_geoserver:',
                                'obs-mod-ast-job': '/ast_supp:',
                                'staging': '/stagedata:'}

# declare job name to id
job_type_name_to_id: dict = {"adcirc2cog-tiff-job": 23, 'adcirctime-to-cog-job': 26, 'ast-run-harvester-job': 27, "complete": 21,
                             "final-staging-job": 20, "geotiff2cog-job": 24, "hazus": 12, "load-geo-server-job": 19, "load-geo-server-s3-job": 28,
                             "obs-mod-ast-job": 25,
                             "staging": 11}


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


@APP.get('/get_job_order/{workflow_type_name}', status_code=200, response_model=None)
async def display_job_order(workflow_type_name: WorkflowTypeName) -> json:
    """
    Displays the job order for the workflow type selected.

    """

    # init the returned html status code
    status_code = 200

    try:
        # try to make the call for records
        ret_val = db_info.get_job_order(WorkflowTypeName(workflow_type_name).value)

    except Exception:
        # return a failure message
        ret_val = f'Exception detected trying to get the {WorkflowTypeName(workflow_type_name).value} job order.'

        # log the exception
        logger.exception(ret_val)

        # set the status to a server error
        status_code = 500

    # return to the caller
    return JSONResponse(content=ret_val, status_code=status_code, media_type="application/json")


@APP.get('/reset_job_order/{workflow_type_name}', status_code=200, response_model=None)
async def reset_job_order(workflow_type_name: WorkflowTypeName) -> json:
    """
    resets the job process order to the default for the workflow selected.

    The normal sequence of ASGS jobs are:
    staging -> hazus -> obs-mod-ast -> adcirc to COGs -> adcirc Time to COGs -> compute COGs geo-tiffs -> load geoserver -> final staging -> complete

    The normal sequence of ECFLOW jobs are:
    staging -> obs-mod-ast -> adcirc to COGs -> adcirc Time to COGs -> compute COG geo-tiffs -> load geoserver -> final staging -> complete

    The normal sequence of HECRAS jobs are
    load geoserver -> complete

    """

    # init the returned html status code
    status_code = 200
    ret_val = ''

    try:
        # try to make the call for records
        ret_val = db_info_no_auto_commit.reset_job_order(WorkflowTypeName(workflow_type_name).value)

        # check the return value for failure, failed == true
        if ret_val:
            raise Exception(f'Failure trying to reset the {WorkflowTypeName(workflow_type_name).value} job order. Error: {ret_val}')

        # get the new job order
        job_order = db_info.get_job_order(WorkflowTypeName(workflow_type_name).value)

        # return a success message with the new job order
        ret_val = [{'message': f'The job order for the {WorkflowTypeName(workflow_type_name).value} workflow has been reset to the default.'},
                   {'job_order': job_order}]

    except Exception:
        # return a failure message
        ret_val = f'Exception detected trying to get the {WorkflowTypeName(workflow_type_name).value} job order.'

        # log the exception
        logger.exception(ret_val)

        # set the status to a server error
        status_code = 500

    # return to the caller
    return JSONResponse(content=ret_val, status_code=status_code, media_type="application/json")


@APP.get('/get_job_defs', status_code=200, response_model=None)
async def display_job_definitions() -> json:
    """
    Displays the job definitions for all workflows. Note that this list is in alphabetical order (not in job execute order).

    """

    # init the returned html status code
    status_code = 200

    # init the return
    job_config_data: dict = {}

    try:
        # try to make the call for records
        job_data = db_info.get_job_defs()

        # make sure we got a list of config data items
        if isinstance(job_data, list):
            for workflow_item in job_data:
                # get the workflow type name
                workflow_type = list(workflow_item.keys())[0]

                # get the data looking like something we are used to
                job_config_data[workflow_type] = {list(x)[0]: x.get(list(x)[0]) for x in workflow_item[workflow_type]}

                # fix the arrays for each job def. they come in as a string
                for item in job_config_data[workflow_type].items():
                    item[1]['COMMAND_LINE'] = json.loads(item[1]['COMMAND_LINE'])
                    item[1]['COMMAND_MATRIX'] = json.loads(item[1]['COMMAND_MATRIX'])
                    item[1]['PARALLEL'] = json.loads(item[1]['PARALLEL']) if item[1]['PARALLEL'] is not None else None

    except Exception:
        # return a failure message
        ret_val = 'Exception detected trying to get the job definitions'

        # log the exception
        logger.exception(ret_val)

        # set the status to a server error
        status_code = 500

    # return to the caller
    return JSONResponse(content=job_config_data, status_code=status_code, media_type="application/json")


@APP.get("/get_log_file_list", response_model=None)
async def get_the_log_file_list(request: Request):
    """
    Gets the log file list. each of these entries could be used in the get_log_file endpoint

    """

    # return the list to the caller in JSON format
    return JSONResponse(content={'Response': get_log_file_list(f'{request.base_url.scheme}://{request.base_url.netloc}')}, status_code=200,
                        media_type="application/json")


@APP.get("/get_log_file/", response_model=None)
async def get_the_log_file(log_file: str = Query('log_file')):
    """
    Gets the log file specified. This method only expects a properly named file.

    """
    # get the path to the log files
    log_dir = LoggingUtil.get_log_path()

    # turn the incoming log file into a path object
    log_file_path = Path(log_file)

    # loop through the log file directory
    for real_log_file in Path(log_dir).rglob('*log*'):
        # if the target file is found in the log directory
        if real_log_file == log_file_path:
            # return the file to the caller
            return FileResponse(path=log_file_path, filename=log_file_path.name, media_type='text/plain')

    # if we get here return an error
    return JSONResponse(content={'Response': 'Error - Log file does not exist.'}, status_code=500, media_type="application/json")


@APP.get("/get_run_list", status_code=200, response_model=None)
async def get_the_run_list():
    """
    Gets the run information for the last 100 runs.

    """

    # init the returned html status code
    status_code = 200

    try:
        # get the run records
        ret_val = db_info.get_run_list()

        # add a final status to each record
        for item in ret_val:
            if item['status'].find('Error') > -1:
                item['final_status'] = 'Error'
            else:
                item['final_status'] = 'Success'

    except Exception:
        # return a failure message
        ret_val = 'Exception detected trying to gather run data'

        # log the exception
        logger.exception(ret_val)

        # set the status to a server error
        status_code = 500

    # return to the caller
    return JSONResponse(content={'Response': ret_val}, status_code=status_code, media_type="application/json")


# sets the run.properties run status to 'new' for a job
@APP.put('/instance_id/{instance_id}/uid/{uid}/status/{status}', status_code=200, response_model=None)
async def set_the_run_status(instance_id: int, uid: str, status: RunStatus = RunStatus('new')):
    """
    Updates the run status of a selected job.

    ex: instance id: 3057, uid: 2021062406-namforecast, status: do not rerun

    """
    # init the returned html status code
    status_code = 200

    # is this a valid instance id
    if instance_id > 0:
        try:
            # try to make the update
            db_info_no_auto_commit.update_run_status(instance_id, uid, status.value)

            # return a success message
            ret_val = f'The status of run {instance_id}/{uid} has been set to {status}'
        except Exception:
            # return a failure message
            ret_val = f'Exception detected trying to update run {instance_id}/{uid} to {status}'

            # log the exception
            logger.exception(ret_val)

            # set the status to a server error
            status_code = 500
    else:
        # return a failure message
        ret_val = f'Error: The instance id {instance_id} is invalid. An instance must be a non-zero integer.'

        # log the error
        logger.ERROR(ret_val)

        # set the status to a bad request
        status_code = 400

    # return to the caller
    return JSONResponse(content={'Response': ret_val}, status_code=status_code, media_type="application/json")


# Updates the image version for a job
@APP.put('/image_repo/{image_repo}/job_type_name/{job_type_name}/image_version/{version}', status_code=200, response_model=None)
async def set_the_supervisor_component_image_version(image_repo: ImageRepo, job_type_name: JobTypeName, version: str):
    """
    Updates a supervisor component image version label in the supervisor job run configuration.

    Notes:
     - This will update the version for ALL references of this component across ALL workflows.
     - Please must select the image repository that houses your container image.
     - The version label must match what has been uploaded to docker hub.

    """
    # init the returned html status code
    status_code = 200

    try:
        # create a regex pattern for the version number
        version_pattern = re.compile(r"(v\d\.+\d\.+\d)")

        # makesure that the input params are legit
        if version_pattern.search(version):
            # make the update. fix the job name (hyphen) so it matches the DB format
            db_info.update_job_image_version(JobTypeName(job_type_name).value + '-',
                                             image_repo_to_repo_name[image_repo] + job_type_to_image_name[job_type_name] + version)

            # return a success message
            ret_val = f"The docker repo/image:version for job name {job_type_name} has been set to " \
                      f"{image_repo_to_repo_name[image_repo] + job_type_to_image_name[job_type_name] + version}"
        else:
            # return a success message
            ret_val = f"Error: The version {version} is invalid. Please use a value in the form of v<int>.<int>.<int>"

            # log the error
            logger.ERROR(ret_val)

            # set the status to a bad request
            status_code = 400

    except Exception:
        # return a failure message
        ret_val = f'Exception detected trying to update the image version {job_type_name} to version {version}'

        # log the exception
        logger.exception(ret_val)

        # set the status to a server error
        status_code = 500

    # return to the caller
    return JSONResponse(content={'Response': ret_val}, status_code=status_code, media_type="application/json")


# Updates a supervisor component's next process.
@APP.put('/workflow_type_name/{workflow_type_name}/job_type_name/{job_type_name}/next_job_type/{next_job_type_name}', status_code=200,
         response_model=None)
async def set_the_supervisor_job_order(workflow_type_name: WorkflowTypeName, job_type_name: JobTypeName, next_job_type_name: NextJobTypeName):
    """
    Modifies the supervisor component's linked list of jobs. Select the workflow type, then select the job process name and the next job
    process name.

    The normal sequence of ASGS jobs are:
    staging -> hazus -> obs-mod-ast -> adcirc to COGs -> adcirc Time to COGs -> compute COGs geo-tiffs -> load geo server -> final staging -> complete

    The normal sequence of ECFLOW jobs are:
    staging -> obs-mod-ast -> adcirc to COGs -> adcirc Time to COGs -> compute COG geo-tiffs -> load geo server -> final staging -> complete

    The normal sequence of HECRAS jobs are
    load geoserver -> complete
    """
    # init the returned html status code
    status_code = 200

    try:
        # check for a recursive situation
        if job_type_name == next_job_type_name:
            # set the error msg
            ret_val = f'You cannot specify a next job type equal to the target job type ({job_type_name}).'

            # declare an error for the user
            status_code = 500
        else:
            # convert the next job process name to an id
            next_job_type_id = job_type_name_to_id.get(NextJobTypeName(next_job_type_name).value)

            # did we get a good type id
            if next_job_type_name is not None:
                # prep the record to update key. complete does not have a hyphen
                if job_type_name != 'complete':
                    job_type_name += '-'

                # make the update
                db_info.update_next_job_for_job(job_type_name, next_job_type_id, WorkflowTypeName(workflow_type_name).value)

                # get the new job order
                job_order = db_info.get_job_order(WorkflowTypeName(workflow_type_name).value)

                # return a success message with the new job order
                ret_val = [{
                    'message': f'The {WorkflowTypeName(workflow_type_name).value} {job_type_name} next process has been set to {next_job_type_name}'},
                    {'new_order': job_order}]
            else:
                # set the error msg
                ret_val = f'The next job process ID was not found for {WorkflowTypeName(workflow_type_name).value} {next_job_type_name}'

                # declare an error for the user
                status_code = 500

    except Exception:
        # return a failure message
        ret_val = f'Exception detected trying to update the {WorkflowTypeName(workflow_type_name).value} next job name for' \
                  f' {job_type_name}, next job name: {next_job_type_name}'

        # log the exception
        logger.exception(ret_val)

        # set the status to a server error
        status_code = 500

    # return to the caller
    return JSONResponse(content={'Response': ret_val}, status_code=status_code, media_type="application/json")
