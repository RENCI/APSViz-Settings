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
import os
import re

from pathlib import Path

from fastapi import FastAPI, Query, Request, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse

from src.common.logger import LoggingUtil
from src.common.pg_impl import PGImplementation
from src.common.utils import GenUtils, WorkflowTypeName, ImageRepo, RunStatus, JobTypeName, NextJobTypeName
from src.common.security import Security
from src.common.bearer import JWTBearer

# set the app version
app_version = os.getenv('APP_VERSION', 'Version number not set')

# declare the FastAPI details
APP = FastAPI(title='APSVIZ Settings', version=app_version)

# declare app access details
APP.add_middleware(CORSMiddleware, allow_origins=['*'], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

# get the log level and directory from the environment.
log_level, log_path = LoggingUtil.prep_for_logging()

# create a logger
logger = LoggingUtil.init_logging("APSVIZ.Settings", level=log_level, line_format='medium', log_file_path=log_path)

# specify the DB to get a connection
# note the extra comma makes this single item a singleton tuple
db_names: tuple = ('asgs',)

# create a DB connection object
db_info: PGImplementation = PGImplementation(db_names, _logger=logger)

# create a DB connection object with auto-commit turned off
db_info_no_auto_commit: PGImplementation = PGImplementation(db_names, _logger=logger, _auto_commit=False)

# create a Security object
security = Security()


@APP.get('/get_job_order/{workflow_type_name}', dependencies=[Depends(JWTBearer(security))], status_code=200, response_model=None)
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


@APP.get('/reset_job_order/{workflow_type_name}', dependencies=[Depends(JWTBearer(security))], status_code=200, response_model=None)
async def reset_job_order(workflow_type_name: WorkflowTypeName) -> json:
    """
    resets the job process order to the default for the workflow selected.

    The normal sequence of ASGS jobs are:
    staging -> adcirc to COGs -> adcirc Kalpana to COGs -> ast run harvester -> adcirc Time to COGs -> obs-mod-ast -> compute COGs geo-tiffs ->
    load geoserver -> final staging -> complete

    The normal sequence of ECFLOW jobs are:
    staging -> adcirc to COGs -> adcirc Kalpana to COGs -> adcirc Time to COGs -> obs-mod-ast -> compute COGs geo-tiffs -> load geoserver ->
    collaborator data sync -> final staging -> complete

    The normal sequence of HECRAS jobs are
    load geoserver from S3 -> complete

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


@APP.get('/get_job_defs', dependencies=[Depends(JWTBearer(security))], status_code=200, response_model=None)
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


@APP.get("/get_log_file_list", dependencies=[Depends(JWTBearer(security))], response_model=None)
async def get_the_log_file_list(request: Request):
    """
    Gets the log file list. each of these entries could be used in the get_log_file endpoint

    """

    # return the list to the caller in JSON format
    return JSONResponse(content={'Response': GenUtils.get_log_file_list(f'https://{request.base_url.netloc}')}, status_code=200,
                        media_type="application/json")


@APP.get("/get_log_file/", dependencies=[Depends(JWTBearer(security))], response_model=None)
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
    return JSONResponse(content={'Response': 'Error - Log file does not exist.'}, status_code=404, media_type="application/json")


@APP.get("/get_run_list", dependencies=[Depends(JWTBearer(security))], status_code=200, response_model=None)
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
@APP.put('/instance_id/{instance_id}/uid/{uid}/status/{status}', dependencies=[Depends(JWTBearer(security))], status_code=200, response_model=None)
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
        logger.error(ret_val)

        # set the status to a bad request
        status_code = 400

    # return to the caller
    return JSONResponse(content={'Response': ret_val}, status_code=status_code, media_type="application/json")


# Updates the image version for a job
@APP.put('/image_repo/{image_repo}/job_type_name/{job_type_name}/image_version/{version}', dependencies=[Depends(JWTBearer(security))],
         status_code=200, response_model=None)
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
        # get the image freeze env param
        freeze_mode = GenUtils.check_freeze_status()

        # are we are not in freeze mode do real work
        if not freeze_mode:
            # create a regex pattern for the version number
            version_pattern = re.compile(r"(v\d\.+\d\.+\d)")

            # strip off any whitespace on the version
            version = version.strip()

            # makesure that the input params are legit
            if version_pattern.search(version) or version.startswith('latest'):
                # make the update. fix the job name (hyphen) so it matches the DB format
                db_info.update_job_image_version(JobTypeName(job_type_name).value + '-',
                                                 GenUtils.image_repo_to_repo_name[image_repo] + GenUtils.job_type_to_image_name[
                                                     job_type_name] + version)

                # return a success message
                ret_val = f"The docker repo/image:version for job name {job_type_name} has been set to " \
                          f"{GenUtils.image_repo_to_repo_name[image_repo] + GenUtils.job_type_to_image_name[job_type_name] + version}"
            else:
                # return a success message
                ret_val = f"Error: The version {version} is invalid. Please use a value in the form of v<int>.<int>.<int>"

                # log the error
                logger.error(ret_val)

                # set the status to a bad request
                status_code = 400
        else:
            # return a success message
            ret_val = 'Error: Image update freeze is in effect.'

            # log the error
            logger.error(ret_val)

            # set the status to an unauthorized request
            status_code = 401

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
@APP.put('/workflow_type_name/{workflow_type_name}/job_type_name/{job_type_name}/next_job_type/{next_job_type_name}',
         dependencies=[Depends(JWTBearer(security))], status_code=200, response_model=None)
async def set_the_supervisor_job_order(workflow_type_name: WorkflowTypeName, job_type_name: JobTypeName, next_job_type_name: NextJobTypeName):
    """
    Modifies the supervisor component's linked list of jobs. Select the workflow type, then select the job process name and the next job
    process name.

    The normal sequence of ASGS jobs are:
    staging -> adcirc to COGs -> adcirc Kalpana to COGs -> ast run harvester -> adcirc Time to COGs -> obs-mod-ast -> compute COGs geo-tiffs ->
    load geoserver -> final staging -> complete

    The normal sequence of ECFLOW jobs are:
    staging -> adcirc to COGs -> adcirc Kalpana to COGs -> adcirc Time to COGs -> obs-mod-ast -> compute COGs geo-tiffs -> load geoserver ->
    collaborator data sync -> final staging -> complete

    The normal sequence of HECRAS jobs are
    load geoserver from S3 -> complete
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
            next_job_type_id = GenUtils.job_type_name_to_id.get(NextJobTypeName(next_job_type_name).value)

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
