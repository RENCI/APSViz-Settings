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
import requests

from fastapi import FastAPI, Depends
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


@APP.get('/get_all_sv_component_versions', dependencies=[Depends(JWTBearer(security))], status_code=200, response_model=None)
async def get_all_sv_component_versions() -> json:
    """
    displays the super-v component versions across all namespaces (AWS, RENCI prod/dev)

    :return:
    """
    # init the returned html status code
    status_code = 200

    # init the intermediate and return values
    results: list = []
    ret_val: list = []

    # get the web service endpoints and bearer token headers
    endpoints: list = [
        [os.getenv('AWS_SETTINGS_URL'), {'Content-Type': 'application/json', 'Authorization': f'Bearer {os.environ.get("AWS_BEARER_TOKEN")}'}],
        [os.getenv('PRD_SETTINGS_URL'), {'Content-Type': 'application/json', 'Authorization': f'Bearer {os.environ.get("PRD_BEARER_TOKEN")}'}],
        [os.getenv('DEV_SETTINGS_URL'), {'Content-Type': 'application/json', 'Authorization': f'Bearer {os.environ.get("DEV_BEARER_TOKEN")}'}]]

    # create a list of target namespaces
    namespaces: list = ['AWS', 'PROD', 'DEV']

    # start collecting data
    try:
        # fore each deployment
        for item in endpoints:
            # create the URL
            url = f'{item[0]}/get_sv_component_versions'

            # execute the post
            data = requests.get(f'{item[0]}/get_sv_component_versions', headers=item[1], timeout=10)

            # was the call unsuccessful
            if data.status_code == 200:
                results.append(json.loads(data.text))
            else:
                # raise the issue
                raise Exception(f'Failure to get image version data from: {url}. HTTP Error: {data.status_code}')

        # now that we have all the data output something human-readable
        # for each workflow type gather the steps. we use the first dataset as the reference
        # for the workflow type and steps
        for ref_type, ref_steps in results[0].items():
            # get a reference to the workflow steps from the other namespaces
            wf_steps_0 = results[1].get(ref_type)
            wf_steps_1 = results[2].get(ref_type)

            # init the output of the namespace results
            image_list: list = []

            # loop through the workflow steps
            for index, component in enumerate(ref_steps):
                # get the component name
                component_name = list(component.keys())[0]

                # init the message for the component status
                status_msg: str = ''

                # get the name of the image from the workflow step without the container registry bit
                ref_image_name = ref_steps[index].get(component_name).split('/')[-1]
                image_name_0 = wf_steps_0[index].get(component_name).split('/')[-1]
                image_name_1 = wf_steps_1[index].get(component_name).split('/')[-1]

                # check to see if there are any version mismatches
                if ref_image_name != image_name_0 or ref_image_name != image_name_1:
                    # set the warning flag
                    status_msg = (
                        f'Mismatch found for {component_name} - '
                        f'{namespaces[0]}: {ref_image_name.split(":")[-1]}, '
                        f'{namespaces[1]}: {image_name_0.split(":")[-1]}, '
                        f'{namespaces[2]}: {image_name_1.split(":")[-1]}')
                else:
                    status_msg = f'All namespace image versions match for {ref_image_name}'

                # save the data for this component
                image_list.append(status_msg)

            # add the item to the return list
            ret_val.append({ref_type: image_list})

    except Exception:
        # return a failure message
        ret_val = [{'Error': 'Exception detected trying to get all the component versions.'}]

        logger.exception('Exception: Request failure getting all component image versions.')

        # set the status to a server error
        status_code = 500

    # return to the caller
    return JSONResponse(content=ret_val, status_code=status_code, media_type="application/json")


@APP.get('/get_sv_component_versions', dependencies=[Depends(JWTBearer(security))], status_code=200, response_model=None)
async def get_sv_component_versions() -> json:
    """
    gets the SV image versions for this namespace

    :return:
    """
    # init the returned html status code
    status_code = 200

    # init the return
    ret_val: dict = {}

    try:
        # try to make the call for records
        job_defs: dict = db_info.get_job_defs()

        # pull out the info needed for each workflow type
        for workflow_type in job_defs:
            # get the workflow type name
            workflow_type_name = list(workflow_type.keys())[0]

            # init a list for the step dicts
            steps: list = []

            # walk through the steps and grab the docker image version details
            for step in workflow_type[workflow_type_name]:
                # save the step image details
                steps.append({list(step.keys())[0]: step.get(list(step.keys())[0])['IMAGE']})

            # add the steps to this workflow type dict
            ret_val.update({workflow_type_name: steps})

    except Exception:
        # return a failure message
        ret_val = {'Error': 'Exception detected trying to get the component versions.'}

        # log the exception
        logger.exception(ret_val)

        # set the status to a server error
        status_code = 500

    # return to the caller
    return JSONResponse(content=ret_val, status_code=status_code, media_type="application/json")


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
    Resets the job process order to the default for the workflow selected.

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
async def get_the_log_file_list(filter_param: str = '', search_backups: bool = False):
    """
    Gets the log file list. An optional filter parameter (case-insensitive) can be used to search for targeted results.

    """

    # return the list to the caller in JSON format
    return JSONResponse(content={'Response': GenUtils.get_log_file_list(filter_param, search_backups)}, status_code=200,
                        media_type="application/json")


@APP.get("/get_log_file/", dependencies=[Depends(JWTBearer(security))], response_model=None)
async def get_the_log_file(log_file: str, search_backups: bool = False):
    """
    Gets the log file specified. This method only expects a properly named file.

    """
    # make sure we got a log file
    if log_file:
        # get the log file path
        if search_backups:
            # get the path to the archive directory
            log_file_path = os.getenv('LOG_BACKUP_PATH', os.path.dirname(__file__))
        else:
            # init the log file path
            log_file_path: str = LoggingUtil.get_log_path()

        # get the full path to the file
        target_log_file_path = os.path.join(log_file_path, log_file)

        # loop through the log file directory
        for found_log_file in Path(log_file_path).rglob('*log*'):
            # if the target file is found in the log directory
            if target_log_file_path == str(found_log_file):
                # return the file to the caller
                return FileResponse(path=target_log_file_path, filename=log_file, media_type='text/plain')

        # if we get here return an error
        return JSONResponse(content={'Response': 'Error - Log file does not exist.'}, status_code=404, media_type="application/json")

    # if we get here return an error
    return JSONResponse(content={'Response': 'Error - You must select a log file.'}, status_code=404, media_type="application/json")


@APP.get("/get_run_properties", dependencies=[Depends(JWTBearer(security))], status_code=200, response_model=None)
async def get_the_run_properties(instance_id: int, uid: str):
    """
    Gets the run properties for the run specified.

    """
    # init the returned html status code
    status_code = 200

    try:
        # try to make the call for records
        ret_val = db_info.get_run_props(instance_id, uid)

    except Exception:
        # return a failure message
        ret_val = f'Exception detected trying to get the run properties for {instance_id}-{uid}.'

        # log the exception
        logger.exception(ret_val)

        # set the status to a server error
        status_code = 500

    # return to the caller
    return JSONResponse(content=ret_val, status_code=status_code, media_type="application/json")


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
