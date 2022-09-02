# SPDX-FileCopyrightText: 2022 Renaissance Computing Institute. All rights reserved.
#
# SPDX-License-Identifier: GPL-3.0-or-later
# SPDX-License-Identifier: LicenseRef-RENCI
# SPDX-License-Identifier: MIT

"""APSVIZ settings server."""
import json
import logging
import os
import re

from enum import Enum
from typing import Union

from fastapi import FastAPI, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse

from common.logger import LoggingUtil
from src.pg_utils import PGUtils

# set the app version
APP_VERSION = 'v0.0.16'

# get the log level and directory from the environment.
# level comes from the container dockerfile, path comes from the k8s secrets
log_level: int = int(os.getenv('LOG_LEVEL', logging.INFO))
log_path: str = os.getenv('LOG_PATH', os.path.dirname(__file__))

# get the DB connection details for the asgs DB
asgs_dbname = os.environ.get('ASGS_DB_DATABASE')
asgs_username = os.environ.get('ASGS_DB_USERNAME')
asgs_password = os.environ.get('ASGS_DB_PASSWORD')

# get the DB connection details for the apsviz DB
apsviz_dbname = os.environ.get('APSVIZ_DB_DATABASE')
apsviz_username = os.environ.get('APSVIZ_DB_USERNAME')
apsviz_password = os.environ.get('APSVIZ_DB_PASSWORD')

# create the dir if it does not exist
if not os.path.exists(log_path):
    os.mkdir(log_path)

# create a logger
logger = LoggingUtil.init_logging("APSVIZ.Settings", level=log_level, line_format='medium', log_file_path=log_path)

# declare the FastAPI details
APP = FastAPI(
    title='APSVIZ Settings',
    version=APP_VERSION
)

# declare app access details
APP.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)


# declare the job type names
class JobTypeName(str, Enum):
    adcirc2cog_tiff_job = 'adcirc2cog-tiff-job'
    adcirctime_to_cog_job = 'adcirctime-to-cog-job'
    ast_run_harvester_job = 'ast-run-harvester-job'
    # compute_mbtiles_job_0_10 = 'compute-mbtiles-job-0-10'
    # compute_mbtiles_job_11 = 'compute-mbtiles-job-11'
    # compute_mbtiles_job_12 = 'compute-mbtiles-job-12'
    final_staging_job = 'final-staging-job'
    geotiff2cog_job = 'geotiff2cog-job'
    hazus = 'hazus'
    # hazus_singleton = 'hazus-singleton'
    load_geo_server_job = 'load-geo-server-job'
    obs_mod_ast_job = 'obs-mod-ast-job'
    # obs_mod_supp_job = 'obs-mod-supp-job'
    # run_geo_tiff_job = 'run-geo-tiff-job'
    staging = 'staging'


# declare the job type names
class NextJobTypeName(str, Enum):
    adcirc2cog_tiff_job = 'adcirc2cog-tiff-job'
    adcirctime_to_cog_job = 'adcirctime-to-cog-job'
    ast_run_harvester_job = 'ast-run-harvester-job'
    complete = 'complete'
    # compute_mbtiles_job_0_10 = 'compute-mbtiles-job-0-10'
    # compute_mbtiles_job_11 = 'compute-mbtiles-job-11'
    # compute_mbtiles_job_12 = 'compute-mbtiles-job-12'
    final_staging_job = 'final-staging-job'
    geotiff2cog_job = 'geotiff2cog-job'
    hazus = 'hazus'
    # hazus_singleton = 'hazus-singleton'
    load_geo_server_job = 'load-geo-server-job'
    obs_mod_ast_job = 'obs-mod-ast-job'
    # obs_mod_supp_job = 'obs-mod-supp-job'
    # run_geo_tiff_job = 'run-geo-tiff-job'
    staging = 'staging'


# declare the run status types
class RunStatus(str, Enum):
    new = 'new'
    debug = 'debug'
    do_not_rerun = 'do not rerun'


# declare the possible image repos
class ImageRepo(str, Enum):
    containers = 'containers.renci.org'
    renciorg = 'renciorg'


image_repo_to_repo_name: dict = {
    'renciorg': 'renciorg',
    'containers.renci.org': 'containers.renci.org/eds'
}

# declare the component job type image name
job_type_to_image_name: dict = {
    'adcirc2cog-tiff-job': '/adcirc2cog:',
    'adcirctime-to-cog-job': '/adcirctime2cogs:',
    'ast-run-harvester-job': '/ast_run_harvester',
    # 'compute-mbtiles-job-0-10': '/adcirc2mbtiles:',
    # 'compute-mbtiles-job-11': '/adcirc2mbtiles:',
    # 'compute-mbtiles-job-12': '/adcirc2mbtiles:',
    'final-staging-job': '/stagedata:',
    'geotiff2cog-job': '/adcirc2cog:',
    'hazus': '/adras:',
    # 'hazus-singleton': '/adras:',
    'load-geo-server-job': '/load_geoserver:',
    'obs-mod-ast-job': '/ast_supp:',
    # 'obs-mod-supp-job': '/adcirc_supp:',
    # 'run-geo-tiff-job': '/adcirc2mbtiles:',
    'staging': '/stagedata:'
}

# declare job name to id
job_type_name_to_id: dict = {
  "adcirc2cog-tiff-job": 23,
  'adcirctime-to-cog-job': 26,
  'ast-run-harvester-job': 27,
  "complete": 21,
  # "compute-mbtiles-job-0-10": 16,
  # "compute-mbtiles-job-11": 17,
  # "compute-mbtiles-job-12": 18,
  "final-staging-job": 20,
  "geotiff2cog-job": 24,
  "hazus": 12,
  # "hazus-singleton": 13,
  "load-geo-server-job": 19,
  "obs-mod-ast-job": 25,
  # "obs-mod-supp-job": 14,
  # "run-geo-tiff-job": 15,
  "staging": 11
}


def get_log_file_list(hostname):
    """
    Gets all the log file path/names

    :return:
    """
    # create a regex
    rx = re.compile(r'\.(log)')

    # init the return
    ret_val = {}

    # init a file counter
    counter = 0

    # go through the root and sub-dirs to find the log files
    for path, dnames, fnames in os.walk(log_path):
        # for each name in that path
        for name in fnames:
            # is it a log file
            if rx.search(name):
                # increment the counter
                counter += 1

                # create the path to the file
                file_path = os.path.join(path, name).replace('\\', '/')
                url = f'{hostname}/get_log_file/?log_file={file_path.replace(log_path, "")[1:]}'

                # save the absolute file path, endpoint url, and file size in a dict
                ret_val.update({f'{name}_{counter}': {'file_name': file_path.replace(log_path, "")[1:], 'url': f'{url}', 'file_size': f'{os.path.getsize(file_path)} bytes'}})

                logger.debug(f'get_file_list(): url: {url}')

    # return the list to the caller
    return ret_val


@APP.get('/get_job_order', status_code=200)
async def display_job_order() -> json:
    """
    Displays the job process order.

    """

    # init the returned html status code
    status_code = 200

    try:
        # create the postgres access object
        pg_db = PGUtils(asgs_dbname, asgs_username, asgs_password)

        # try to make the call for records
        ret_val = pg_db.get_job_order()

    except Exception as e:
        # return a failure message
        ret_val = f'Exception detected trying to get the job order'

        # log the exception
        logger.exception(ret_val)

        # set the status to a server error
        status_code = 500

    # return to the caller
    return JSONResponse(content=ret_val, status_code=status_code, media_type="application/json")


@APP.get('/reset_job_order', status_code=200)
async def reset_job_order() -> json:
    """
    resets the job process order to the default.

    The normal sequence of jobs are:
    staging -> hazus -> obs-mod-ast -> adcirc to COGs -> compute COGs -> load geo server -> final staging -> complete

    """

    # init the returned html status code
    status_code = 200

    try:
        # create the postgres access object
        pg_db = PGUtils(asgs_dbname, asgs_username, asgs_password, auto_commit=False)

        # try to make the call for records
        ret_val = pg_db.reset_job_order()

        # check the return value for failure, failed == true
        if ret_val:
            raise 'Failure trying to reset the job order'
        else:
            # get the new job order
            job_order = pg_db.get_job_order()

        # return a success message with the new job order
        ret_val = [{'message': f'The job order has been reset to the default.'}, {'job_order': job_order}]

    except Exception as e:
        # return a failure message
        ret_val = f'Exception detected trying to get the job order'

        # log the exception
        logger.exception(ret_val)

        # set the status to a server error
        status_code = 500

    # return to the caller
    return JSONResponse(content=ret_val, status_code=status_code, media_type="application/json")


@APP.get('/get_job_defs', status_code=200)
async def display_job_definitions() -> json:
    """
    Displays the job definitions.

    """

    # init the returned html status code
    status_code = 200

    try:
        # create the postgres access object
        pg_db = PGUtils(asgs_dbname, asgs_username, asgs_password)

        # try to make the call for records
        job_data = pg_db.get_job_defs()

        # make the data readable
        ret_val = {list(x)[0]: x.get(list(x)[0]) for x in job_data}

        # fix the arrays for each job def.
        # they come in as a string
        for item in ret_val.items():
            item[1]['COMMAND_LINE'] = json.loads(item[1]['COMMAND_LINE'])
            item[1]['COMMAND_MATRIX'] = json.loads(item[1]['COMMAND_MATRIX'])

    except Exception as e:
        # return a failure message
        ret_val = f'Exception detected trying to get the job definitions'

        # log the exception
        logger.exception(ret_val)

        # set the status to a server error
        status_code = 500

    # return to the caller
    return JSONResponse(content=ret_val, status_code=status_code, media_type="application/json")


@APP.get('/get_terria_map_data', status_code=200)
async def get_terria_map_catalog_data(grid_type: Union[str, None] = Query(default=None),
                                      event_type: Union[str, None] = Query(default=None),
                                      instance_name: Union[str, None] = Query(default=None),
                                      run_date: Union[str, None] = Query(default=None),
                                      end_date: Union[str, None] = Query(default=None),
                                      limit: Union[int, None] = Query(default=4)) -> json:
    """
    Gets the json formatted terria map UI catalog data.
    <br/>Note: Leave filtering params empty if not desired.
    <br/>&nbsp;&nbsp;&nbsp;grid_type: Filter by the name of the ASGS grid
    <br/>&nbsp;&nbsp;&nbsp;event_type: Filter by the event type
    <br/>&nbsp;&nbsp;&nbsp;instance_name: Filter by the name of the ASGS instance
    <br/>&nbsp;&nbsp;&nbsp;run_date: Filter by the run date in the form of yyyy-mm-dd
    <br/>&nbsp;&nbsp;&nbsp;end_date: Filter by the data between the run date and end date
    <br/>&nbsp;&nbsp;&nbsp;limit: Limit the number of catalog records returned (default is 4)
    """

    # init the returned html status code
    status_code = 200

    try:
        # create the postgres access object
        pg_db = PGUtils(apsviz_dbname, apsviz_username, apsviz_password)

        # prep the data for the DB SP
        grid_type = 'null' if not grid_type else f"'{grid_type}'"
        event_type = 'null' if not event_type else f"'{event_type}'"
        instance_name = 'null' if not instance_name else f"'{instance_name}'"
        run_date = 'null' if not run_date else f"'{run_date}'"
        end_date = 'null' if not end_date else f"'{end_date}'"

        # try to make the call for records
        ret_val = pg_db.get_terria_map_catalog_data(grid_type, event_type, instance_name, run_date, end_date, limit)
    except Exception as e:
        # return a failure message
        ret_val = f'Exception detected trying to get the terria map catalog data.'

        # log the exception
        logger.exception(ret_val)

        # set the status to a server error
        status_code = 500

    # return to the caller
    return JSONResponse(content=ret_val, status_code=status_code, media_type="application/json")


@APP.get('/get_terria_map_data_file', status_code=200)
async def get_terria_map_catalog_data_file(file_name: Union[str, None] = Query(default='apsviz.json'),
                                           grid_type: Union[str, None] = Query(default=None),
                                           event_type: Union[str, None] = Query(default=None),
                                           instance_name: Union[str, None] = Query(default=None),
                                           run_date: Union[str, None] = Query(default=None),
                                           end_date: Union[str, None] = Query(default=None),
                                           limit: Union[int, None] = Query(default=4)) -> FileResponse:
    """
    Returns the json formatted terria map UI catalog data in a file specified.
    <br/>Note: Leave filtering params empty if not desired.
    <br/>&nbsp;&nbsp;&nbsp;file_name: The name of the output file (default is apsviz.json)
    <br/>&nbsp;&nbsp;&nbsp;grid_type: Filter by the name of the ASGS grid
    <br/>&nbsp;&nbsp;&nbsp;event_type: Filter by the event type
    <br/>&nbsp;&nbsp;&nbsp;instance_name: Filter by the name of the ASGS instance
    <br/>&nbsp;&nbsp;&nbsp;run_date: Filter by the run date in the form of yyyy-mm-dd
    <br/>&nbsp;&nbsp;&nbsp;end_date: Filter by the data between the run date and end date
    <br/>&nbsp;&nbsp;&nbsp;limit: Limit the number of catalog records returned (default is 2)
    """
    # init the returned html status code
    status_code = 200

    # get the full file path to the dummy file
    file_path = os.path.join(os.path.dirname(__file__), file_name)

    # prep the data for the DB SP
    grid_type = 'null' if not grid_type else f"'{grid_type}'"
    event_type = 'null' if not event_type else f"'{event_type}'"
    instance_name = 'null' if not instance_name else f"'{instance_name}'"
    run_date = 'null' if not run_date else f"'{run_date}'"
    end_date = 'null' if not end_date else f"'{end_date}'"

    try:
        # create the postgres access object
        pg_db = PGUtils(apsviz_dbname, apsviz_username, apsviz_password)

        # try to make the call for records
        ret_val = pg_db.get_terria_map_catalog_data(grid_type, event_type, instance_name, run_date, end_date, limit)

        # write out the data to a file
        with open(file_path, 'w') as fp:
            json.dump(ret_val, fp)

    except Exception as e:
        # log the exception
        logger.exception(e)

        # set the status to a server error
        status_code = 500

    # return to the caller
    return FileResponse(path=file_path, filename=file_name, media_type='text/json')


@APP.get("/get_log_file_list")
async def get_the_log_file_list(request: Request):
    """
    Gets the log file list. each of these entries could be used in the get_log_file endpoint

    """

    # return the list to the caller in JSON format
    return JSONResponse(content={'Response': get_log_file_list(f'{request.base_url.scheme}://{request.base_url.netloc}')}, status_code=200, media_type="application/json")


@APP.get("/get_log_file/")
async def get_the_log_file(log_file: str = Query('log_file')):
    """
    Gets the log file specified. This method only expects a properly named file.

    """
    # make sure this is a valid log file
    if log_file.endswith('.log') or (log_file[:-2].endswith('.log') and isinstance(int(log_file[-1]), int)):
        # append the log file to the log file path
        log_file_path = f'{os.path.join(log_path, log_file)}'

        # do some file path validation
        if os.path.exists(log_file_path):
            # return the file to the caller
            return FileResponse(path=log_file_path, filename=os.path.basename(log_file_path), media_type='text/plain')
        else:
            # return an error
            return JSONResponse(content={'Response': 'Error - Log file does not exist.'}, status_code=500, media_type="application/json")
    else:
        # return an error
        return JSONResponse(content={'Response': 'Error - Invalid log file name.'}, status_code=500, media_type="application/json")


@APP.get("/get_run_list", status_code=200)
async def get_the_run_list():
    """
    Gets the run information for the last 100 runs.

    """

    # init the returned html status code
    status_code = 200

    try:
        # create the postgres access object
        pg_db = PGUtils(asgs_dbname, asgs_username, asgs_password)

        # get the run records
        ret_val = pg_db.get_run_list()

        # add a final status to each record
        for r in ret_val:
            if r['status'].find('Error') > -1:
                r['final_status'] = 'Error'
            else:
                r['final_status'] = 'Success'

    except Exception as e:
        # return a failure message
        ret_val = f'Exception detected trying to gather run data'

        # log the exception
        logger.exception(ret_val)

        # set the status to a server error
        status_code = 500

    # return to the caller
    return JSONResponse(content={'Response': ret_val}, status_code=status_code, media_type="application/json")


# sets the run.properties run status to 'new' for a job
@APP.put('/instance_id/{instance_id}/uid/{uid}/status/{status}', status_code=200)
async def set_the_run_status(instance_id: int, uid: str, status: RunStatus = RunStatus('new')):
    """
    Updates the run properties run status of a job.

    ex: instance id: 3057, uid: 2021062406-namforecast, status: do not rerun

    """
    # init the returned html status code
    status_code = 200

    # is this a valid instance id
    if instance_id > 0:
        try:
            # create the postgres access object
            pg_db = PGUtils(asgs_dbname, asgs_username, asgs_password)

            # try to make the update
            pg_db.update_run_status(instance_id, uid, status)

            # return a success message
            ret_val = f'The status of run {instance_id}/{uid} has been set to {status}'
        except Exception as e:
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
@APP.put('/image_repo/{image_repo}/job_type_name/{job_type_name}/image_version/{version}', status_code=200)
async def set_the_supervisor_component_image_version(image_repo: ImageRepo, job_type_name: JobTypeName, version: str):
    """
    Updates a supervisor component image version label in the supervisor job run configuration.

    Notes:
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
            # create the postgres access object
            pg_db = PGUtils(asgs_dbname, asgs_username, asgs_password)

            # make the update. fix the job name (hyphen) so it matches the DB format
            pg_db.update_job_image_version(JobTypeName(job_type_name).value + '-', image_repo_to_repo_name[image_repo] + job_type_to_image_name[job_type_name] + version)

            # return a success message
            ret_val = f'The docker repo/image:version for job name {job_type_name} has been set to {image_repo_to_repo_name[image_repo] + job_type_to_image_name[job_type_name] + version}'
        else:
            # return a success message
            ret_val = f'Error: The version {version} is invalid. Please use a value in the form of v<int>.<int>.<int>'

            # log the error
            logger.error(ret_val)

            # set the status to a bad request
            status_code = 400

    except Exception as e:
        # return a failure message
        ret_val = f'Exception detected trying to update the image version {job_type_name} to version {version}'

        # log the exception
        logger.exception(ret_val)

        # set the status to a server error
        status_code = 500

    # return to the caller
    return JSONResponse(content={'Response': ret_val}, status_code=status_code, media_type="application/json")


# Updates a supervisor component's next process.
@APP.put('/job_type_name/{job_type_name}/next_job_type/{next_job_type_name}', status_code=200)
async def set_the_supervisor_job_order(job_type_name: JobTypeName, next_job_type_name: NextJobTypeName):
    """
    Modifies the supervisor component's linked list of jobs. Select the job process name and then select it's next job process name.

    The normal sequence of jobs are:
    staging -> hazus -> obs-mod-ast -> adcirc to COGs -> compute COGs -> load geo server -> final staging -> complete
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
                # create the postgres access object
                pg_db = PGUtils(asgs_dbname, asgs_username, asgs_password)

                # prep the record to update key. complete does not have a hyphen
                if job_type_name != 'complete':
                    job_type_name += '-'

                # make the update
                pg_db.update_next_job_for_job(job_type_name, next_job_type_id)

                # get the new job order
                job_order = pg_db.get_job_order()

                # return a success message with the new job order
                ret_val = [{'message': f'The {job_type_name} next process has been set to {next_job_type_name}'}, {'new_order': job_order}]
            else:
                # set the error msg
                ret_val = f'The next job process ID was not found for {next_job_type_name}'

                # declare an error for the user
                status_code = 500

    except Exception as e:
        # return a failure message
        ret_val = f'Exception detected trying to update the next job name. Job name {job_type_name}, next job name: {next_job_type_name}'

        # log the exception
        logger.exception(ret_val)

        # set the status to a server error
        status_code = 500

    # return to the caller
    return JSONResponse(content={'Response': ret_val}, status_code=status_code, media_type="application/json")
