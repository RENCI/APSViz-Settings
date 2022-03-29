"""APSVIZ settings server."""
import json
import logging
import os
import re
from enum import Enum

from fastapi import FastAPI, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse

from common.logging import LoggingUtil
from src.pg_utils import PGUtils

# set the app version
APP_VERSION = 'v0.0.4'

# get the log level and directory from the environment.
# level comes from the container dockerfile, path comes from the k8s secrets
log_level: int = int(os.getenv('LOG_LEVEL', logging.INFO))
log_path: str = os.getenv('LOG_PATH', os.path.join(os.path.dirname(__file__), 'logs'))

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
    allow_headers=["*"],
)

# declare the component image names
image_name: dict = {
    'hazus-': 'renciorg/adras:',
    'hazus-singleton-': 'renciorg/adras:',
    'obs-mod-supp-job-': 'renciorg/adcirc_supp:',
    'run-geo-tiff-job-': 'renciorg/adcirc2mbtiles:',
    'compute-mbtiles-job-0-10-': 'renciorg/adcirc2mbtiles:',
    'compute-mbtiles-job-11-': 'renciorg/adcirc2mbtiles:',
    'compute-mbtiles-job-12-': 'renciorg/adcirc2mbtiles:',
    'staging-': 'renciorg/stagedata:',
    'final-staging-job-': 'renciorg/stagedata:',
    'load-geo-server-job-': 'renciorg/load_geoserver:'}


# declare the job names
class JobName(str, Enum):
    hazus = 'hazus'
    hazus_singleton = 'hazus-singleton'
    obs_mod_supp_job = 'obs-mod-supp-job'
    run_geo_tiff_job = 'run-geo-tiff-job'
    compute_mbtiles_job_0_10 = 'compute-mbtiles-job-0-10'
    compute_mbtiles_job_11 = 'compute-mbtiles-job-11'
    compute_mbtiles_job_12 = 'compute-mbtiles-job-12'
    staging = 'staging'
    final_staging_job = 'final-staging-job'
    load_geo_server_job = 'load-geo-server-job'


# declare the run status types
class RunStatus(str, Enum):
    hazus = 'hazus'
    new = 'new'
    do_not_rerun = 'do not rerun'
    debug = 'debug'


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
                url = f'{hostname}/get_log_file/?log_file_path={file_path}'

                # save the absolute file path, endpoint url, and file size in a dict
                ret_val.update({f'{name}_{counter}': {'file_path': file_path, 'url': f'{url}', 'file_size': f'{os.path.getsize(file_path)} bytes'}})

                logger.debug(f'get_file_list(): url: {url}')

    # return the list to the caller
    return ret_val


@APP.get('/get_job_defs', status_code=200)
async def display_job_definitions() -> json:
    """
    Displays the job definitions.

    """

    # init the returned html status code
    status_code = 200

    try:
        # create the postgres access object
        pg_db = PGUtils()

        # try to make the call for records
        job_data = pg_db.get_job_defs()

        # get the data looking like we are used to
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
        logger.exception(ret_val, e)

        # set the status to a server error
        status_code = 500

    # return to the caller
    return JSONResponse(content=ret_val, status_code=status_code, media_type="application/json")


@APP.get("/get_log_file/")
async def get_the_log_file(log_file_path: str = Query('log_file_path')):
    """
    Gets the log file specified. This method expects the full file path.

    """

    # return the file to the caller
    return FileResponse(path=log_file_path, filename=os.path.basename(log_file_path), media_type='text/plain')


@APP.get("/get_log_file_list")
async def get_the_log_file_list(request: Request):
    """
    Gets the log file list. each of these entries could be used in the get_log_file endpoint

    """

    # return the list to the caller in JSON format
    return JSONResponse(content={'Response': get_log_file_list(f'{request.base_url.scheme}://{request.base_url.netloc}')}, status_code=200, media_type="application/json")


@APP.get("/get_run_list", status_code=200)
async def get_the_run_list():
    """
    Gets the run information for the last 100 runs.

    """

    # init the returned html status code
    status_code = 200

    try:
        # create the postgres access object
        pg_db = PGUtils()

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
        logger.exception(ret_val, e)

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
            pg_db = PGUtils()

            # try to make the update
            pg_db.update_run_status(instance_id, uid, status)

            # return a success message
            ret_val = f'The status of run {instance_id}/{uid} has been set to {status}'
        except Exception as e:
            # return a failure message
            ret_val = f'Exception detected trying to update run {instance_id}/{uid} to {status}'

            # log the exception
            logger.exception(ret_val, e)

            # set the status to a server error
            status_code = 500
    else:
        # return a failure message
        ret_val = f'Error: The instance if {instance_id} is invalid. An instance must be a non-zero integer.'

        # log the error
        logger.error(ret_val)

        # set the status to a bad request
        status_code = 400

    # return to the caller
    return JSONResponse(content={'Response': ret_val}, status_code=status_code, media_type="application/json")


# updates the image version for a job
@APP.put('/job_name/{job_name}/image_version/{version}', status_code=200)
async def set_the_supervisor_component_image_version(job_name: JobName, version: str):
    """
    Updates a supervisor component image version label in the supervisor job run configuration.

    Notes:
     - The version label must match what has been uploaded to docker hub

    """
    # init the returned html status code
    status_code = 200

    try:
        # create a regex pattern for the version number
        pattern = re.compile(r"([v]\d\.+\d\.+\d)")

        # insure that the input params are legit
        if pattern.search(version):
            # make sure the underscores end with a hyphen
            job_name = JobName(job_name).value + '-'

            # create the postgres access object
            pg_db = PGUtils()

            # try to make the update
            pg_db.update_job_image(job_name, image_name[job_name] + version)

            # return a success message
            ret_val = f'The docker image:version for job name {job_name} has been set to {image_name[job_name] + version}'
        else:
            # return a success message
            ret_val = f'Error: The version {version} is invalid. Please use a value in the form of v<int>.<int>.<int>'

            # log the error
            logger.error(ret_val)

            # set the status to a bad request
            status_code = 400

    except Exception as e:
        # return a failure message
        ret_val = f'Exception detected trying to update the image version. Job name {job_name}, version: {version}'

        # log the exception
        logger.exception(ret_val, e)

        # set the status to a server error
        status_code = 500

    # return to the caller
    return JSONResponse(content={'Response': ret_val}, status_code=status_code, media_type="application/json")
