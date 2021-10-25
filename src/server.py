"""APSVIZ settings server."""
import os
import logging
import json

from common.logging import LoggingUtil
from enum import Enum
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from src.pg_utils import PGUtils

# get the log level and directory from the environment.
# level comes from the container dockerfile, level and path both come from the k8s secrets
log_level: int = int(os.getenv('LOG_LEVEL', logging.INFO))
log_path: str = os.getenv('LOG_PATH', os.path.dirname(__file__))

# create the dir if it does not exist
if not os.path.exists(log_path):
    os.mkdir(log_path)

# create a logger
logger = LoggingUtil.init_logging("APSVIZ.Settings.pg_utils", level=log_level, line_format='medium', log_file_path=log_path)

# set the app version
APP_VERSION = '0.0.1'

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


# declare the
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


class RunStatus(str, Enum):
    hazus = 'hazus'
    new = 'new'
    do_not_rerun = 'do not rerun'
    debug = 'debug'


# declare the
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


# updates the image version for a job
@APP.put('/job_name/{job_name}/image_version/{version}', status_code=200)
async def set_the_supervisor_image_version(job_name: JobName, version: str):
    """
    Updates the jobs image version.

    Notes:
     - the resultant version identifier must match what has been uploaded to docker hub

    :param job_name:
    :param version:
    :return:
    """
    # init the returned html status code
    status_code = 200

    try:
        # insure that the input params are legit

        # make sure the underscores end with a hyphen
        job_name = JobName(job_name).value + '-'

        # create the postgres access object
        pg_db = PGUtils()

        # try to make the update
        pg_db.update_job_image(job_name, image_name[job_name] + version)

        # return a success message
        ret_val = f'The docker image:version for job name {job_name} has been set to {image_name[job_name] + version}'
    except Exception as e:
        # return a failure message
        ret_val = f'Exception detected trying to update the image version. Job name {job_name}, version: {version}'

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
    Updates the run properties run status to 'new' for a job.

    ex: instance id: 3057, uid: 2021062406-namforecast, status: do not rerun

    :param instance_id:
    :param uid:
    :param status:
    :return:
    """

    # init the returned html status code
    status_code = 200

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

    # return to the caller
    return JSONResponse(content={'Response': ret_val}, status_code=status_code, media_type="application/json")


@APP.get('/get_job_defs', status_code=200)
async def display_job_definitions() -> json:
    """
    Displays the job definitions.

    :return:
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
