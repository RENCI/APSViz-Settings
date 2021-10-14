"""aragorn one hop server."""
import os
import logging

from common.logging import LoggingUtil
from enum import Enum
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from src.pg_utils import PGUtils

# get the log level and directory from the environment.
# level comes from the container dockerfile, path comes from the k8s secrets
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


# declare the
image_name: dict = {
    'hazus-': 'renciorg/adras:v',
    'hazus-singleton-': 'renciorg/adras:v',
    'obs-mod-supp-job-': 'renciorg/adcirc_supp:v',
    'run-geo-tiff-job-': 'renciorg/adcirc2mbtiles:v',
    'compute-mbtiles-job-0-10-': 'renciorg/adcirc2mbtiles:v',
    'compute-mbtiles-job-11-': 'renciorg/adcirc2mbtiles:v',
    'compute-mbtiles-job-12-': 'renciorg/adcirc2mbtiles:v',
    'staging-': 'renciorg/stagedata:v',
    'final-staging-job-': 'renciorg/stagedata:v',
    'load-geo-server-job-': 'renciorg/load_geoserver:v'}


# updates the image version for a job
@APP.get('/job_name/{job_name}/image_version/{version}', status_code=200)
async def get_supervisor_image_version(job_name: JobName, version: str):
    """
    updates the jobs image version

    :param job_name:
    :param version:
    :return:
    """

    try:
        # insure that the input params are legit

        # make sure the underscores are hyphens
        job_name = JobName(job_name).value + '-'

        # create the postgres access object
        pg_db = PGUtils()

        # try to make the update
        pg_db.update_job_image(job_name, image_name[job_name] + version)

        # return a success message
        ret_val = f'The docker image:version for job name {job_name} has been set to {image_name[job_name] + version}'
    except Exception as e:
        # return a failure message
        ret_val = f'Exception detected trying to update the image version. Job name {job_name}, version" {version}'

        # log the exception
        logger.exception(ret_val, e)

    # return to the caller
    return ret_val
