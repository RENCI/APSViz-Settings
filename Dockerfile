# This Dockerfile is used to build THE APSVIZ-Settings python image
# starts with the python image
# creates a directory for the repo
# gets the APSVIZ-Settings repo
# and runs main which starts the web server

FROM renciorg/renci-python-image:v0.0.1

# get some credit
LABEL maintainer="powen@renci.org"

# install basic tools
RUN apt-get update

# create log level env param (debug=10, info=20)
ENV LOG_LEVEL 20

# make a directory for the repo
RUN mkdir /repo

# go to the directory where we are going to upload the repo
WORKDIR /repo

# get the latest code
RUN git clone https://github.com/RENCI/APSVIZ-Settings.git

# go to the repo dir
WORKDIR /repo/APSVIZ-Settings

# make sure everything is read/write in the repo code
RUN chmod 777 -R .

# install requirements
RUN pip install -r requirements.txt

# switch to the non-root user (nru). defined in the base image
USER nru

# expose the default port
EXPOSE 4000

# start the service entry point
ENTRYPOINT ["python", "main.py"]