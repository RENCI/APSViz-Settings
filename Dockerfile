# This Dockerfile is used to build THE APSVIZ-Settings python image
# starts with the python image
# creates a directory for the repo
# gets the APSVIZ-Settings repo
# and runs main which starts the web server

FROM python:3.9.7

# get some credit
LABEL maintainer="powen@renci.org"

# install basic tools
RUN apt-get update

# make a directory for the repo
RUN mkdir /repo

# go to the directory where we are going to upload the repo
WORKDIR /repo

# get the latest code
RUN git clone https://github.com/RENCI/APSVIZ-Settings.git

# go to the repo dir
WORKDIR /repo/APSVIZ-Settings

# install requirements
RUN pip install -r requirements.txt

# expose the default port
EXPOSE 4000

# start the service entry point
ENTRYPOINT ["python", "main.py"]