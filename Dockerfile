FROM python:3.10-slim-buster

# skip debconf: delaying package configuration, since apt-utils is not installed
ARG DEBIAN_FRONTEND=noninteractive

# update system && install system-reqb
RUN apt-get update \
    && apt-get install gcc -y \
    && apt-get install g++ -y \
    && apt-get install git -y \
    && apt-get clean && \
    rm -rf /var/lib/apt/lists/*

#create user and group
RUN groupadd -r ch && useradd --no-log-init -m -r -g ch ch

#update path env
ENV PATH="/home/ch/.local/bin:${PATH}"

# create working dir
RUN mkdir /app && chown ch:ch /app

# switch to non-root user
USER ch
WORKDIR /app

# copy files to workdir
COPY --chown=ch:ch . /app/
# install req
RUN pip install --upgrade pip && pip install -r requirements.txt
