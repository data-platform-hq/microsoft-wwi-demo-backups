FROM mcr.microsoft.com/mssql/server:2019-latest

ARG ACCEPT_EULA=Y

USER root

RUN export ACCEPT_EULA=${ACCEPT_EULA}
RUN export DEBIAN_FRONTEND=noninteractive

RUN ACCEPT_EULA=${ACCEPT_EULA} DEBIAN_FRONTEND=noninteractive

RUN ACCEPT_EULA=${ACCEPT_EULA} DEBIAN_FRONTEND=noninteractive apt update -y
RUN ACCEPT_EULA=${ACCEPT_EULA} DEBIAN_FRONTEND=noninteractive apt upgrade -y

RUN apt install curl -y

RUN apt-get update && apt-get install -y gnupg2
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
RUN curl https://packages.microsoft.com/config/ubuntu/20.04/prod.list | tee /etc/apt/sources.list.d/msprod.list

RUN apt-get update 
RUN apt-get install mssql-tools unixodbc-dev -y

RUN mkdir /opt/mssql/data
