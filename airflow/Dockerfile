ARG AIRFLOW_VERSION
ARG PYTHON_VERSION

FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

USER root

RUN apt update && \
    apt-get install -y default-jdk && \
    apt-get install -y ant && \
    apt-get clean;

ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64/
RUN export JAVA_HOME


USER airflow
COPY ./requirements-airflow.txt /
RUN pip install -r /requirements-airflow.txt