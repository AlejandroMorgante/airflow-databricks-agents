ARG AIRFLOW_VERSION=3.3.2
FROM apache/airflow:slim-${AIRFLOW_VERSION}-python3.12

# Use main by default; pass a commit SHA to reproduce a particular provider build.
ARG AIRFLOW_VERSION
ARG AIRFLOW_SOURCE_REF=main

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir \
        "apache-airflow==${AIRFLOW_VERSION}" \
        -r /tmp/requirements.txt \
        "apache-airflow-providers-common-compat @ https://github.com/apache/airflow/archive/${AIRFLOW_SOURCE_REF}.tar.gz#subdirectory=providers/common/compat" \
        "apache-airflow-providers-databricks[common.ai] @ https://github.com/apache/airflow/archive/${AIRFLOW_SOURCE_REF}.tar.gz#subdirectory=providers/databricks" \
    && pip check

COPY scripts/*.py /opt/airflow-tools/
