FROM apache/airflow:2.9.0-python3.9

USER root

# Copy the requirements file
COPY docker/airflow/requirements.txt /tmp/requirements.txt

# Switch back to airflow user
USER airflow

# Install required Python packages
RUN pip install --no-cache-dir -r /tmp/requirements.txt


# Set working directory
WORKDIR /opt/airflow

# Copy project files relative to Dockerfile location (docker/airflow/)
# => ".." là bước ra khỏi thư mục docker/airflow/ để vào root project

COPY ../../etl/dags /opt/airflow/dags
COPY ../../etl/scripts /opt/airflow/scripts
COPY ../../config /opt/airflow/config
COPY ../../database /opt/airflow/database
COPY ../../notebooks /opt/airflow/notebooks
# COPY ../../plugins /opt/airflow/plugins
