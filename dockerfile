FROM apache/airflow:2.9.0-python3.9

USER root

# Copy the requirements file
COPY requirements.txt /requirements.txt

# Switch to airflow user before running pip
USER airflow

# Install required Python packages
RUN pip install --no-cache-dir -r /requirements.txt

WORKDIR /opt/airflow

# Copy other necessary directories 
COPY ./scripts ./scripts
COPY ./config ./config
COPY ./database ./database
COPY ./notebooks ./notebooks
COPY ./dags ./dags
COPY ./plugins ./plugins

