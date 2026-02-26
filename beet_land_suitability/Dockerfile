
FROM apache/airflow:2.8.1-python3.11

USER airflow

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install dbt-bigquery==1.7.0 # Install dbt-bigquery

# Copy DAGs and dags_dependencies
COPY airflow/dags ${AIRFLOW_HOME}/dags
COPY airflow/dags_dependencies ${AIRFLOW_HOME}/dags_dependencies

# Copy dbt project
COPY dbt ${AIRFLOW_HOME}/dbt

# Copy Flask app (if running in the same container, though typically separate)
# For a CV project, demonstrating all components in one Dockerfile might be acceptable
# For production, Flask app would be in its own Dockerfile.
COPY flask_app ${AIRFLOW_HOME}/flask_app

# Set environment variables for GEE authentication (if using service account)
# ENV GOOGLE_APPLICATION_CREDENTIALS=/path/to/your/keyfile.json

# Expose Airflow webserver port (if running webserver in this container)
EXPOSE 8080

# You might want to add an entrypoint or command here depending on how you plan to run this Docker image.
# For example, to run Airflow webserver:
# CMD ["airflow", "webserver"]
