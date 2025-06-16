FROM python:3.11-bookworm

ENV AIRFLOW_HOME=/opt/airflow
ENV AIRFLOW_VERSION=2.9.1
WORKDIR /opt/airflow
RUN pip install poetry==1.4.2
COPY pyproject.toml poetry.lock /opt/airflow/
RUN poetry config virtualenvs.create false && poetry install --no-root
RUN airflow db migrate && airflow users create --username airflow --firstname Peter --lastname Parker --role Admin --password airflow --email spiderman@superhero.org
COPY td7/ /opt/airflow/td7
COPY README.md .env /opt/airflow/
RUN poetry install
CMD airflow webserver --port 8080 & airflow scheduler
