FROM --platform=linux/amd64 ghcr.io/dbt-labs/dbt-postgres:1.8.latest
RUN apt update -y && apt upgrade -y

COPY . /dbt
WORKDIR /dbt

RUN dbt deps

ENTRYPOINT ["dbt"]
