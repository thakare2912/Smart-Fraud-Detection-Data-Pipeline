# -------------------------------
# BASE IMAGE
# -------------------------------
FROM apache/airflow:2.8.1

# -------------------------------
#  ENV VARS
# -------------------------------
ENV AIRFLOW_HOME=/opt/airflow

# -------------------------------
#  SWITCH TO AIRFLOW USER
# -------------------------------
USER root

# -------------------------------
# INSTALL SYSTEM DEPENDENCIES (OPTIONAL)
# For example: curl, gcc, etc.
# -------------------------------
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    python3-dev \
    build-essential \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# -------------------------------
# SWITCH BACK TO airflow USER
# -------------------------------
USER airflow

# -------------------------------
# COPY YOUR REQUIREMENTS (OPTIONAL)
# Example: if you want custom Python libs beyond _PIP_ADDITIONAL_REQUIREMENTS
# -------------------------------
 COPY ./src/airflow/requirements/requirements.txt /requirements.txt


RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /requirements.txt

# -------------------------------
# COPY ANY CUSTOM SCRIPTS / DAGS (OPTIONAL)
# If needed — usually mounted as volumes though
# -------------------------------
# COPY ./src/airflow/dags /opt/airflow/dags
# COPY ./src/airflow/plugins /opt/airflow/plugins

# -------------------------------
# ✅ DEFAULT CMD (Docker Compose overrides this)
# -------------------------------
CMD ["webserver"]
