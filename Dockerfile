FROM jupyter/pyspark-notebook:latest

ENV PYSPARK_MAJOR_PYTHON_VERSION=3
# so we can import modules from app folder
ENV PYTHONPATH /app
WORKDIR /app

# RUN wget  https://jdbc.postgresql.org/download/postgresql-42.2.5.jar
# RUN mv postgresql-42.2.5.jar /opt/spark/jars

COPY requirements.txt .
RUN pip3 install -r requirements.txt

COPY . /app