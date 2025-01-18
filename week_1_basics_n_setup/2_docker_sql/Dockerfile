FROM python:3.9.1

RUN apt-get install wget 
RUN pip install pandas sqlalchemy psycopg2 


# Create a directory called app in the docker container and copy the local copy of pipeline.py into this app directory with the name pipeline.py
WORKDIR /app
COPY ingest_data.py ingest_data.py 

ENTRYPOINT ["python", "ingest_data.py"]