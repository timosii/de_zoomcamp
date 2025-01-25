FROM python:3.12

RUN apt-get install wget
RUN pip install pandas sqlalchemy psycopg2 pyarrow
WORKDIR /app
COPY data_inserting.py data_inserting.py
ENTRYPOINT ["python", "data_inserting.py"]