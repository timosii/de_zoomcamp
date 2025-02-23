FROM python:3.12

RUN pip install pandas dlt
WORKDIR /app
COPY data_inserting.py data_inserting.py
ENTRYPOINT ["python", "data_inserting.py"]