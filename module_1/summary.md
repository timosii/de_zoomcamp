simple Dockerfile:
```
FROM python:3.12
RUN pip install pandas
WORKDIR /app
COPY pipeline.py pipeline.py
ENTRYPOINT ["python", "pipeline.py"]
```

pipeline.py:
```python
import sys
import pandas as pd

print(sys.argv)
day = sys.argv[1]
# do something
print(f'job finished successfully for day = {day}')
```

adding ny_taxi db locally
```
# basic configuration
docker run -it \
	-e POSTGRES_USER="root" \
	-e POSTGRES_PASSWORD=""root \
	-e POSTGRES_DB="ny_taxi" \
	-v $(pwd)/ny_taxi_postgres_data:/var/lib/postgresql/data \
	-p 5432:5432 \
	postgres:13

# in folder ny_taxi_postgres_data now created postgres directories

pgcli (another terminal window)
pgcli -h localhost -p 5432 -u root -d ny_taxi
```
- if something problem with psycopg2 when starting engine 
```
sudo apt install libpq-dev gcc
```
- at repo you can see code step by step for adding data to database. I used iterator for inserting data by chunks
- finally we can see in pgcli by `SELECT COUNT(*) from yellow_taxi_data;` that inserting complete
- at the end of inserting by while loop we get StopIteration Error, that mean iterator exhausted himself (it's normal)

- Run pgadmin
```
docker run -it \
	-e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
	-e PGADMIN_DEFAULT_PASSWORD='root' \
	-p 8080:80 \
	dpage/pgadmin4
```
- For container with pgadmin can see container with postgres we need to put them in one network 
```
docker network create pg-network
```
- And then we need to define network and name in our containers
```
# for pgadmin
--network=pg-network \
--name pgadmin

docker run -it \
	-e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
	-e PGADMIN_DEFAULT_PASSWORD='root' \
	-p 8080:80 \
	--network=pg-network \
	--name pgadmin \
	dpage/pgadmin4

# and for postgres
--network=pg-network \
--name pg-database

docker run -it \
	-e POSTGRES_USER="root" \
	-e POSTGRES_PASSWORD="root" \
	-e POSTGRES_DB="ny_taxi" \
	-v ~/data/ny_taxi_data/ny_taxi_postgres_data:/var/lib/postgresql/data \
	-p 5432:5432 \
	--network=pg-network \
	--name pg-database \
	postgres:13
```

- argparse - module for parse cli arguments
```python
parser = argparse.ArgumentParser(
	prog='ProgramName',
    description='Ingest CSV data to Postgres',
    epilog='Text at the bottom of help')
parser.add_argument('--user', help='user name for postgres')
parser.add_argument('--password', help='password for postgres')
parser.add_argument('--host', help='host for postgres')
parser.add_argument('--port', help='port for postgres')
parser.add_argument('--db', help='database name for postgres')
parser.add_argument('--table-name', help='name of the table where we will write the results to')
parser.add_argument('--url', help='url of the csv file')
args = parser.parse_args()
print(args.accumulate(args.integers))
```

```bash
URL="https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet" # define variable
python data_inserting.py \
    --user=root \
    --password=root \
    --host=localhost  \
    --port=5432 \
    --db=ny_taxi \
    --table_name=yellow_taxi_trips \
    --url=${URL}
# actually we need to organize environment
```
- run with docker
```
# now we can run it with docker. we need use same script but modify Dockerfile to install dependencies

RUN apt-get install wget
RUN pip install pandas sqlalchemy psycopg2 pyarrow

and then we can run scripts:

docker build -t taxi_ingest:v001
# -it is interactive terminal, we can't execute container without it. we use the same network what we use with pd_admin

docker run -it --network=pg-network taxi_ingest:v001 # image name
	--user=root \
    --password=root \
    --host=pg-database  \
    --port=5432 \
    --db=ny_taxi \
    --table_name=yellow_taxi_trips \
    --url=${URL}
```
- simple docker_compose file. we don't need to specify network (there is default common network for docker compose images)
```
services:
  pgdatabase:
    image: postgres:13
    environment:
      - POSTGRES_USER=root
      - POSTGRES_PASSWORD=root
      - POSTGRES_DB=ny_taxi

    volumes:
      - "/home/timosii/data/ny_taxi_data/ny_taxi_postgres_data:/var/lib/postgresql/data: rw"
    ports:
      - "5432:5432"

  pgadmin:
    image: dpage/pgadmin4
    environment:
      - PGADMIN_DEFAULT_EMAIL="admin@admin.com"
      - PGADMIN_DEFAULT_PASSWORD='root'
    ports:
      - "8080:80"
```
- ZONES and joins
```
!wget https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv

df_zones = pd.read_csv('taxi_zone_lookup.csv')
```
- inner join
```sql
SELECT
	t.tpep_pickup_datetime,
	t.tpep_dropoff_datetime,
	t.total_amount,
	CONCAT(zpu."Borough",' / ',zpu."Zone") AS "pickup_loc",
	CONCAT(zdo."Borough",' / ',zdo."Zone") AS "dropoff_loc"
FROM
	yellow_taxi_trips t,
	zones zpu,
	zones zdo
WHERE 
	t."PULocationID" = zpu."LocationID" AND
	t."DOLocationID" = zdo."LocationID"
```
- there is second way of inner join (there are equivalent)
```sql
SELECT
	t.tpep_pickup_datetime,
	t.tpep_dropoff_datetime,
	t.total_amount,
	CONCAT(zpu."Borough",' / ',zpu."Zone") AS "pickup_loc",
	CONCAT(zdo."Borough",' / ',zdo."Zone") AS "dropoff_loc"
FROM
	yellow_taxi_trips t JOIN zones zpu
		ON t."PULocationID" = zpu."LocationID"
	JOIN zones zdo
		ON t."DOLocationID" = zdo."LocationID"
```
- check if there is NULL pull off location o drop out location
```sql
SELECT
	tpep_pickup_datetime,
	tpep_dropoff_datetime,
	total_amount,
	"PULocationID",
	"DOLocationID"
FROM
	yellow_taxi_trips
WHERE
	("PULocationID" is NULL) OR
	("DOLocationID" is NULL)
```
- check that all of location ids is present in zones (we have describe infromation about all ID)
```sql
-- there is query is recommend to use if we are sure that zones don't have null values
SELECT
	tpep_pickup_datetime,
	tpep_dropoff_datetime,
	total_amount,
	"PULocationID",
	"DOLocationID"
FROM
	yellow_taxi_trips
WHERE
	"DOLocationID" NOT IN (
		SELECT "LocationID" FROM zones
	)

-- we can use this query for check that zones ensures robust handling of `NULL` values in `zones.LocationID`
SELECT tpep_pickup_datetime, tpep_dropoff_datetime, total_amount, "PULocationID", "DOLocationID" FROM yellow_taxi_trips t
WHERE
	NOT EXISTS (
		SELECT 1
		FROM zones z
		WHERE z."LocationID" = t."PULocationID" ) OR
		NOT EXISTS (
			SELECT 1 FROM zones z WHERE z."LocationID" = t."DOLocationID" );
```
- date converting
```sql
DATE_TRUNC('DAY', t.tpep_dropoff_datetime), # return datetime with 0 hours min and sec
CAST(tpep_dropoff_datetime AS DATE), # return a date
```
- groupby queries
```sql
SELECT
	CAST(tpep_dropoff_datetime AS DATE) as "day",
	t."DOLocationID",
	COUNT(1) as "count",
	MAX(total_amount) as "max_amount",
	MAX(passenger_count) as "max_passengers"
FROM
	yellow_taxi_trips t
GROUP BY
	"day",
	"DOLocationID"
ORDER BY
	"day" ASC,
	"DOLocationID" ASC
```