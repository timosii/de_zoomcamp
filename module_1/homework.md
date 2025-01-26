- 1: 
`docker run -it python:3.12.8 bash`
`pip --version`
- 2: answer is db:5432 because pgadmin talk with postgres in internal network and use service_name like host
- 3 : 104,802; 198,924; 109,603; 27,678; 35,189
example for trip_distance <= 1:
```sql
SELECT
	COUNT(*)
FROM green_taxi_data 
WHERE
	CAST(lpep_pickup_datetime AS DATE) >= '2019-10-01'::date and
	CAST(lpep_dropoff_datetime AS DATE) < '2019-11-01'::date and
	trip_distance <= 1
```
- 4 : "2019-10-11"	95.78
```sql
SELECT
	CAST(lpep_pickup_datetime AS DATE) as pickup_date,
	MAX(trip_distance) AS maximum
FROM green_taxi_data 
WHERE
	CAST(lpep_pickup_datetime AS DATE) >= '2019-10-01'::date and
	CAST(lpep_dropoff_datetime AS DATE) < '2019-11-01'::date
GROUP BY
	CAST(lpep_pickup_datetime AS DATE)
ORDER BY
	maximum DESC
```
- 5 : - East Harlem North, East Harlem South, Morningside Heights
```sql
SELECT
	g."PULocationID",
	z."Zone",
	SUM(g.total_amount) as sum_total
FROM green_taxi_data g JOIN zones z
	ON g."PULocationID" = z."LocationID"
WHERE
	CAST(g.lpep_pickup_datetime AS DATE) = '2019-10-18'::date
GROUP BY
	g."PULocationID",
	z."Zone"
HAVING
	SUM(g.total_amount) > 13000
```
- 6: - JFK Airport: 87.3
```sql
SELECT
	z_out."Zone" as "do_zone",
	MAX(g.tip_amount) as max_tip
FROM green_taxi_data g LEFT JOIN zones z_in
	ON g."PULocationID" = z_in."LocationID" LEFT JOIN zones z_out
	ON g."DOLocationID" = z_out."LocationID"
WHERE
	CAST(g.lpep_pickup_datetime AS DATE) >= '2019-10-01'::date and
	CAST(g.lpep_pickup_datetime AS DATE) < '2019-11-01'::date and
	z_in."Zone" = 'East Harlem North'
GROUP BY
	z_out."Zone"
ORDER BY
	max_tip DESC
```
- 7
```bash
terraform init, terraform apply -auto-approve, terraform destroy
```