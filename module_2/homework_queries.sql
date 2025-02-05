-- 1 answer in kestra output of extract task
-- 2 answer in kestra output during execution file
-- 3
SELECT COUNT(*) FROM yellow_tripdata
WHERE substring('filename' from 17 for 4) == '2020'
-- 4
SELECT COUNT(*) FROM green_tripdata
WHERE substring('filename' from 16 for 4) == '2020'
-- 5
SELECT COUNT(*) FROM yellow_tripdata
WHERE substring('filename' from 17 for 7) == '2020-03'
-- 6 answer in kestra docs