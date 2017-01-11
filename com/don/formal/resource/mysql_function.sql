select unix_timestamp('1990-02-03 11:29:23')

select from_unixtime(634044563)

SELECT UTC_DATE()

select utc_time()

select NOW()

select weekday(NOW())

select EXTRACT(HOUR_MINUTE FROM '1990-02-03 11:29:23')

select get_format(date, 'usa')

select replace(cast(truncate(RAND()*10, 15) AS char(20)), '.', '')

select TIME()
