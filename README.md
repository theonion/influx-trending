# influx-trending

> stuff that influx should be able to do, but doesn't, so now i have to do it


## percentile.py

just a really basic threshold-based means of getting trending content. this works if you have a continuous query 
running in influx that calculates percentiles.

for example, i have two CQs going -- one rolls up data daily, and the other then computes the 99th percentile 
for that day by total number of clicks

```sql
select content_id,sum(clicks) as clicks from /^[a-zA-Z0-9_\-]+$/ group by content_id,time(1d) into :series_name.rollup.1d
```

```sql
select percentile(clicks,99) as threshold from /^.*\.rollup.1d$/ group by time(1d) into :series_name.percentile.99
```

this script then pulls out the percentile values, gets stuff from the daily content rollup, makes the comparison and 
then dumps it into another series.

### arguments

if you're going to run this as a one off script, you'll need to supply the following arguments inline:

| name              | type   | description                        |
| ----------------- | ------ | ---------------------------------- |
| host              | string | name of the influxdb host machine  |
| port              | integer| port number influxdb listens on    |
| username          | string | read/write user                    |
| password          | string | read/write user                    |
| db                | string | name of the database to connect    |
| percentile_series | string | name of the series with thresholds |
| content_series    | string | name of the series with content    |
| trending_series   | string | name of the series to write trends |
| offset            | string | influxdb formatted time offset     |

so for example, locally you would do:

```bash
$ python3 percentile.py localhost 8086 root root my_db site.1d.pcnt site.1d site.1d.trend 1d
```
