# air-quality-data
Serverless ETL pipeline and OLAP database for air quality data analitics based on AWS cloud 

## Intoduction
[Sensor.Community](https://sensor.community/en/) build, deploy and collect data from air quality sensors.
Data is publicly available - you can see current value on a map or download historical data from [archive](https://archive.sensor.community/) starting from 2015.
But the data in the archive is aggregated only by date - it makes any analytics difficult - because you typically want to have other aggregations like country, city, proximity to some point, etc.
The goal of this project is to build a cloud-based (for data processing parallelization) serverless (to reduce cost with uneven computing power consumption) ETL pipeline and OLAP database for 
air quality data analitics.
