# air-quality-data
Serverless ETL pipeline and OLAP database for air quality data analitics based on AWS cloud 

## Intoduction
[Sensor.Community](https://sensor.community/en/) build, deploy and collect data from air quality sensors.
Data is publicly available - you can see current value on a map or download historical data from [archive](https://archive.sensor.community/) starting from 2015.
But the data in the archive is aggregated only by date - it makes any analytics difficult - because you typically want to have other aggregations like country, city, proximity to some point, etc.
The goal of this project is to build a cloud-based (for data processing parallelization) serverless (to reduce cost with uneven computing power consumption) ETL pipeline and OLAP database for 
air quality data analitics.

## Data model
![data-model](docs/data-model.png)

We use star schema for the database with 2 fact tables (concentration and temperature) to store measurements from sensors and 3 dimension tables. P1 and P2 are pm2.5 and pm10 particles concentration. Sensor_type store type of sensor (like 'bme289' or 'sds011'), is_indoor indicates when the sensor is install inside a room. We get address information from sensor lat and long using [
Nominatim](https://nominatim.org/) API.
