create or replace view vcities_air
as
select ca.city_id,
       ca.city_name,
       ca.country_name,
       ca.longitude,
       ca.latitude,
       ca.ts_hour timestamp_hour,
	  date_trunc('day', ca.ts_hour) timestamp_day,
	  date_trunc('month', ca.ts_hour) timestamp_month,
       ca.aqi,
       ca.co,
       ca.no,
       ca.no2,
       ca.o3,
       ca.pm2_5,
       ca.pm10,
       ca.nh3
  from transform.cities_air ca;