create or replace view vcities_air
as
select ca.city_id,
       ca.city_name,
       ca.country_name,
       ca.longitude,
       ca.latitude,
       ca.ts_hour,
       ca.aqi,
       ca.co,
       ca.no,
       ca.no2,
       ca.o3,
       ca.so2,
       ca.pm2_5,
       ca.pm10,
       ca.nh3,
       ca.ts_insert
  from cities_air ca;