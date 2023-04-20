select city_name,
       timestamp_hour,
       avg(aqi) aqi,
       avg(co) co,
       avg(no) no,
       avg(no2) no2,
       avg(o3) o3,
       avg(so2) so2,
       avg(pm2_5) pm2_5,
       avg(pm10) pm10,
       avg(nh3) nh3
  from reporting.vcities_air
where {{timestamp_hour}} and {{city_name}}
group by city_name, timestamp_hour;