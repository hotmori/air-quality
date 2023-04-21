select city_name,
       timestamp_day,
       avg(aqi) aqi,
       PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY aqi) median_aqi,
       avg(co) co,
       avg(no) no,
       avg(no2) no2,
       avg(o3) o3,
       avg(so2) so2,
       avg(pm2_5) pm2_5,
       avg(pm10) pm10,
       avg(nh3) nh3
  from reporting.vcities_air
where {{timestamp_day}} and {{city_name}}
group by city_name, timestamp_day;