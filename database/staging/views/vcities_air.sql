create or replace view vcities_air
as
with r as (
select city_id,
       ca.aqi ,
       ca.co ,
       ca.no ,
       ca.no2 ,
       ca.o3 ,
       ca.pm2_5 ,
       ca.pm10 ,
       ca.nh3 ,
       ca.ts_insert,
	   date_round(ts at time zone 'UTC', '60 minutes') at time zone 'UTC' ts_hour,
	   ts
  from staging.cities_air ca 
order by ts desc, city_id  ),
r2 as (
select r.*, 
	   case 
		   when row_number() over(partition by city_id, ts_hour order by ts desc) > 1 then 1 else 0 
	   end is_duplicate,
	   count(*) over() cnt
 from r)
select r2.city_id,
       r2.ts_hour,
       r2.ts_insert,
       r2.aqi ,
       r2.co ,
       r2.no ,
       r2.no2 ,
       r2.o3 ,
       r2.pm2_5 ,
       r2.pm10 ,
       r2.nh3    
  from r2
 where r2.is_duplicate = 0;