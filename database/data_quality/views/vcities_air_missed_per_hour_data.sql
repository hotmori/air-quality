create or replace view vcities_air_filled_per_hour_data 
as
select count(*), v.country, v.name 
  from staging.vcities_air va 
  join staging.vcities v on v.city_id=va.city_id 
  group by  v.country, v.name
 order by v.country , v.name;