create or replace view vstaging_cities_air_filled_per_hour_data 
as
select count(*) cnt, v.country, v.name 
  from staging.vcities_air va 
  join staging.vcities v on v.city_id=va.city_id 
  group by  v.country, v.name
 order by v.country , v.name;