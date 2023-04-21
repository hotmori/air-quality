create or replace view vcount_records_per_cities_staging 
as
select count(*), v.name, v.country
  from staging.vcities_air va 
  join staging.vcities v on v.city_id=va.city_id 
  group by  v.name, v.country
 order by v.country , v.name;