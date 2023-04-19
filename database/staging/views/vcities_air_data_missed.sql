create or replace view vcities_air_data_missed
as
select vh.ts_hour,
       vc.city_id,
       vca.ts_hour ts_hour_loaded
  from vhours_since vh
  cross join vcities vc
  left join vcities_air vca
  on vca.ts_hour = vh.ts_hour
  and vca.city_id = vc.city_id
order by vc.city_id , vh.ts_hour desc ;