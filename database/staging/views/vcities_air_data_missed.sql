create or replace view vcities_air_data_missed
as
select vhb.ts_hour,
       vc.city_id,
       vca.ts_hour ts_hour_loaded,
       case when vca.ts_hour is null then 1 else 0 end is_missed
  from vhours_before vhb
  cross join vcities vc
  left join vcities_air vca
  on vca.ts_hour = vhb.ts_hour
  and vca.city_id = vc.city_id
order by vc.city_id , vhb.ts_hour desc ;