create or replace view vcities_air_data_missed_ranges
as
select max(ts_hour), min(ts_hour) , city_id
from vcities_air_data_missed
where ts_hour_loaded is null
group by city_id
;