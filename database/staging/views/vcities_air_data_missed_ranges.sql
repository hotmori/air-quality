create or replace view vcities_air_data_missed_ranges
as
select min(ts_hour) min_ts_hour,
       max(ts_hour) max_ts_hour,
       city_id
from vcities_air_data_missed
where ts_hour_loaded is null
group by city_id
;