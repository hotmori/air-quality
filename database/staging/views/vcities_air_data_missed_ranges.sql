create or replace view vcities_air_data_missed_ranges
as
select min(ts_hour) min_ts_hour,
       max(ts_hour) max_ts_hour,
       city_id,
       -- to load no more than 3 months at once
       greatest (max(ts_hour) - interval '3 months',min(ts_hour)) min_inc_ts_hour
from vcities_air_data_missed
where ts_hour_loaded is null
group by city_id
;