drop view vcities_air_data_missed_ranges;
create or replace view vcities_air_data_missed_ranges
as
select min(ts_hour) min_ts_hour,
       max(ts_hour) max_ts_hour,
       city_id
from vcities_air_data_missed
where ts_hour_loaded is null
group by city_id
;

select min_ts_hour, max_ts_hour, city_id
from vcities_air_data_missed_ranges
order by city_id;



create or replace view vcities_coordinates as
select cc.city_id ,
       cc.longitude ,
       cc.latitude ,
       cc.ts_insert 
from cities_coordinates cc ;

create table t (a integer);
insert into t values (1);

select * from t;
rollback;

 :AUTOCOMMIT;

delete from staging.cities where city_id  =5;

select count(*), source_flg from cities_air ca group by source_flg ;
