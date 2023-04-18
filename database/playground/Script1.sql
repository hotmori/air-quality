drop view vhours_before;

create or replace view vcities as
select city_id,name,country,is_active,ts_insert
from cities c ;

select * from vcities;

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


select extract (epoch from vcadm.ts_hour),
       vcadm.ts_hour,
       vcadm.ts_hour_loaded,
       vcadm.is_missed,
       cc.longitude,
       cc.latitude 
from vcities_air_data_missed vcadm
join cities_coordinates cc on cc.city_id  = vcadm.city_id;

--1681570800.000000	2023-04-15 15:00:00.000		1	30.316229	59.938732
--1681826400.000000	2023-04-18 14:00:00.000	2023-04-18 14:00:00.000	0	30.316229	59.938732

1681570800
1681574400
1681578000
1681581600

;

with r as (
select date_trunc('month', now() at time zone 'UTC') - ('1 month')::interval as ts) 
select r.ts,
       extract (epoch from r.ts)		
from r
;

1677628800
1677632400
1677636000
1677639600
1677643200
1677646800
--
1681819200
1681822800
1681826400