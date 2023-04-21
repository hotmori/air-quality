create or replace view vreporting_cities_air_data_count as
with r as (
select city_name , country_name , count(*) cnt
from reporting.vcities_air va 
group by country_name, city_name)
select  country_name ,
        city_name ,
        cnt,
        max(cnt) over() cnt_expected,
        case when cnt <> max(cnt) over() then 0 else 1 end is_valid_cnt
 from r
order by country_name, city_name; 
