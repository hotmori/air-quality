create or replace view vcities as
select city_id,name,country,ts_insert
from cities c 
where is_active != 0;