create or replace view vcities as
select city_id,name,country,is_active,ts_insert
from cities c ;