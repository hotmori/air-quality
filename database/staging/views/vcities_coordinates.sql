create or replace view vcities_coordinates as
select cc.city_id ,
       cc.longitude ,
       cc.latitude ,
       cc.ts_insert 
from cities_coordinates cc ;