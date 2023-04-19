CREATE OR REPLACE FUNCTION transform.load_data()
 RETURNS int4
 LANGUAGE sql
AS $function$


delete from transform.cities_air;

  merge into transform.cities_air t2
  using staging.vcities_air t1 
  	 on t1.city_id = t2.city_id 
    and t1.ts_hour = t2.ts_hour
  when not matched then insert (city_id, ts_hour) values (t1.city_id, t1.ts_hour);
SELECT 0;
$function$
;
