CREATE OR REPLACE FUNCTION transform.load_data()
 RETURNS integer
 LANGUAGE plpgsql
AS $function$
declare
  l_deleted_rows integer;
  l_row_count integer;
begin

  GET DIAGNOSTICS l_deleted_rows = ROW_COUNT;
  delete
  from transform.cities_air ca
  where not exists (select null
					  from staging.vcities_air va
					 where va.city_id = ca.city_id
                       and va.ts_hour = ca.ts_hour);

  merge into transform.cities_air t2
  using (select va.city_id,
                vc.name city_name,
                vc.country country_name,
                vcc.longitude,
                vcc.latitude,
                va.ts_hour,
                va.aqi,
                va.co,
                va.no,
                va.no2,
                va.o3,
                va.pm2_5,
                va.pm10,
                va.nh3
           from staging.vcities_air va
           join staging.vcities vc on vc.city_id = va.city_id
           join staging.vcities_coordinates vcc on vcc.city_id = vc.city_id
           ) as t1
  	 on t1.city_id = t2.city_id
    and t1.ts_hour = t2.ts_hour
  when matched then update set city_name = t1.city_name,
                               country_name = t1.country_name,
                               longitude = t1.longitude,
                               latitude = t1.latitude,
                               aqi = t1.aqi,
                               co = t1.co,
                               no = t1.no,
                               no2 = t1.no2,
                               o3 = t1.o3,
                               pm2_5 = t1.pm2_5,
                               pm10 = t1.pm10,
                               nh3 = t1.nh3
  when not matched then insert (city_id,
                                city_name,
                                country_name,
                                longitude,
                                latitude,
                                ts_hour,
                                aqi,
                                co,
                                no,
                                no2,
                                o3,
                                pm2_5,
                                pm10,
                                nh3)
						values (t1.city_id,
                                t1.city_name,
                                t1.country_name,
                                t1.longitude,
                                t1.latitude,
                                t1.ts_hour,
                                t1.aqi,
                                t1.co,
                                t1.no,
                                t1.no2,
                                t1.o3,
                                t1.pm2_5,
                                t1.pm10,
                                t1.nh3);

  --select 1;
   select count(*) into l_row_count from transform.cities_air;
   return l_row_count;
end;

$function$
;