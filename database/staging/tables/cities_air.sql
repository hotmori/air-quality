SET search_path TO staging;


create table cities_air(city_id integer, 
						ts timestamp,
						aqi integer,
						co float,
						no float,
						no2 float,
						o3 float,
						pm2_5 float,
						pm10 float,
						nh3 float,
						ts_insert TIMESTAMP DEFAULT now(),
						source_flg char default 'C'
						);

create unique index city_air_pk on city_air (id, ts);


comment on column cities_air.source_flg is 'C - current, H - history, F - forecast (tbd)';
--"co":1735.69,"no":148.42,"no2":122.01,"o3":17.17,"so2":350.95,"pm2_5":43.04,"pm10":52.46,"nh3":0.14					  