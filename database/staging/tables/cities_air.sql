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

create index cities_air_idx on cities_air (city_id);


comment on column cities_air.source_flg is 'C - current, H - history, F - forecast (tbd)';
