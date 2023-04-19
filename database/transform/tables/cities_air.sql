create table cities_air(city_id integer,
						city_name varchar,
						country_name varchar,
						longitude float,
						latitude float,
						timestamp timestamp,
						aqi integer,
						co float,
						no float,
						no2 float,
						o3 float,
						pm2_5 float,
						pm10 float,
						nh3 float,
						ts_insert TIMESTAMP DEFAULT now()
						);

--create unique index city_air_pk on city_air (id, ts);


--comment on column cities_air.source_flg is 'C - current, H - history, F - forecast (tbd)';
