SET search_path TO staging;


create table cities_air(city_id integer not null,
						ts timestamp not null,
						aqi integer not null,
						co float not null,
						no float not null,
						no2 float not null,
						o3 float not null,
						so2 float not null,
						pm2_5 float not null,
						pm10 float not null,
						nh3 float not null,
						ts_insert TIMESTAMP DEFAULT now() not null,
						source_flg char default 'C' not null
						);

create index cities_air_idx on cities_air (city_id);


comment on column cities_air.source_flg is 'C - current, H - history, F - forecast (tbd)';
