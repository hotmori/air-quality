create table cities_air(city_id integer,
						city_name varchar,
						country_name varchar,
						longitude float,
						latitude float,
						ts_hour timestamp,
						aqi integer,
						co float,
						no float,
						no2 float,
						o3 float,
						so2 float,
						pm2_5 float,
						pm10 float,
						nh3 float,
						ts_insert TIMESTAMP DEFAULT now()
						);


create unique index cities_air_pk on cities_air (city_id, date_trunc('hour', ts_hour));

create index cities_country_name_idx on cities_air(country_name);