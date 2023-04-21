create table cities_air(city_id integer not null,
						city_name varchar not null,
						country_name varchar not null,
						longitude float not null,
						latitude float not null,
						ts_hour timestamp not null,
						aqi integer not null,
						co float not null,
						no float not null,
						no2 float not null,
						o3 float not null,
						so2 float not null,
						pm2_5 float not null,
						pm10 float not null,
						nh3 float not null,
						ts_insert TIMESTAMP DEFAULT now()
						);


create unique index cities_air_pk on cities_air (city_id, date_trunc('hour', ts_hour));

create index cities_name_idx on cities_air(city_name);
create index cities_country_name_idx on cities_air(country_name);