SET search_path TO staging;


create table cities_coordinates(city_id integer primary key,
						longitude float not null,
						latitude float not null,
						ts_insert TIMESTAMP DEFAULT now());