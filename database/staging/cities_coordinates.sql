SET search_path TO staging;


create table cities_coordinates(city_id integer primary key,
						longitude float,
						latitude float,
						ts_insert TIMESTAMP DEFAULT now());