SET search_path TO staging;


create table cities(city_id integer primary key,
					name varchar,
					longitude float,
				    latitude float,
					ts_insert TIMESTAMP DEFAULT now());