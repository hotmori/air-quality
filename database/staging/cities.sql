SET search_path TO staging;


create table cities(city_id integer primary key,
					name varchar not null,
                    country varchar not null,
					is_active integer default 1 not null,
					ts_insert TIMESTAMP DEFAULT now());
