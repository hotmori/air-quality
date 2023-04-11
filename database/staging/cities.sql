create schema staging;

SET search_path TO staging;

create table cities(city_id integer primary key,
					name varchar,
					longitude float,
				    latitude float,
					ts_insert TIMESTAMP DEFAULT now());

insert into cities values(1, 'Saint Petersburg', 30.3351, 59.9343);
insert into cities values(2, 'Peterhof', 29.8852, 59.8845);


create table city_air(id integer, 
					  ts timestamp,
					  aqi integer,
					  co float,
					  no float,
					  no2 float,
					  o3 float,
					  pm2_5 float,
					  pm10 float,
					  nh3 float,
					  longitude float,
				      latitude float,
					  ts_insert TIMESTAMP DEFAULT now()
					  );

create unique index city_air_pk on city_air (id, ts);

//"co":1735.69,"no":148.42,"no2":122.01,"o3":17.17,"so2":350.95,"pm2_5":43.04,"pm10":52.46,"nh3":0.14					  