with wi (city_id, name, country, is_active)
as (select 1,  'Saint Petersburg', 'Russia', 1 union all
    select 2,  'Peterhof', 'Russia', 1  union all
	select 3, 'Lomonosov', 'Russia', 1	union all
	select 4, 'Moscow', 'Russia', 1 union all
    select 5, 'Bremerhaven', 'Germany', 1 union all
    select 6, 'Magnitogorsk', 'Russia', 1 union all
    select 7, 'Zagreb', 'Croatia', 1 union all
    select 8, 'Split', 'Croatia', 1 union all
    select 9, 'Bremen', 'Germany', 1 union all
    select 10, 'Wroclaw', 'Poland', 1 union all
    select 11, 'Warsaw', 'Poland', 1 union all
    select 12, 'Gdansk', 'Poland', 1 union all
    select 13, 'Berlin', 'Germany', 1 union all
    select 14, 'Sarapul', 'Russia', 1 union all
    select 15, 'Izhevsk', 'Russia', 1 union all
    select 100, 'Beijing', 'China', 1 union all
	select 200, 'New York City', 'United States', 0 union all
	select 201, 'San Francisco', 'United States', 0 union all
    select 300, 'Brasilia', 'Brazil', 1 union all
    select 400, 'Auckland', 'New Zealand', 1 union all
    select 500, 'Astana', 'Kazakhstan', 1 union all
    select -1, null, null, null where 1=0 -- fake ending statement
	)
merge into staging.cities as t2
using wi as t1
on (t1.city_id = t2.city_id)
when not matched then insert(city_id, name, country, is_active)
                      values (t1.city_id, t1.name, t1.country, t1.is_active)
when matched then update set is_active = t1.is_active;

commit;