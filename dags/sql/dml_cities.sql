with wi (city_id, name, country, is_active)
as (select 1,  'Saint Petersburg', 'Russia', 1
       union all
       select 2,  'Peterhof', 'Russia', 1 )
merge into staging.cities as t2
using wi as t1
on (t1.city_id = t2.city_id)
when not matched then insert(city_id, name, country, is_active)
                      values (t1.city_id, t1.name, t1.country, t1.is_active)
when matched then update set is_active = t1.is_active;

commit;