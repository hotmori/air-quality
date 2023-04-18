create or replace view vhours_before
as
with r as
(select  date_round(now() , '60 minutes')  at time zone 'UTC' now_ts,
         3 days)
select r.now_ts,
       extract(hour from r.now_ts) hour_now,
       r.now_ts -  (n || ' hour')::interval ts_hour
  from r
  cross join generate_series(0, r.days*24) n;