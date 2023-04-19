create or replace view vhours_since
as
with r as (
select '2023-04-01 00:00:00'::timestamp ts_hour_start ,
        date_round(now() , '60 minutes')  at time zone 'UTC' ts_hour_end),
r2 as (
select r.ts_hour_start,
       r.ts_hour_end,
       extract (epoch from  r.ts_hour_end - r.ts_hour_start)/3600 hours_since_start_date
  from r),
r3 as (
select r2.hours_since_start_date,
       r2.ts_hour_end now_ts,
       r2.ts_hour_end -  (n || ' hour')::interval ts_hour
  from r2
  cross join generate_series(0, r2.hours_since_start_date) n)
select r3.now_ts,
       r3.ts_hour 
  from r3;