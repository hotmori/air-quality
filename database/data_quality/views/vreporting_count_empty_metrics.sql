create or replace view vreporting_count_empty_metrics
as
select count(case when va.aqi is null then 1 end) cnt_aqi,
        count(case when va.co is null then 1 end) cnt_co,
		count(case when va.no is null then 1 end) cnt_no,
		count(case when va.no2 is null then 1 end) cnt_no2,
		count(case when va.o3 is null then 1 end) cnt_o3,
		count(case when va.so2 is null then 1 end) cnt_so2,
		count(case when va.pm2_5 is null then 1 end) cnt_pm2_5,
		count(case when va.pm10 is null then 1 end) cnt_pm10,
		count(case when va.nh3  is null then 1 end) cnt_nh3
   from reporting.vcities_air va  ;