create or replace view vreporting_data_delivery_metrics as
select greatest(trunc(extract( epoch from (now() at time zone 'UTC' - max(timestamp_hour)))/60),0) data_delay_in_minutes,
       min(timestamp_hour) historical_data_accessible_since,
       max(timestamp_hour) historical_data_accessible_till
from reporting.vcities_air;