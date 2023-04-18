CREATE FUNCTION date_round(base_date timestamptz, round_interval interval) RETURNS timestamptz AS $BODY$
SELECT to_timestamp((EXTRACT(epoch FROM $1)::integer + EXTRACT(epoch FROM $2)::integer / 2)
                / EXTRACT(epoch FROM $2)::integer * EXTRACT(epoch FROM $2)::integer)
$BODY$ LANGUAGE SQL STABLE;