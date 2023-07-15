CREATE OR REPLACE FUNCTION public.arrival_depart_los()
 RETURNS integer
 LANGUAGE plpgsql
AS $function$
begin

with timebuckets1 as(
select 
		date(b.dates::timestamp) as dates, 
		date_part('hour', b.dates) as hours,
		count(date_part('hour', ab.ed_arrival_datetime)) as arr_cnt
		--count(date_part('hour', ab.ed_departure_datetime)) as dep_cnt
	from "public".data_intake as ab 
	full join "public".t_bucket as b
	on date(b.dates) = date(ab.ed_arrival_datetime)
	and date_part('hour', b.dates) = date_part('hour', ab.ed_arrival_datetime)
	group by 1,2
	order by 1,2
),

 depart as (
 
 select 
		date(b.dates::timestamp) as dates, 
		date_part('hour', b.dates) as hours,
		count(date_part('hour', ab.ed_departure_datetime)) as dep_cnt

	from "public".data_intake as ab 
	full join "public".t_bucket as b
	on date(b.dates) = date(ab.ed_departure_datetime)
	and date_part('hour', b.dates) = date_part('hour', ab.ed_departure_datetime)
	group by 1,2
	order by 1,2
	
 
 ),
 alos as (
	select 
	--date(di.ed_arrival_datetime) as a_date,
	--date_part('hour', di.ed_arrival_datetime) as hours,
	date(di.ed_departure_datetime) as a_date,
	date_part('hour', di.ed_departure_datetime) as hours,
	avg(di.ed_length_of_stay_in_hours) as losinhours
	
	from public.data_intake di
	where di.encounter_type in ('OP in a Bed', 'Observation', 'Inpatient')
	group by 1, 2
	order by 1, 2
),

 dlos as (
	select 
		date(di.ed_departure_datetime) as d_date,
	--date(di.ed_arrival_datetime) as d_date,
	--date_part('hour', di.ed_arrival_datetime) as hours,
	date_part('hour', di.ed_departure_datetime) as hours,
	avg(di.ed_length_of_stay_in_hours) as losinhours
	from public.data_intake di
	where di.encounter_type in ('Emergency')
	group by 1, 2
	order by 1, 2
)

insert into "public".arrival_depart_los ("date_of_service", "hour_of_day", "arrival_count", "depart_count", "avg_admit_los", "avg_discharge_los" )
select 
	date(timebuckets1.dates) as "date_of_service", 
	timebuckets1.hours as "hour_of_day",
	sum(timebuckets1.arr_cnt) as "arrival_count",
	sum(d.dep_cnt) as "depart_count",
	(z.losinhours) as "avg_admit_los",
	(x.losinhours) as "avg_discharge_los"
from timebuckets1
	left outer join alos as z
	on timebuckets1.dates = z.a_date
		and timebuckets1.hours = z.hours
	left outer join dlos as x 
	on timebuckets1.dates = x.d_date
		and timebuckets1.hours = x.hours
	left join depart as d 
	on timebuckets1.hours = d.hours
		and timebuckets1.dates = d.dates
	group by 1,2,5,6
	order by 1,2;


return 1; 
end
$function$
;
