create or replace
function public.get_schedule(search_date text,
rec text) returns table(query json) language plpgsql as 
$function$ 
begin return query with initialQuery as (
select
	left(s."Shift1", 2) as "type" ,
	s."ID" ,
	initcap(substring(left(lower(s."Shift"), 12), '((mon|tues|wed(nes)?|thur(s)?|fri|sat(ur)?|sun)(day)?)')) as "dayOfWeek" ,
	to_char(concat(date(s."Date"), ' ', regexp_replace(regexp_replace(regexp_replace(right(s."Shift1", 4), '_', ''), 'PM', ':00 PM'), 'AM', ':00 AM'))::timestamp, 'HH24:MI:SS') as "startTime" ,
	trim(substring(regexp_replace(replace(right(s."Shift1", 9), '_', ''), 'HR', ' '), 0, 3)) as duration ,
	s."Shift1",
	s."Shift" ,
	regexp_replace(s."Recommendation", 'Rec', '') as "recommendation" ,
	case
		when split_part(regexp_replace(s."Action", '_Shift', ''), '_', 1) = 'Drop' then 'remove'
		when split_part(regexp_replace(s."Action", '_Shift', ''), '_', 1) = 'Add' then 'add'
		when split_part(regexp_replace(s."Action", '_Shift', ''), '_', 1) = 'Cancel' then 'cancel'
		else '' end as "types" ,
		case
			when split_part(regexp_replace(s."Action", '_Shift', ''), '_', 1) = 'Drop' then 'false'
			when s."Action" = 'Add_Shift' then 'true'
			when split_part(regexp_replace(s."Action", '_Shift', ''), '_', 1) = 'Cancel' then 'false'
			else 'false' end as "addRow" ,
			case
				when split_part(regexp_replace(s."Action", '_Shift', ''), '_', 1) = 'Drop' then 'false'
				when split_part(regexp_replace(s."Action", '_Shift', ''), '_', 1) = 'Add' then 'false'
				when split_part(regexp_replace(s."Action", '_Shift', ''), '_', 1) = 'Cancel' then 'true'
				else 'false' end as "removeRow" ,
				case
					when split_part(regexp_replace(s."Action", '_Shift', ''), '_', 3) = 'AtBeginning' then 'left'
					when split_part(regexp_replace(s."Action", '_Shift', ''), '_', 3) = 'AtEnd' then 'right'
					else '' end as "side" ,
					concat(date(s."Date"), ' ', regexp_replace(regexp_replace(regexp_replace(right(s."Shift1", 4), '_', ''), 'PM', ':00 PM'), 'AM', ':00 AM'))::timestamp ,
					regexp_replace(split_part(regexp_replace(s."Action", '_Shift', ''), '_', 2), 'HR', '') as "durations" ,
					date(s."Date") as "date" ,
					concat(date(s."Date"), ' ', regexp_replace(regexp_replace(regexp_replace(right(s."Shift1", 4), '_', ''), 'PM', ':00 PM'), 'AM', ':00 AM'))::timestamp as "start_dt_tm"
				from
					"output"."Shift Flexing Decision Table" as s
				where
					lower(s."Recommendation") = lower(rec)
					and concat(date(s."Date"), ' ', regexp_replace(regexp_replace(regexp_replace(right(s."Shift1", 4), '_', ''), 'PM', ':00 PM'), 'AM', ':00 AM'))::timestamp between concat(search_date, ' 0700')::timestamp and concat((search_date), ' 0659')::timestamp + interval '1 DAY'
				order by
					"dayOfWeek",
					"start_dt_tm" ) ,
w2 as (
--remove null vals
select
	w."ID",
	json_strip_nulls(json_agg(w."adjs"::json)) as "justments"
from
	w
where
	w."adjs"::text != '[null]'
group by
	1 ) ,
obj as (
--create flex and add in adjustments
select
	w."date",
	w."ID",
	case
		when w."addRow" = 'false'
		and w."removeRow" = 'false'
		and w."types" = '' then json_build_object('type', w."type", 'dayOfWeek', trim(w."dayOfWeek"), 'startTime', w."startTime", 'duration', w."duration"::integer, 'flex', '{}'::json)::text
		--remove row, do not include adjustments array
		when w."addRow" = 'false'
		and w."removeRow" = 'true'
		and w."types" = 'cancel'
		and w."side" = '' then json_build_object('type', w."type", 'dayOfWeek', w."dayOfWeek", 'startTime', w."startTime", 'duration', w."duration"::integer, 'flex', (json_build_object('addRow', w."addRow", 'removeRow', w."removeRow", 'adjustments', '{}'::text[] )))::text
		--add row, do not include adjustments array
		when w."addRow" = 'true'
		and w."removeRow" = 'false' then json_build_object('type', w."type", 'dayOfWeek', trim(w."dayOfWeek"), 'startTime', w."startTime", 'duration', w."duration"::integer, 'flex', (json_build_object('addRow', w."addRow", 'removeRow', w."removeRow", 'adjustments', '{}'::text[] )))::text
		else json_strip_nulls(json_build_object('type', w."type", 'dayOfWeek', trim(w."dayOfWeek"), 'startTime', w."startTime", 'duration', w."duration"::integer, 'flex', (json_build_object('addRow', w."addRow", 'removeRow', w."removeRow", 'adjustments', w2."justments"))))::text end as jsonobj
	from
		w
	full join w2 on
		w2."ID" = w."ID"
	order by
		w."start_dt_tm") ,
flexing as (
select
	date("Date") as "date",
	trim(substring(regexp_replace(replace(right("Shift", 9), '_', ''), 'HR', ' '), 0, 3)) as "hours",
	"ScheduleditemsPerHourInput",
	"FlexeditemsPerHourDecisions-Rec1",
	"FlexeditemsPerHourDecisions-Rec2",
	"FlexeditemsPerHourDecisions-Rec3",
	"ScheduleditemsPerHourWithFlexedDecisions-Rec1",
	"ScheduleditemsPerHourWithFlexedDecisions-Rec2",
	"ScheduleditemsPerHourWithFlexedDecisions-Rec3",
	"itemsNeeded-Rec1",
	"itemsNeeded-Rec2",
	"itemsNeeded-Rec3"
from
	"output"."Hourly Supply Matching"
where
	date("Date") = date(search_date)
	and concat(date(search_date), ' ', trim(substring(regexp_replace(replace(right("Shift", 9), '_', ''), 'HR', ' '), 0, 3)), '00')::timestamp between concat(date(search_date), ' 0700')::timestamp and concat(date(search_date), ' 2359')::timestamp
union
select
	date("Date") as "date",
	trim(substring(regexp_replace(replace(right("Shift", 9), '_', ''), 'HR', ' '), 0, 3)) as "hours",
	"ScheduleditemsPerHourInput",
	"FlexeditemsPerHourDecisions-Rec1",
	"FlexeditemsPerHourDecisions-Rec2",
	"FlexeditemsPerHourDecisions-Rec3",
	"ScheduleditemsPerHourWithFlexedDecisions-Rec1",
	"ScheduleditemsPerHourWithFlexedDecisions-Rec2",
	"ScheduleditemsPerHourWithFlexedDecisions-Rec3",
	"itemsNeeded-Rec1",
	"itemsNeeded-Rec2",
	"itemsNeeded-Rec3"
from
	"output"."Hourly Supply/Demand Matching" as j
where
	date(j."Date") = search_date::date + interval '1 DAY'
	and date(j."Date") between concat(search_date, ' 0000')::timestamp + interval '1 DAY' and concat(search_date, ' 0659')::timestamp + interval '1 DAY'
	and j."Hour"::integer < 7
order by
	1,
	2 ) ,
sumit as (
--sum it up sub query 
select
	flexing."date" as "date" ,
	flexing."hours" ,
	flexing."ScheduleditemsPerHourInput"::integer as "original" ,
	flexing."ScheduleditemsPerHourWithFlexedDecisions-Rec1"::integer as "Rec1" ,
	flexing."ScheduleditemsPerHourWithFlexedDecisions-Rec2"::integer as "Rec2" ,
	flexing."ScheduleditemsPerHourWithFlexedDecisions-Rec3"::integer as "Rec3" ,
	flexing."FlexeditemsPerHourDecisions-Rec1"::integer as "rec1delta" ,
	flexing."FlexeditemsPerHourDecisions-Rec2"::integer as "rec2delta" ,
	flexing."FlexeditemsPerHourDecisions-Rec3"::integer as "rec3delta"
from
	flexing
order by
	1,
	2 ) ,
jsonprep as (
select
	json_build_object('original', recQuery."orig", 'rec1', recQuery."r1", 'rec2', recQuery."r2", 'rec3', recQuery."r3", 'rec1delta', recQuery."r1d", 'rec2delta', recQuery."r2d", 'rec3delta', recQuery."r3d" ) as "aggrecQuery"
from
	(
	select
		json_agg("original") as "orig" ,
		json_agg("Rec1") as "r1" ,
		json_agg("Rec2") as "r2" ,
		json_agg("Rec3") as "r3" ,
		json_agg("rec1delta") as "r1d" ,
		json_agg("rec2delta") as "r2d" ,
		json_agg("rec3delta") as "r3d"
	from
		sumit ) as recQuery ) ,
finalOutput as (
select
	json_agg(obj."jsonobj"::json) as "sched"
from
	obj )
select
	json_build_object('schedule', mm."sched", 'summary', j."aggrecQuery")
from
	finalOutput as mm
full join jsonprep as j on
	1 = 1;

end $function$ ;
