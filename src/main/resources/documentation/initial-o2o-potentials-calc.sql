begin;

drop table if exists o2o_potentials_start;

UPDATE config_parameters set value = date_trunc('milliseconds', now()-'2 year'::interval)::text where key = 'O2O_POTENTIALS_DECREMENT_FROM';

create table o2o_potentials_start as
select * from (
    select system_id, consumer_id,
    case when action_id in (1828,1728,1814,1898,1858,1776,1866) and system_id = 1 then 1
    	 when action_id in (3069,3064,1898,1775) and system_id = 2 then 1
    	 when action_id in (1729,1740,1951,1964,1922) and system_id = 1 then 2
         else 0 end as action_type,
    count(*) as total
    from consumer_actions
    where external_system_date >= now()-'2 year'::interval and action_id in (1828,1728,1814,1898,1858,1776,1866,3069,3064,1898,1775,1729,1740,1951,1964,1922) and
        ( (id <=:lastRmcActionId and system_id = 1) or (id <=:lastRrpActionId and system_id = 2))
    group by system_id, consumer_id, action_type
    union
    select system_id, consumer_id, 3 as action_type, count(*) as total
    from consumer_actions
    where external_system_date >= now()-'1 year'::interval and action_id in (1791,1920) and
        ( (id <=:lastRmcActionId and system_id = 1) )
    group by system_id, consumer_id
) foo
where action_type > 0;

alter table o2o_potentials_start add column id serial;

commit;