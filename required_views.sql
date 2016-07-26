/*-- this view will check all contracts of every operator and list the most recent one-----*/
create or replace view vw_contracts_ranking as
select  a.id, a.operator_id, a.service_id, a.start_date, a.end_date,
        count(b.end_date)+1 as rank
from  
    contracts a 
left join 
    contracts b 
on 
    a.end_date<b.end_date 
and 
    a.operator_id=b.operator_id 
and 
    a.service_id=b.service_id 
group by 
    a.id, a.operator_id, a.service_id, a.start_date, a.end_date;
/*-------------------------------------------*/
create or replace view vw_contract_recent
as select * from vw_contracts_ranking where rank=1;
/*-------------------------------------------*/
/*-----this view will just link up the below two tables--------*/
create or replace view ntc.vw_rate_profile_link as
select a.operator_id, a.service_id, a.contract_id, v.tier_detail_id, a.is_vbr, v.rate_profile_detail_id, rp.rate_type  
from
    ntc.rate_profile_details a 
left join 
    ntc.vbr_combinations v 
on 
    v.rate_profile_detail_id = a.id
left join
    (select distinct rate_profile_detail_id, rate_type from ntc.rate_profiles) rp
on  
    v.rate_profile_detail_id=rp.rate_profile_detail_id
;
/*-------------------------------------------------------------------*/
/*-------this is the final view that will link all the info ---------*/
#-------------------------------------------------------------------#
create or replace view vw_call_volume_link as
select distinct  
	concat(case when vrp.is_vbr='yes' then 'vbr' else v.id end,'-',vrp.rate_profile_detail_id,'-',c.id,'-',o.id,'-',s.id) as uniqueid,
    v.id as callvolumeid,
	v.call_duration as duration,
    c.id as contractid,
    o.id as operatorid,
    t.id as tierid,
    s.id as serviceid, 
    mx.id as recentcontractid,
	vbr.id as vbrid,
	vbr.product_id as product_id,
	ifnull(vrp.is_vbr,'no') as is_vbr,
	vrp.rate_profile_detail_id,
	vrp.rate_type 
from 
    ntc.data_call_volumes v
left join
    ntc.operators o
on
    v.operator_code=o.code
left join
    ntc.tier_details t
on
    v.tier=t.tier_code
left join
    ntc.services s
on
    v.call_type= s.code
and 
    v.component_direction = upper(substr(s.slug,1,1))
left join
    ntc.contracts c
on 
    v.start_date >= c.start_date 
and 
    v.end_date<= c.end_date
and 
    o.id= c.operator_id
and
    s.id=c.service_id
left join	
	vbr_combinations vbr
on	
	t.id= vbr.tier_detail_id
left join
    vw_contract_recent mx
on
    mx.operator_id=o.id
and
    mx.service_id=s.id 
left join 
	vw_rate_profile_link vrp
on	
	vrp.operator_id=o.id
and 
	vrp.service_id=s.id
and	
	vrp.contract_id= coalesce(c.id, mx.id)
and
	vrp.tier_detail_id=t.id
where 
    v.calculate_flag
	;
/*-------------------------------------------------------------------*/