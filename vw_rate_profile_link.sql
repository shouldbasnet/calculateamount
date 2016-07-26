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