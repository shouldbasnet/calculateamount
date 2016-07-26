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
#-------------------------------------------#
create or replace view vw_contract_recent
as select * from vw_contracts_ranking where rank=1;
#-------------------------------------------#