use ntc;
drop procedure if exists calculate_amount_t;
delimiter $$
create procedure calculate_amount_t()

begin
	declare v_callduration integer(19);
	declare v_tier_code varchar(200);
	declare v_uniqueid varchar(200);
	declare v_operator_code varchar(200);
	declare v_service_code varchar(200);
	declare v_product_id varchar(200);
	declare v_start_date date;
	declare v_end_date date;
	declare v_is_vbr varchar(5);
	declare v_rateprofileid varchar(200);
	declare v_rate_type varchar(20);
	declare v_ceiling int(20) default null;
	declare v_rate double(15,2);
	declare v_back_to_first_rate double(15,2);
	declare v_amount double(15,2);
	declare v_hybrid_type varchar(20);
	declare v_back_to_first int(20);
	declare amountapplied double(15,2);
	declare lastceiling int(20);
	declare thisceiling int(20);
	declare maxceiling int(20);
	declare callsapplied int(20);
	declare callsleft int(20);
	declare back_to_first_flag boolean;
	declare done boolean;

/*-- these cursors can be changed to suit the need of application --*/
/*		select uniqueid, group_concat(callvolumeid separator ',') as callvolumeid, sum(duration) as duration, rate_profile_detail_id, rate_type, is_vbr 
from vw_call_volume_link group by uniqueid, rate_type, is_vbr; */
	declare maincursor cursor for 
		select
		group_concat(uniqueid separator ',') as uniqueid, 
		group_concat(tier_code separator '-') as tier_code,
		operator_code, 
		service_code,
		product_code,
		date_format(start_date, '%d/%m/%Y') as start_date,
		date_format(end_date, '%d/%m/%Y') as end_date,
		sum(duration) as duration,
		rate_profile_detail_id, 
		rate_type, 
		is_vbr
	from 
		vw_call_volume_link 
	group by 
		operator_code, 
		service_code,
		product_code,
		date_format(start_date, '%d/%m/%Y'),
		date_format(end_date, '%d/%m/%Y'),
		rate_profile_detail_id, 
		rate_type, 
		is_vbr;
	declare cur_calc cursor for select ceiling, rate, amount, hybrid_type from rate_profiles where deleted_at is null and rate_profile_detail_id=v_rateprofileid order by hybrid_type, ifnull(ceiling,100000000000) asc; 
	declare continue handler for not found set done = true; 
/*--this is the handler that will check if fetch is complete, variable done is boolen defined above--*/
/*----------------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------------*/
open maincursor;

notvbr_loop: loop

	fetch maincursor into v_uniqueid, v_tier_code, v_operator_code, v_service_code, v_product_id, v_start_date, v_end_date, v_callduration, v_rateprofileid, v_rate_type, v_is_vbr;

if done then
    close maincursor;
    leave notvbr_loop;
end if;
/*----------------------------------------------------------------------------------*/
set @v_query= concat('update data_final_details_t set deleted_at=sysdate() where uniqueid =''',v_uniqueid,''' and deleted_at is null');
prepare stmt from @v_query;
execute stmt;
deallocate prepare stmt;
commit;
set @v_query=null;
/*----------------------------------------------------------------------------------*/
if v_rate_type='flat' then 
open cur_calc;
fetch cur_calc into v_ceiling, v_rate, v_amount, v_hybrid_type;
	if done then
		close cur_calc;
    leave notvbr_loop;
	end if;	
    
/*	set @v_query= concat('insert into data_final_details_t values(''',v_uniqueid,''',''',v_rate_type,''',', ifnull(v_ceiling,0),',',ifnull(v_rate,0),',',v_amount,',''',ifnull(v_hybrid_type,'null'),''',',v_amount,',sysdate(),''',v_rateprofileid,''',''',v_is_vbr,''',null)'); */

	set @v_query=concat('insert into data_final_details_t values(''',v_uniqueid,''',''',v_tier_code,''',''',v_rate_type,''',',ifnull(v_ceiling,0),',',ifnull(v_rate,0),',',ifnull(v_callduration,0),',''',ifnull(v_hybrid_type,'null'),''',',v_amount,',sysdate(),''',v_rateprofileid,''',''',v_is_vbr,''',''',v_operator_code,''',''',v_service_code,''',''',v_product_id,''',str_to_date(''',v_start_date,''',''%d/%m/%Y''),str_to_date(''',v_end_date,''',''%d/%m/%Y''), null)');

	insert into query_log values(v_uniqueid, @v_query, sysdate()); 
	prepare stmt from @v_query;
	execute stmt;
	deallocate prepare stmt;
end if;
/*----------------------------------------------------------------------------------*/
if v_rate_type='fixed' then 
open cur_calc;
fetch cur_calc into v_ceiling, v_rate, v_amount, v_hybrid_type;
	if done then
		close cur_calc;
		leave notvbr_loop;
	end if;	
/*	set @v_query= concat('insert into data_final_details_t values(''',v_uniqueid,''',''',v_rate_type,''',', ifnull(v_ceiling,0),',',ifnull(v_rate,0),',',round(v_amount*v_rate,2),',''',ifnull(v_hybrid_type,'null'),''',',v_amount,',sysdate(),''',v_rateprofileid,''',''',v_is_vbr,''',null)');*/
	set @v_query=concat('insert into data_final_details_t values(''',v_uniqueid,''',''',v_tier_code,''',''',v_rate_type,''',',ifnull(v_ceiling,0),',',ifnull(v_rate,0),',',ifnull(v_callduration,0),',''',ifnull(v_hybrid_type,'null'),''',',v_amount,',sysdate(),''',v_rateprofileid,''',''',v_is_vbr,''',''',v_operator_code,''',''',v_service_code,''',''',v_product_id,''',str_to_date(''',v_start_date,''',''%d/%m/%Y''),str_to_date(''',v_end_date,''',''%d/%m/%Y''), null)');

	insert into query_log values(v_uniqueid, @v_query, sysdate());
	prepare stmt from @v_query;
	execute stmt;
	commit;
	deallocate prepare stmt; 
end if;
/*----------------------------------------------------------------------------------*/
if v_rate_type='incremental' then	
	set callsleft = v_callduration;
	set lastceiling=0;
	open cur_calc;
	calc_loop: loop 
		fetch cur_calc into v_ceiling, v_rate, v_amount, v_hybrid_type;
			if done then
				close cur_calc;
				leave calc_loop;
			end if;	
		set thisceiling = v_ceiling - lastceiling;

		if (v_callduration - v_ceiling) is null or (v_callduration - v_ceiling) <=0 then			
			set callsapplied=callsleft;
			set callsleft=0;
		else
			set callsapplied=thisceiling;
			set callsleft = callsleft - thisceiling;
		end if;

		set amountapplied= round(callsapplied * v_rate, 2);

		set lastceiling= v_ceiling;
		if amountapplied>0 then
			/*		set @v_query= concat('insert into data_final_details_t values(''',v_uniqueid,''', ''',v_rate_type,''', ',v_ceiling,', ',v_rate,', ',callsapplied,', ''', v_hybrid_type,''', ',amountapplied,', sysdate(), ''',v_rateprofileid,''',''',v_is_vbr,''',null)');	*/
			
			set @v_query=concat('insert into data_final_details_t values(''',v_uniqueid,''',''',v_tier_code,''',''',v_rate_type,''',',ifnull(v_ceiling,0),',',ifnull(v_rate,0),',',callsapplied,',''',ifnull(v_hybrid_type,'null'),''',',amountapplied,',sysdate(),''',v_rateprofileid,''',''',v_is_vbr,''',''',v_operator_code,''',''',v_service_code,''',''',v_product_id,''',str_to_date(''',v_start_date,''',''%d/%m/%Y''),str_to_date(''',v_end_date,''',''%d/%m/%Y''), null)');
			insert into query_log values(v_uniqueid, @v_query, sysdate());				
			prepare stmt from @v_query;
			execute stmt;
			commit;
			deallocate prepare stmt; 
		end if;
		set @v_query =null;
	end loop calc_loop;
end if;
/*----------------------------------------------------------------------------------*/ 
if v_rate_type='hybrid' then
	set callsleft = v_callduration;
	set lastceiling=0;
	open cur_calc;
	calc_hyb_loop: loop
	fetch cur_calc into v_ceiling, v_rate, v_amount, v_hybrid_type;
		if done then
			close cur_calc;
			leave calc_hyb_loop;
		end if;		
/*----------------------------------*/
	if v_hybrid_type='back_to_first' then
		select max(ceiling) from rate_profiles where rate_profile_detail_id=v_rateprofileid into maxceiling;
		set v_back_to_first= v_ceiling;
		set v_back_to_first_rate= v_rate;
		set callsapplied=0;
		set amountapplied= 0;
/*----------------------------------*/
	elseif v_hybrid_type='flat' then
		set thisceiling = v_ceiling - lastceiling;
		if (v_callduration - v_ceiling) <= 0 then 
			set callsapplied=callsleft;
			set v_rate=0;
			set callsleft=0;
		else 
			set callsleft = callsleft - thisceiling;
			set callsapplied=thisceiling;
			set v_rate=0;
		end if;
		set lastceiling= v_ceiling;
		set amountapplied= round(v_amount,2);
/*----------------------------------*/
	elseif v_hybrid_type='incremental' then
		
		set thisceiling = v_ceiling - lastceiling;
				
		if v_callduration> ifnull(maxceiling,100000000000) and v_ceiling > ifnull(v_back_to_first,100000000000) then 
			set v_rate= v_back_to_first_rate;
			set callsapplied=callsleft;
			set back_to_first_flag=true;
			set callsleft=0;
		elseif 	ifnull(v_callduration - v_ceiling,0) <= 0 then 
			set callsapplied=callsleft;
			set callsleft=0;
		else 
			set callsleft = callsleft - thisceiling;
			set callsapplied=thisceiling;
		end if;
			set lastceiling= v_ceiling;
			set amountapplied= round(callsapplied * v_rate, 2);
			if back_to_first_flag then set v_hybrid_type='back_to_first';
			end if;
	end if;
/*----------------------------------*/
	if amountapplied>0 then
			set @v_query=concat('insert into data_final_details_t values(''',v_uniqueid,''',''',v_tier_code,''',''',v_rate_type,''',',ifnull(v_ceiling,0),',',ifnull(v_rate,0),',',callsapplied,',''',ifnull(v_hybrid_type,'null'),''',',amountapplied,',sysdate(),''',v_rateprofileid,''',''',v_is_vbr,''',''',v_operator_code,''',''',v_service_code,''',''',v_product_id,''',str_to_date(''',v_start_date,''',''%d/%m/%Y''),str_to_date(''',v_end_date,''',''%d/%m/%Y''), null)');
			insert into query_log values(v_uniqueid, @v_query, sysdate());
			prepare stmt from @v_query;
			execute stmt;
			commit;
			deallocate prepare stmt; 
		end if;
		set @v_query =null;
	end loop calc_hyb_loop;	
end if;
/*----------------------------------------------------------------------------------*/
set @v_query=null;
/*----------------------------------*/
end loop notvbr_loop;
/*----------------------------------------------------------------------------------*/
set @v_query='update data_final_amount a set a.deleted_at=sysdate()
where concat(a.billing_operator,a.service_id,date_format(a.start_date,''%d%m%Y''),date_format(a.end_date,''%d%m%Y''))
IN
(select Concat(b.operator_code, b.service_code,date_format(b.start_date,''%d%m%Y''),date_format(b.end_date,''%d%m%Y'')) 
from vw_call_volume_link b) and a.deleted_at is null';
prepare stmt from @v_query;
execute stmt;
commit;
deallocate prepare stmt;
set @v_query=null;
/*-------------------------------------------*/
set @v_query='insert into data_final_amount(billing_operator, billing_operator_name, event_duration_minutes, average_rate, total_amount, service_id, created_at, start_date, end_date)
select a.operator_code, o.name, sum(a.callduration), round(sum(a.calculated_amount)/sum(a.callduration),2), sum(a.calculated_amount), a.service_code, a.calculated_at, a.start_date, a.end_date
from data_final_details_t a
left join
operators o
on a.operator_code=o.code
where a.deleted_at is null and 
Concat(a.operator_code, a.service_code,date_format(a.start_date,''%d%m%Y''),date_format(a.end_date,''%d%m%Y''))
in 
(select Concat(v.operator_code,v.service_code,date_format(v.start_date,''%d%m%Y''),date_format(v.end_date,''%d%m%Y'')) 
from vw_call_volume_link v)
group by a.operator_code, o.name, a.service_code, a.calculated_at, a.start_date, a.end_date';
prepare stmt from @v_query;
execute stmt;
commit;
deallocate prepare stmt; 
set @v_query=null;
/*----------------------------------------------------------------------------------*/	
end;
/*
insert into data_final_amount(billing_operator, billing_operator_name, event_duration_minutes, average_rate, total_amount, service_id, created_at, start_date, end_date)
select a.operator_code, o.name, sum(callduration), round(sum(calculated_amount)/sum(callduration),2), sum(calculated_amount), service_code, calculated_at, start_date, end_date
from data_final_details_t a
left join
operators o
on a.operator_code=b.code
group by a.operator_code, o.name, service_code, calculated_at, start_date, end_date;
commit;

update data_final_details_t a,data_final_amount b
set a.data_final_id=b.id
where a.billing_operator=b.operator_code and a.service_id=b.service_code and a.start_date=b.start_date and a.end_date=b.end_date;
commit;
*/