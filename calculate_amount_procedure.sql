use ntc;
drop procedure if exists calculate_amount;
delimiter $$
create procedure calculate_amount()

begin
	declare v_callduration integer(19);
	declare v_callvolumeid varchar(200);
	declare v_uniqueid varchar(400);
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
	declare maincursor cursor for 
		select uniqueid, group_concat(callvolumeid separator ',') as callvolumeid, sum(duration) as duration, rate_profile_detail_id, rate_type, is_vbr 
		from vw_call_volume_link group by uniqueid, rate_type, is_vbr; 
	declare cur_calc cursor for select ceiling, rate, amount, hybrid_type from rate_profiles where deleted_at is null and rate_profile_detail_id=v_rateprofileid order by hybrid_type, ifnull(ceiling,100000000000) asc; 
	declare continue handler for not found set done = true; 
/*--this is the handler that will check if fetch is complete, variable done is boolen defined above--*/
/*----------------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------------*/
open maincursor;

notvbr_loop: loop

	fetch maincursor into v_uniqueid, v_callvolumeid, v_callduration, v_rateprofileid, v_rate_type, v_is_vbr;

if done then
    close maincursor;
    leave notvbr_loop;
end if;
/*----------------------------------------------------------------------------------*/
set @v_query= concat('update calculated_details set deleted_at=sysdate() where uniqueid =''',v_uniqueid,''' and deleted_at is null');
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
    
	set @v_query= concat('insert into calculated_details values(''',v_uniqueid,''',''',v_rate_type,''',', ifnull(v_ceiling,0),',',ifnull(v_rate,0),',',v_amount,',''',ifnull(v_hybrid_type,'null'),''',',v_amount,',sysdate(),''',v_rateprofileid,''',''',v_is_vbr,''',null)');
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
	set @v_query= concat('insert into calculated_details values(''',v_uniqueid,''',''',v_rate_type,''',', ifnull(v_ceiling,0),',',ifnull(v_rate,0),',',round(v_amount*v_rate,2),',''',ifnull(v_hybrid_type,'null'),''',',v_amount,',sysdate(),''',v_rateprofileid,''',''',v_is_vbr,''',null)');

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
			set @v_query= concat('insert into calculated_details values(''',v_uniqueid,''', ''',v_rate_type,''', ',v_ceiling,', ',v_rate,', ',callsapplied,', ''', v_hybrid_type,''', ',amountapplied,', sysdate(), ''',v_rateprofileid,''',''',v_is_vbr,''',null)');	
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
			set @v_query= concat('insert into calculated_details values(''',v_uniqueid,''', ''',v_rate_type,''', ',v_ceiling,', ',v_rate,', ',callsapplied,', ''', v_hybrid_type,''', ',amountapplied,', sysdate(), ''',v_rateprofileid,''',''',v_is_vbr,''',null)');
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
set @v_query= concat('delete from calculated_average_rate where uniqueid = ''',v_uniqueid,'''');
prepare stmt from @v_query;
execute stmt;
commit;
deallocate prepare stmt;
/*----------------------------------*/
set @v_query= concat(
'insert into calculated_average_rate
select a.callvolumeid as claimid, a.uniqueid, a.rate_type, sum(b.calculated_amount)/sum(b.callduration) as average_rate
from vw_call_volume_link a
left join
calculated_details b on
a.uniqueid=b.uniqueid and b.deleted_at is null where a.uniqueid=''',v_uniqueid,'''');	
prepare stmt from @v_query;
execute stmt;
commit;
deallocate prepare stmt; 
/*----------------------------------*/
end loop notvbr_loop;
/*----------------------------------------------------------------------------------*/

/*----------------------------------------------------------------------------------*/	
end;