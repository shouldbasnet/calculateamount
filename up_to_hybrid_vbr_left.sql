use ntc;
DROP PROCEDURE IF EXISTS CALCULATE;
DELIMITER $$
CREATE PROCEDURE ntc.CALCULATE (Calculate_Claim_ID VARCHAR(200)) 
/*--we can simply add a flag in the data_volume table and not pass the Calculate_Claim_ID, we can modify the cursor Cur_isnotvbr and Cur_isvbr below--*/
/*--but if we add a flag, we must change the flag after calculation is complete --*/
BEGIN
	DECLARE v_Query varchar(2000);
	DECLARE v_CallVolume integer(19);
	DECLARE v_ClaimID varchar(200);
	DECLARE v_RateProfileID VARCHAR(200);
	DECLARE v_rate_type VARCHAR(20);
	DECLARE v_ceiling int(20) DEFAULT NULL;
	DECLARE v_rate double(15,2);
	DECLARE v_back_to_first_rate double(15,2);
	DECLARE v_amount double(15,2);
	DECLARE v_hybrid_type varchar(20);
	DECLARE v_back_to_first int(20);
	DECLARE AmountApplied double(15,2);
	DECLARE lastceiling int(20);
	DECLARE ThisCeiling int(20);
	DECLARE maxCeiling int(20);
	DECLARE CallsApplied int(20);
	DECLARE CallsLeft INT(20);
	DECLARE back_to_first_flag boolean;
	DECLARE done boolean;

	DECLARE Cur_isnotvbr CURSOR FOR SELECT CallVolumeID, Volume, rate_profile_detail_id, rate_type FROM ntc.VW_CALL_VOLUME_LINK WHERE Upper(is_vbr)='NO' AND CallVolumeID=Calculate_Claim_ID;
/*-- these cursors can be changed to suit the need of application --*/
	DECLARE Cur_isvbr CURSOR FOR SELECT CallVolumeID, Volume, rate_profile_detail_id, rate_type FROM ntc.VW_CALL_VOLUME_LINK WHERE Upper(is_vbr)='YES' AND CallVolumeID=Calculate_Claim_ID;
/*-- we might not need a separate cursor for non-vbr if we use a RollupID of some sort in VW_CALL_VOLUME_LINK to sum up the VBRs --*/
	DECLARE Cur_calc CURSOR FOR SELECT ceiling, rate, amount, hybrid_type from ntc.rate_profiles WHERE deleted_at IS NULL AND rate_profile_detail_id=(SELECT DISTINCT rate_profile_detail_id FROM VW_CALL_VOLUME_LINK WHERE CallVolumeID=Calculate_Claim_ID) ORDER BY hybrid_type, IFNULL(ceiling,100000000000) asc; 
	DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE; /*--this is the handler that will check if fetch is complete, variable done is boolen defined above--*/
/*----------------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------------*/
OPEN Cur_isnotvbr;

notvbr_loop: LOOP

	Fetch Cur_isnotvbr into v_ClaimID, v_CallVolume, v_RateProfileID, v_rate_type;

	IF done THEN
		CLOSE Cur_isnotvbr;
		LEAVE notvbr_loop;
	END IF;
/*----------------------------------------------------------------------------------*/
IF v_rate_type='flat' THEN 
OPEN Cur_calc;
FETCH Cur_calc INTO v_ceiling, v_rate, v_amount, v_hybrid_type;
	IF done THEN
		CLOSE Cur_calc;
		LEAVE notvbr_loop;
	END IF;	
	SET v_Query= CONCAT('INSERT INTO ntc.calculated_details VALUES(''',v_ClaimID,''',''',v_rate_type,''',IFNULL(',v_ceiling,',0),IFNULL(',v_rate,',0),',v_amount,',IFNULL(''',v_hybrid_type,''',''NULL''),',v_amount,',sysdate(),''',v_RateProfileID,''')');
	SELECT v_Query; /*--this will print the dynamic query before executing it, it'd be better to log all queries into a query_log table --*/
	PREPARE stmt from v_Query;
	EXECUTE stmt;
	COMMIT;
	DEALLOCATE PREPARE stmt; 
END IF;
/*----------------------------------------------------------------------------------*/
IF v_rate_type='fixed' THEN 
OPEN Cur_calc;
FETCH Cur_calc INTO v_ceiling, v_rate, v_amount, v_hybrid_type;
	IF done THEN
		CLOSE Cur_calc;
		LEAVE notvbr_loop;
	END IF;	
	SET v_Query= CONCAT('INSERT INTO ntc.calculated_details VALUES(''',v_ClaimID,''',''',v_rate_type,''',IFNULL(',v_ceiling,',0),IFNULL(',v_rate,',0),',Round(v_amount*v_rate,2),',IFNULL(''',v_hybrid_type,''',''NULL''),',v_amount,',sysdate(),''',v_RateProfileID,''')');
	SELECT v_Query;
	PREPARE stmt from v_Query;
	EXECUTE stmt;
	COMMIT;
	DEALLOCATE PREPARE stmt; 
END IF;
/*----------------------------------------------------------------------------------*/
IF v_rate_type='incremental' THEN	
	SET CallsLeft = v_CallVolume;
	SET lastceiling=0;
	OPEN Cur_calc;
	calc_loop: LOOP 
		FETCH Cur_calc INTO v_ceiling, v_rate, v_amount, v_hybrid_type;
			IF done THEN
				CLOSE Cur_calc;
				LEAVE calc_loop;
			END IF;	
		SET ThisCeiling = v_ceiling - lastceiling;

		IF (v_CallVolume - v_ceiling) IS NULL OR (v_CallVolume - v_ceiling) <=0 THEN			
			SET CallsApplied=CallsLeft;
			SET CallsLeft=0;
		ELSE
			SET CallsApplied=ThisCeiling;
			SET CallsLeft = CallsLeft - ThisCeiling;
		END IF;

		SET AmountApplied= Round(CallsApplied * v_rate, 2);

		SET lastceiling= v_ceiling;

		INSERT INTO ntc.calculated_details VALUES(v_ClaimID, v_rate_type, v_ceiling, v_rate, CallsApplied, v_hybrid_type, AmountApplied, sysdate(), v_RateProfileID);
		COMMIT;
	END LOOP calc_loop;
END IF;
/*----------------------------------------------------------------------------------*/ 
IF v_rate_type='hybrid' THEN
	SET CallsLeft = v_CallVolume;
	SET lastceiling=0;
	OPEN Cur_calc;
	calc_hyb_loop: LOOP
	FETCH Cur_calc INTO v_ceiling, v_rate, v_amount, v_hybrid_type;
		IF done THEN
			CLOSE Cur_calc;
			LEAVE calc_hyb_loop;
		END IF;		
/*----------------------------------*/
	IF v_hybrid_type='back_to_first' THEN
		SELECT Max(ceiling) FROM ntc.rate_profiles WHERE rate_profile_detail_id=v_RateProfileID INTO maxCeiling;
		SET v_back_to_first= v_ceiling;
		SET v_back_to_first_rate= v_rate;
		SET CallsApplied=0;
/*----------------------------------*/
	ELSEIF v_hybrid_type='flat' THEN
		SET ThisCeiling = v_ceiling - lastceiling;
		IF (v_CallVolume - v_ceiling) <= 0 THEN 
			SET CallsApplied=v_amount;
			SET v_rate=1;
			SET CallsLeft=0;
		ELSE 
			SET CallsLeft = CallsLeft - ThisCeiling;
			SET CallsApplied=v_amount;
			SET v_rate=1;
		END IF;
		SET lastceiling= v_ceiling;
/*----------------------------------*/
	ELSEIF v_hybrid_type='incremental' THEN
		SET ThisCeiling = v_ceiling - lastceiling;
		IF v_CallVolume> IFNULL(maxCeiling,100000000000) AND v_ceiling>= IFNULL(v_back_to_first,0) THEN 
			SET v_rate=v_back_to_first_rate;
			SET back_to_first_flag=TRUE;
		END IF;					
		IF IFNULL(v_CallVolume - v_ceiling,0) <= 0 THEN 
			SET CallsApplied=CallsLeft;
			SET CallsLeft=0;
		ELSE 
			SET CallsLeft = CallsLeft - ThisCeiling;
			SET CallsApplied=ThisCeiling;
		END IF;
			SET lastceiling= v_ceiling;
	END IF;
/*----------------------------------*/
	SET AmountApplied= Round(CallsApplied * v_rate, 2);
/*----------------------------------*/
	INSERT INTO ntc.calculated_details VALUES(v_ClaimID, v_rate_type, v_ceiling, v_rate, CallsApplied, v_hybrid_type, AmountApplied, sysdate(), v_RateProfileID);
	COMMIT;
	END LOOP calc_hyb_loop;	
/*----------------------------------*/
	IF back_to_first_flag AND CallsLeft>0 THEN 
		SET AmountApplied= Round(CallsLeft * v_back_to_first_rate, 2);
		INSERT INTO ntc.calculated_details VALUES(v_ClaimID, v_rate_type, NULL, v_back_to_first_rate, CallsLeft, 'incremental', AmountApplied, sysdate(), v_RateProfileID);
		COMMIT;
	END IF;
END IF;
/*----------------------------------------------------------------------------------*/
END LOOP notvbr_loop;		
END;