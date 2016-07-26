use ntc;
DROP PROCEDURE IF EXISTS CALCULATE;
DELIMITER $$
CREATE PROCEDURE ntc.CALCULATE (Calculate_Claim_ID VARCHAR(200)) 
BEGIN
	DECLARE vbquery VARCHAR(2000);
	DECLARE vQuery VARCHAR(2000);
	DECLARE vbquery2 varchar(2000);
	DECLARE Calls INT(19);
	DECLARE CallsLeft INT(20);
	DECLARE v_CallVolume integer(19);
	DECLARE v_ClaimID varchar(200);
	DECLARE v_RateProfileID VARCHAR(200);
	DECLARE v_rate_type VARCHAR(20);
	DECLARE v_ceiling int(20) DEFAULT NULL;
	DECLARE lastceiling int(20);
	DECLARE ThisCeiling int(20);
	DECLARE CallsApplied int(20);
	DECLARE v_rate double(15,2);
	DECLARE v_amount double(15,2);
	DECLARE AmountApplied double(15,2);
	DECLARE v_hybrid_type varchar(20);
	DECLARE done boolean;

	DECLARE Cur_isnotvbr CURSOR FOR SELECT CallVolumeID, Volume, rate_profile_detail_id, rate_type FROM VW_CALL_VOLUME_LINK WHERE Upper(is_vbr)='NO' AND CallVolumeID=Calculate_Claim_ID;
	DECLARE Cur_isvbr CURSOR FOR SELECT CallVolumeID, Volume, rate_profile_detail_id, rate_type FROM VW_CALL_VOLUME_LINK WHERE Upper(is_vbr)='YES' AND CallVolumeID=Calculate_Claim_ID;
	DECLARE Cur_calc CURSOR FOR SELECT ceiling, rate, amount, hybrid_type from ntc.rate_profiles WHERE deleted_at IS NULL AND rate_profile_detail_id=(SELECT DISTINCT rate_profile_detail_id FROM VW_CALL_VOLUME_LINK WHERE CallVolumeID=Calculate_Claim_ID) ORDER BY hybrid_type, IFNULL(ceiling,100000000000) asc; 
	DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;


OPEN Cur_isnotvbr;

notvbr_loop: LOOP

	Fetch Cur_isnotvbr into v_ClaimID, v_CallVolume, v_RateProfileID, v_rate_type;

	IF done THEN
		CLOSE Cur_isnotvbr;
		LEAVE notvbr_loop;
	END IF;

/* SELECT Max(IFNULL(ceiling,0))+ @v_CallVolume FROM ntc.rate_profiles WHERE deleted_at IS NULL AND rate_profile_detail_id=@v_RateProfileID INTO @NewCeiling;*/
	
		IF v_rate_type='flat' THEN 
      OPEN Cur_calc;
			FETCH Cur_calc INTO v_ceiling, v_rate, v_amount, v_hybrid_type;
			IF done THEN
			CLOSE Cur_calc;
			LEAVE notvbr_loop;
			END IF;	
			SET @vbquery2:= CONCAT('INSERT INTO ntc.calculated_details VALUES(''',v_ClaimID,''',''',v_rate_type,''',IFNULL(',v_ceiling,',0),IFNULL(',v_rate,',0),',v_amount,',IFNULL(''',v_hybrid_type,''',''NULL''),',v_amount,',sysdate(),''',v_RateProfileID,''')');
			SELECT @vbquery2;
			PREPARE stmt from @vbquery2;
			EXECUTE stmt;
			COMMIT;
			DEALLOCATE PREPARE stmt; 
		END IF;

		IF v_rate_type='flat' THEN 
      OPEN Cur_calc;
			FETCH Cur_calc INTO v_ceiling, v_rate, v_amount, v_hybrid_type;
			IF done THEN
			CLOSE Cur_calc;
			LEAVE notvbr_loop;
			END IF;	
			SET @vbquery2:= CONCAT('INSERT INTO ntc.calculated_details VALUES(''',v_ClaimID,''',''',v_rate_type,''',IFNULL(',v_ceiling,',0),IFNULL(',v_rate,',0),',v_amount,',IFNULL(''',v_hybrid_type,''',''NULL''),',v_amount,',sysdate(),''',v_RateProfileID,''')');
			SELECT @vbquery2;
			PREPARE stmt from @vbquery2;
			EXECUTE stmt;
			COMMIT;
			DEALLOCATE PREPARE stmt; 
		END IF;

		IF v_rate_type='incremental' THEN	
		SET Calls=0;
		SET CallsLeft = v_CallVolume;
		SET lastceiling=0;
        
    OPEN Cur_calc;
		calc_loop: LOOP 
			FETCH Cur_calc INTO v_ceiling, v_rate, v_amount, v_hybrid_type;
			IF done THEN
				CLOSE Cur_calc;
				LEAVE calc_loop;
			END IF;	
			/*
			select Max(rank), Min(rank) from 
			(
			SELECT a.ceiling, a.rate, @curRank := @curRank + 1 AS rank
			FROM  
			ntc.rate_profiles a, (SELECT @curRank := 0) r
			Where rate_profile_detail_id = @v_RateProfileID
			ORDER BY ceiling asc
			) z into @maxrank, @minrank;
			*/
			SET calls= v_CallVolume - v_ceiling;
			SET ThisCeiling = v_ceiling - lastceiling;
			
		IF calls IS NULL OR calls <=0 THEN			
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
END LOOP notvbr_loop;		
END;