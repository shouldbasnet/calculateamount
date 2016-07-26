CREATE OR REPLACE PROCEDURE CALCULATE ( CLAIMID VARCHAR DEFAULT NULL) 
IS
	vbquery varchar(2000);
	vbquery2 varchar(2000);
	Calls integer(19);
	DECLARE CallVolume integer(19);
	DECLARE ClaimID varchar(200);
	DECLARE RateProfileID VARCHAR(200);
	rate_type VARCHAR(20);
	ceiling int(20) DEFAULT NULL;
	rate double(15,2);
	amount double(15,2);
	hybrid_type varchar(20);
	TYPE cur_type IS REF CURSOR;
	isvbr cur_type;
	calc cur_type;
	isnotvbr curtype;
	DECLARE done BOOLEAN;
	DECLARE Cur_isnotvbr CURSOR FOR SELECT CallVolumeID, Volume, rate_profile_detail_id, rate_type FROM VW_CALL_VOLUME_LINK WHERE Upper(is_vbr)='NO';
	DECLARE Cur_isvbr CURSOR FOR SELECT CallVolumeID, Volume, rate_profile_detail_id, rate_type FROM VW_CALL_VOLUME_LINK WHERE Upper(is_vbr)='YES';
	DECLARE Cur_clac CURSOR FOR SELECT * from vw_rate_prof_pull;
	DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;
	
BEGIN 
	OPEN Cur_isnotvbr;
	
notvbr_loop: LOOP

	Fetch isnotvbr into @ClaimID, @CallVolume, @RateProfileID, @Rate_type;
	IF done THEN
		CLOSE Cur_isnotvbr;
		LEAVE notvbr_loop;
	END IF;
	
/* SELECT Max(IFNULL(ceiling,0))+ @CallVolume FROM ntc.rate_profiles WHERE deleted_at IS NULL AND rate_profile_detail_id=@RateProfileID INTO @NewCeiling;*/

	CREATE OR REPLACE VIEW vw_rate_prof_pull as SELECT rate_type, ceiling, rate, amount, hybrid_type from ntc.rate_profiles WHERE deleted_at IS NULL AND rate_profile_detail_id=@RateProfileID ORDER BY IFNULL(ceiling,100000000000) asc;

	OPEN Cur_calc;
	
		IF @rate_type='flat' THEN 
			FETCH Cur_calc INTO @rate_type2, @ceiling, @rate, @amount, @hybrid_type;
			IF done THEN
			CLOSE Cur_calc;
			LEAVE notvbr_loop;
			END IF;	
			SET @vbquery2:= CONCAT('INSERT INTO ntc.calculated_details VALUES(''',@ClaimID,''',''',@rate_type,''',IFNULL(',@ceiling,',0),IFNULL(',@rate,',0),',@amount,',IFNULL(''',@hybrid_type,''',''NULL''),',@amount,',sysdate(),''',@RateProfileID,''')');
			SELECT @vbquery2;
			PREPARE stmt from @vbquery2;
			EXECUTE stmt;
			COMMIT;
			DEALLOCATE PREPARE stmt; 
		END IF;

		IF @rate_type='flat' THEN 
			FETCH Cur_calc INTO @rate_type2, @ceiling, @rate, @amount, @hybrid_type;
			IF done THEN
			CLOSE Cur_calc;
			LEAVE notvbr_loop;
			END IF;	
			SET @vbquery2:= CONCAT('INSERT INTO ntc.calculated_details VALUES(''',@ClaimID,''',''',@rate_type,''',IFNULL(',@ceiling,',0),IFNULL(',@rate,',0),',@amount,',IFNULL(''',@hybrid_type,''',''NULL''),',@amount,',sysdate(),''',@RateProfileID,''')');
			SELECT @vbquery2;
			PREPARE stmt from @vbquery2;
			EXECUTE stmt;
			COMMIT;
			DEALLOCATE PREPARE stmt; 
		END IF;

		IF @rate_type='incremental' THEN	
		SET @Calls=0;
		SET @CallsLeft = @CallAmount;
		SET @lastceiling=0;
		calc_loop: LOOP 
			FETCH Cur_calc INTO @rate_type2, @ceiling, @rate, @amount, @hybrid_type;
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
			Where rate_profile_detail_id = @RateProfileID
			ORDER BY ceiling asc
			) z into @maxrank, @minrank;
			*/
			@calls= @CallAmount - @ceiling;
			@ThisCeiling = @ceiling - @lastceiling;
			
			IF @calls IS NULL OR @calls <=0 THEN			
				SET @result=@CallsLeft;
				SET @CallsLeft=0;
			ELSE
				SET @result=@ThisCeiling;
			END IF;
			
			SET @CallsLeft = @CallsLeft - @ThisCeiling;
						
			SET @AmountApplied= Round(@result * @rate, 2);
			
			SET @lastceiling= @ceiling;
			
			INSERT INTO ntc.calculated_details VALUES(@ClaimID, @rate_type, @ceiling, @rate, @amount, @hybrid_type, @AmountApplied, sysdate(), @RateProfileID);
			COMMIT;
		END LOOP calc_loop;
			
			
		IF @rate_type='flat' THEN 
			SET @vbquery2:= CONCAT('INSERT INTO ntc.calculated_details VALUES(''',@ClaimID,''',''',@rate_type,''',IFNULL(',@ceiling,',0),IFNULL(',@rate,',0),',@amount,',IFNULL(''',@hybrid_type,''',''NULL''),',@amount,',sysdate(),''',@RateProfileID,''')');
			SELECT @vbquery2;
			PREPARE stmt from @vbquery2;
			EXECUTE stmt;
			DEALLOCATE PREPARE stmt; 
			END IF;
			
		IF @rate_type='fixed' THEN 
			SET @vbquery2:= 'INSERT INTO ntc.calculated_details VALUES(''',@ClaimID,''',''',@rate_type,''',IFNULL(',@ceiling,',0),IFNULL(',@rate,',0),',@amount,',IFNULL(''',@hybrid_type,''',''NULL''),Round('@CallVolume'*'@rate',2),sysdate(),''',@RateProfileID,''')';
			SELECT @vbquery2;
			PREPARE stmt from @vbquery2;
			EXECUTE stmt;
			DEALLOCATE PREPARE stmt; 
			END IF;
			
		IF rate_type='incremental' THEN	
			select Max(rank), Min(rank) from 
			(
			SELECT a.ceiling, a.rate, @curRank := @curRank + 1 AS rank
			FROM  
			ntc.rate_profiles a, (SELECT @curRank := 0) r
			Where rate_profile_detail_id = @RateProfileID
			ORDER BY ceiling asc
			) z into @maxrank, @minrank;
				
				WHILE @maxrank>0 DO
				
				
		
		


		
		
		
			
		