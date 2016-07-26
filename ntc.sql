CREATE OR REPLACE PROCEDURE CALCULATE ( CLAIMID VARCHAR DEFAULT NULL) 
IS
	vbquery varchar(2000);
	vbquery2 varchar(2000);
	Calls integer(19);
	CallVolume integer(19);
	ClaimID varchar(200);
	RateProfileID VARCHAR(200);
	rate_type VARCHAR(20);
	ceiling int(20) DEFAULT NULL;
	rate double(15,2);
	amount double(15,2);
	hybrid_type varchar(20);
	TYPE cur_type IS REF CURSOR;
	isvbr cur_type;
	calc cur_type;
	isnotvbr curtype;
	
BEGIN 

OPEN isnotvbr FOR 'SELECT CallVolumeID, Volume, rate_profile_detail_id FROM VW_CALL_VOLUME_LINK WHERE Upper(is_vbr)=''NO''';
	
LOOP
	Fetch isnotvbr into ClaimID, CallVolume, RateProfileID;
	EXIT WHEN isnotvbr%NOTFOUND;
		OPEN calc FOR 'SELECT rate_type, ceiling, rate, amount, hybrid_type from ntc.rate_profiles WHERE deleted_at IS NULL AND rate_profile_detail_id='''||RateProfileID||'''';
		LOOP 
			Fetch calc into rate_type, ceiling, rate, amount, hybrid_type;
			EXIT WHEN calc%NOTFOUND;
			
			IF rate_type='flat' THEN 
			vbquery:= 'INSERT INTO ntc.calculated_details
					   ('''||ClaimID||''', '''||rate_type||''', '||ceiling||', '||rate||', '||amount||', '''||hybrid_type||''', '||amount||', sysdate(), '''||RateProfileID||''')';
			BEGIN EXECUTE IMMEDIATE vbquery;
			EXCEPTION WHEN OTHERS THEN GET DIAGNOSTICS CONDITION 1 @p1 = RETURNED_SQLSTATE, @p2 = MESSAGE_TEXT;
			SELECT @p1, @p2;
			END;
			END IF;
			
			IF rate_type='fixed' THEN 
			vbquery:= 'INSERT INTO ntc.calculated_details
					   ('''||ClaimID||''', '''||rate_type||''', '||ceiling||', '||rate||', '||amount||', '''||hybrid_type||''', Round('||CallVolume||'*'||rate||',2), sysdate(), '''||RateProfileID||''')';
			BEGIN EXECUTE IMMEDIATE vbquery;
			EXCEPTION WHEN OTHERS THEN GET DIAGNOSTICS CONDITION 1 @p1 = RETURNED_SQLSTATE, @p2 = MESSAGE_TEXT;
			SELECT @p1, @p2;
			END;
			END IF;
			
			IF rate_type='incremental' THEN	
			vbQuery:=
			'select Max(rank) from 
			(
			SELECT a.ceiling, a.rate, @curRank := @curRank + 1 AS rank
			FROM  
			ntc.rate_profiles a, (SELECT @curRank := 0) r
			Where rate_profile_detail_id ='''||RateProfileID||'''
			ORDER BY ceiling asc
			) z into @maxrank';
			BEGIN EXECUTE IMMEDIATE vbQuery;
			EXCEPTION WHEN OTHERS THEN GET DIAGNOSTICS CONDITION 1 @p1 = RETURNED_SQLSTATE, @p2 = MESSAGE_TEXT;
			SELECT @p1, @p2;
			END;
			label1: LOOP 
				SELECT 

		
		
		
			
		