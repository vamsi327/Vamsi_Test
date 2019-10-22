    CREATE OR REPLACE  PACKAGE BODY NBO_COLLOSSUS_DATA IS 
    v_message VARCHAR2(4000);
    PROCEDURE GET_COLOSSUS_DATA(Product_code IN VARCHAR2, P_RETURN_CODE out number ) 
    IS 
    L_DATE_1                            DATE ;
    L_DATE_2                            DATE;
    Trunc_N_dummy_sql                   VARCHAR2(1000); 
    Trun_Rel_col_sql                    VARCHAR2(1000); 
    Insert_rel_Col_sql                  VARCHAR2(32767);
    Drop_main_tbl                       VARCHAR2(32767);
    Create_main_table_sql               CLOB;
    Main_Insert_sql                     CLOB;
    C_LIST                              VARCHAR2(32767);
    Rel_table_name                      VARCHAR2(100);
    R_table_name                        VARCHAR2(100);
    Rel_str                             VARCHAR2(100);
    N_DUMMY_COL_LIST                    VARCHAR2(32767);
    Y_MAIN_MAX_DATE 					DATE;
    NOVATOR_START_DATE 					DATE;
    NOVATOR_END_DATE  					DATE;
    COLOSSUS_DATE 						DATE;
    
    BEGIN
    
    SELECT LAST_EXECUTION_DATE INTO Y_MAIN_MAX_DATE FROM NBO_EXECUTION_DATE where rownum =1;
    
    SELECT (CSS_WEEK_START_DATE-5), (CSS_WEEK_START_DATE+1) 
    INTO    NOVATOR_START_DATE,NOVATOR_END_DATE
    FROM DM_OWNER_STR_DL.dm_date 
    WHERE trunc(EDW_DATE)=TRUNC (Y_MAIN_MAX_DATE);
    
    COLOSSUS_DATE:=NOVATOR_END_DATE;
    
    Trunc_N_dummy_sql :='Truncate table NOVATOR_DUMMY ';
    EXECUTE IMMEDIATE Trunc_N_dummy_sql;
    
    insert into novator_dummy 
    select /*+ PARALLEL(a, 8) */ a.*,c.WEEKLY_TAB_NAME as TABLE_NAME,c.CREATED_DT as CREATE_DT
    from W_GEN_RESP_Y_MAIN_TABLE  a
    ,COLOSSUS_OWNER.COLOSSUS_ARC_LKP_WEEKLY c
    where trim( substr (c.WEEKLY_TAB_NAME,23,6))=to_char(COLOSSUS_DATE,'ddmmyy')
    and a.product_name = Product_code
    and trunc(EXECUTION_LAUNCH_DT) between trunc(NOVATOR_START_DATE) and trunc(NOVATOR_END_DATE) ;
    commit;
    
    /*
    insert into NOVATOR_DUMMY 
    ( select /*+ PARALLEL(a, 8) */ 
    /*a.*
    ,c.WEEKLY_TAB_NAME as TABLE_NAME
    ,c.CREATED_DT as CREATE_DT
    from W_GEN_RESP_Y_MAIN_TABLE  a
    ,COLOSSUS_OWNER.COLOSSUS_ARC_LKP_WEEKLY c
    where To_date (c.CREATED_DT,'DD-MON-YY')=
    (select TO_DATE (max( x.CREATED_DT),'DD-MON-YY')-7
    From COLOSSUS_OWNER.COLOSSUS_ARC_LKP_WEEKLY x ,W_GEN_RESP_Y_MAIN_TABLE  a
    where X.CREATED_DT <=  a.EXECUTION_LAUNCH_DT
    ) and a.product_name =Product_code
    );
    commit;*/
    
    Select TABLE_NAME,TABLE_NAME||'_V' ,SUBSTR(TABLE_NAME,23,6)  
       into R_table_name, Rel_table_name ,Rel_str
    from NOVATOR_DUMMY 
    where CREATE_DT = (select MAX(CREATE_DT) from NOVATOR_DUMMY) 
    and rownum=1;
    
    Rel_table_name :=UPPER(R_table_name);
    
    Trun_Rel_col_sql:='Truncate table  RELEVANT_COLUMNS ';
    
    EXECUTE IMMEDIATE Trun_Rel_col_sql;
    
    Insert_rel_Col_sql :='Insert into  RELEVANT_COLUMNS  (
    select TABLE_NAME, stragg(COLUMN_NAME) col_list
    from DBA_TAB_COLS
    where UPPER(TABLE_NAME) ='''|| Rel_table_name||'''
    and COLUMN_NAME NOT LIKE ''%ID''
    and COLUMN_NAME NOT LIKE ''ST_%''
    and owner=''COLOSSUS_OWNER''
    group by TABLE_NAME)';
    
    execute immediate Insert_rel_Col_sql;
    
    C_LIST:='';
    
    select stragg(COLUMN_NAME)C_NAME INTO  N_DUMMY_COL_LIST from USER_TAB_COLS WHERE TABLE_NAME = 'NOVATOR_DUMMY' ;
    
    select col_list into c_list from RELEVANT_COLUMNS where TABLE_NAME =Rel_table_name;
    
    Drop_main_tbl:='drop table CAMPAIGN_COLOSSUS_WEEKLY_SNAP purge';
    
    EXECUTE IMMEDIATE Drop_main_tbl;
    
    Create_main_table_sql := 'Create table CAMPAIGN_COLOSSUS_WEEKLY_SNAP  PARTITION BY LIST (PRODUCT_NAME)
    (
    PARTITION p_Mobile VALUES (''BT Mobile''),
    PARTITION p_tv VALUES (''BT TV''),
    PARTITION p_fs VALUES (''Family Sim''),
    PARTITION p_Bs VALUES (''BT Sport''),
    PARTITION p_bb VALUES (''Broadband''),
    PARTITION p_fibre VALUES (''Fibre''),
    PARTITION p_hs VALUES (''Handsets''),
    PARTITION p_simo VALUES (''SIMO''),
    PARTITION p_gf VALUES (''G Fast''),
    PARTITION p_cb VALUES (''BB_CHURN''),
    PARTITION p_cs VALUES (''SIM_CHURN''),
    PARTITION p_ch VALUES (''HANDSET_CHURN''),
    PARTITION p_lb VALUES (''LIVE_BAC''))
    as
    (SELECT  
     '||N_DUMMY_COL_LIST||', '||C_LIST||' from NOVATOR_DUMMY a ,
    MAP_OWNER.COLOSSUS_EXTRACT_DATA c
    where 1=2)';
    
    EXECUTE IMMEDIATE Create_main_table_sql;
    
    Main_Insert_sql:=' insert  /*+ APPEND*/  into CAMPAIGN_COLOSSUS_WEEKLY_SNAP  
    SELECT  /*+  full(a) full(c) PARALLEL(a, 8)  PARALLEL(C, 8) */ 
     '||N_DUMMY_COL_LIST||', '||C_LIST||' 
    from NOVATOR_DUMMY a LEFT JOIN map_owner.COLOSSUS_EXTRACT_DATA c ON a.BILLING_ACCOUNT_KEY = c.BILL_ACCNT_KEY ';
    
    
    EXECUTE IMMEDIATE Main_Insert_sql;
    commit;
    
    
    MERGE INTO W_GEN_RESP_Y_MAIN_TABLE AB
    USING 
    (SELECT base.BILLING_ACCOUNT_KEY,base.CAMPAIGN_ID,base.product_name,TREATMENT_ID,WAVE_ID,
    CASE 
    WHEN base.X_ATTR_5 IS NULL AND base.bb_product_status = 'L' THEN base.BB_PROMOTION_GROUP_CURR 
    ELSE base.X_ATTR_5 END bb_prom_group_prev        
         FROM CAMPAIGN_COLOSSUS_WEEKLY_SNAP base
    LEFT JOIN dm_owner_str_dl.dm_promotion mob_pro
           ON base.mob_promotion_key = mob_pro.edw_product_key
              AND mob_pro.promotion_product_line = 'BT Mobile' ) stg
    ON  (stg.product_name       =ab.product_name 
    and stg.CAMPAIGN_ID        =ab.CAMPAIGN_ID
    and stg.TREATMENT_ID      =ab.TREATMENT_ID
    and stg.WAVE_ID              =ab.WAVE_ID
    and stg.BILLING_ACCOUNT_KEY=ab.BILLING_ACCOUNT_KEY)
    WHEN MATCHED THEN
            UPDATE SET X_ATTR_5 = stg.bb_prom_group_prev;
    
    COMMIT;
    
    P_RETURN_CODE:=0;
    nbo_campaign_data.nbo_audit_log(sysdate,'GET_COLOSSUS_DATA','Success');
    
    exception when others then 
    v_message:='Error:'||sqlerrm;
    nbo_campaign_data.nbo_audit_log(sysdate,'GET_COLOSSUS_DATA',v_message);
    P_RETURN_CODE:=1;
    
    END GET_COLOSSUS_DATA;
    
    PROCEDURE RANDOM_SAMPLING_DATA (Product_type In VARCHAR2,P_RETURN_CODE OUT NUMBER)
    IS 
    
    prev_count                       	NUMBER;
    post_count                       	NUMBER;
    remaining_count                     NUMBER;
    Record_count                        Number;
    VERIFIED_count                      NUMBER ;
    Random_user_count                   NUMBER ;
    small_record_count               	NUMBER ;
    MRD_INNER_QUERY                   	VARCHAR2(10000);
    MRD_INNER_QUERY2                  	VARCHAR2(10000);
    Main_insert_sql                   	VARCHAR2(10000);
    getting_count                     	VARCHAR2(10000);
    l_random                          	NUMBER (10) := 0;
    l_random1                         	NUMBER (10);
    l_random2                         	NUMBER (10);
    respective_count                	NUMBER;
    PRE_INSERT_SQL                    	CLOB;
    MRD_SQL                           	CLOB;
    least_val                        	NUMBER;
    highest_val                      	NUMBER;
    tbl_lookup_value                 	NUMBER;
    Lookup_value_1                   	NUMBER;
    final_count1                      	VARCHAR2(1000);  
    Trunc_count_tbl                   	VARCHAR2(1000):='truncate table W_GEN_RESP_Y_COUNT_TABLE' ;
    Trunc_main_count_tbl                VARCHAR2(1000):='truncate table W_GEN_RESP_Y_MAIN_COUNT_TABLE';
    Trunc_Random_tbl                  	VARCHAR2(1000):='truncate table W_GEN_RESP_Y_RANDOM_VAL_TABLE' ;
    Trunc_Temp_tbl                     	VARCHAR2(1000):='truncate table W_GEN_RESP_Y_TEMP_TABLE' ;
    Trunc_Main_tbl                      VARCHAR2(1000):='truncate table W_GEN_RESP_Y_SAMPLE_TABLE' ;
    Max_Count_in_product                NUMBER;
    Min_Count_in_product                NUMBER;
    From_date                           DATE;
    End_date                            DATE;
    Y_MAIN_MAX_DATE                   	DATE;
    Lookup_random                       NUMBER;
    begin
    l_random2 :=0;
    
    SELECT LAST_EXECUTION_DATE INTO Y_MAIN_MAX_DATE FROM NBO_EXECUTION_DATE where rownum =1;
    
    
    SELECT (CSS_WEEK_START_DATE-5), (CSS_WEEK_START_DATE+1) 
    INTO    from_date,End_date
    FROM DM_OWNER_STR_DL.dm_date 
    WHERE trunc(EDW_DATE)=TRUNC (Y_MAIN_MAX_DATE);
    
    select min (EDW_LKP_KEY),max(EDW_LKP_KEY) into Min_Count_in_product,Max_Count_in_product from X_DATA_NBO_SAMPL_RULE_LKP where PRODUCT=Product_type;
    
    
    execute immediate Trunc_count_tbl;
    execute immediate Trunc_main_count_tbl;
    /******************************** Count table population PART ****************************************/
    for Lookup_value in Min_Count_in_product..Max_Count_in_product loop
    Mrd_Inner_query := get_mrd_query(Lookup_value,Product_type,from_date,End_date);
    
    getting_count :='insert into W_GEN_RESP_Y_COUNT_TABLE('||' select * from (select '||Lookup_value||',  count(1)  from ('||Mrd_Inner_query||')  group by '||Lookup_value||'))';
    
    execute immediate getting_count;
    commit;
    end loop;
    /********************************Main count table population PART ****************************************/
    final_count1 := 'insert into W_GEN_RESP_Y_MAIN_COUNT_TABLE(RESP_LOOKUP_VALUE,LOOKUP_RECORD_COUNT,ORDERED_VALUE) select * from (select REC_COUNT,ANO_VAL,ROW_NUMBER()over ( order by ANO_VAL) val from W_GEN_RESP_Y_COUNT_TABLE )';
    execute immediate final_count1;
    commit;
    /********************************Percent count generation PART ****************************************/
    for lkp_value   in Min_Count_in_product..Max_Count_in_product loop
    select RANDOM_VALUE  into Lookup_random from X_DATA_NBO_SAMPL_RULE_LKP  where EDW_LKP_KEY=lkp_value;
    if mod(lkp_value,2)=1 then 
    update W_GEN_RESP_Y_MAIN_COUNT_TABLE set  PERCENT_COUNT =ceil(nvl ( LOOKUP_RECORD_COUNT*(Lookup_random /100),0)) where  mod(RESP_LOOKUP_VALUE,2)=1 ;
    else
    update W_GEN_RESP_Y_MAIN_COUNT_TABLE ab 
    set  PERCENT_COUNT =ceil (Lookup_random*nvl((select PERCENT_COUNT from W_GEN_RESP_Y_MAIN_COUNT_TABLE bc where bc.RESP_LOOKUP_VALUE =ab.RESP_LOOKUP_VALUE-1),0))
     where  mod(RESP_LOOKUP_VALUE,2)=0 ;
     commit;
     end if;
     end loop;
     
    /******************************** Random Value Generation part ****************************************/
    
    select sum(PERCENT_COUNT )into Random_user_count from W_GEN_RESP_Y_MAIN_COUNT_TABLE ;
    
    select count(1) into Record_count from W_GEN_RESP_Y_MAIN_TABLE;
    
    small_record_count:=ceil (Random_user_count/Max_Count_in_product)*6;
    for i in 1.. small_record_count  loop
    
    insert into W_GEN_RESP_Y_RANDOM_VAL_TABLE (random_val)
    (select A.random_val
    from (select round (random_num *101) random_val from dual) A, W_GEN_RESP_Y_RANDOM_VAL_TABLE B
    Where A.random_val=B.random_val(+)
        and B.random_val is null
        );
    exit when i=small_record_count;
    end loop;
    commit;
    for i in small_record_count.. ceil (Random_user_count*3)  loop
    
    insert into W_GEN_RESP_Y_RANDOM_VAL_TABLE (random_val)
    (select A.random_val
    from (select round (random_num *201) random_val from dual
    union all
    select round (random_num *1010) random_val from dual
    union all
    select round (random_num *10101) random_val from dual) A, W_GEN_RESP_Y_RANDOM_VAL_TABLE B
    Where A.random_val=B.random_val(+)
        and B.random_val is null
        );
    select count(1) into prev_count from W_GEN_RESP_Y_RANDOM_VAL_TABLE;
    
    exit when prev_count =ceil (Random_user_count*2.5);
    end loop;
    DELETE FROM W_GEN_RESP_Y_RANDOM_VAL_TABLE WHERE random_val >Record_count;
    DELETE FROM W_GEN_RESP_Y_RANDOM_VAL_TABLE WHERE random_val =0;
    select count(1) into post_count from W_GEN_RESP_Y_RANDOM_VAL_TABLE;
    
    commit;
    
    execute immediate Trunc_Main_tbl;
    /************************************************Random sampling Part Begins here ************************************************************/
    Pre_Insert_sql:='Insert into W_GEN_RESP_Y_SAMPLE_TABLE ( X_ATTR_25,X_ATTR_24,X_ATTR_23,X_ATTR_22,X_ATTR_21,X_ATTR_20,X_ATTR_19
    ,X_ATTR_18,X_ATTR_17,X_ATTR_16,X_ATTR_15,X_ATTR_14,X_ATTR_13,X_ATTR_12,X_ATTR_11,X_ATTR_10,X_ATTR_9,X_ATTR_8,X_ATTR_7,X_ATTR_6
    ,X_ATTR_5,X_ATTR_4,X_ATTR_3,X_ATTR_2,X_ATTR_1,OFFER_NAME,OFFER_DESCRIPTION,TREATMENT_NAME,TREATMENT_ID,EXECUTION_LAUNCH_DT
    ,MARKETING_CHANNEL,WAVE_ID,CAMPAIGN_NAME,CAMPAIGN_ID,BILLING_ACCOUNT_KEY,PRODUCT_NAME,EDW_LOOKUP_KEY,EDW_CREATE_DATE,EDW_UPDATE_DATE)  ';
    
    select min (ORDERED_VALUE),max(ORDERED_VALUE) into  least_val,highest_val  from W_GEN_RESP_Y_MAIN_COUNT_TABLE;
    
    for Lookup_value_1 in least_val..highest_val loop
    
    select RESP_LOOKUP_VALUE,LOOKUP_RECORD_COUNT,PERCENT_COUNT into tbl_lookup_value,respective_count,l_random 
    from W_GEN_RESP_Y_MAIN_COUNT_TABLE where ORDERED_VALUE=Lookup_value_1;
    
    Mrd_Inner_query2 := get_mrd_query(tbl_lookup_value,Product_type,from_date,End_date);
    
    /********************************Lesser count PART ****************************************/
    
    if respective_count <700 then
    
    Mrd_sql :='( SELECT X_ATTR_25,X_ATTR_24,X_ATTR_23,X_ATTR_22,X_ATTR_21,X_ATTR_20,CONVERSION
    ,X_ATTR_18,X_ATTR_17,X_ATTR_16,X_ATTR_15,X_ATTR_14,X_ATTR_13,X_ATTR_12,X_ATTR_11,X_ATTR_10,X_ATTR_9,X_ATTR_8,X_ATTR_7,X_ATTR_6
    ,X_ATTR_5,X_ATTR_4,X_ATTR_3,X_ATTR_2,X_ATTR_1,OFFER_NAME,OFFER_DESCRIPTION,TREATMENT_NAME,TREATMENT_ID,EXECUTION_LAUNCH_DT
    ,MARKETING_CHANNEL,WAVE_ID,CAMPAIGN_NAME,CAMPAIGN_ID,BILLING_ACCOUNT_KEY,'''||Product_type ||''','''||tbl_lookup_value ||''',sysdate
    ,sysdate FROM( '||Mrd_Inner_query2 ||' ) ab,  
    (select random_val, rownum rn from W_GEN_RESP_Y_RANDOM_VAL_TABLE,W_GEN_RESP_Y_MAIN_COUNT_TABLE where RESP_LOOKUP_VALUE ='||tbl_lookup_value||') b, W_GEN_RESP_Y_TEMP_TABLE c
    WHERE
     b.random_val <= '||respective_count || '
        and ab.row_num = b.random_val 
        and ab.BILLING_ACCOUNT_KEY =c.bac_id(+)
        and c.bac_id is null) ';
    
        Main_insert_sql:=Pre_Insert_sql||Mrd_sql;
    
    execute immediate Main_insert_sql;
    COMMIT;
    
    SELECT COUNT(1) INTO Verified_Count FROM W_GEN_RESP_Y_SAMPLE_TABLE where  EDW_LOOKUP_KEY=tbl_lookup_value;
    remaining_count :=verified_count -l_random ;
    
            if remaining_count >0 then 
            delete from W_GEN_RESP_Y_SAMPLE_TABLE where edw_lookup_key =tbl_lookup_value and  rownum < (remaining_count+1);
            Commit;
            end if;
    
    else 
    /********************************More count PART ****************************************/
    l_random1:=l_random*2;
    l_random:=nvl(l_random2,0)+l_random;
    
    Mrd_sql :='( SELECT X_ATTR_25,X_ATTR_24,X_ATTR_23,X_ATTR_22,X_ATTR_21,X_ATTR_20,CONVERSION
    ,X_ATTR_18,X_ATTR_17,X_ATTR_16,X_ATTR_15,X_ATTR_14,X_ATTR_13,X_ATTR_12,X_ATTR_11,X_ATTR_10,X_ATTR_9,X_ATTR_8,X_ATTR_7,X_ATTR_6
    ,X_ATTR_5,X_ATTR_4,X_ATTR_3,X_ATTR_2,X_ATTR_1,OFFER_NAME,OFFER_DESCRIPTION,TREATMENT_NAME,TREATMENT_ID,EXECUTION_LAUNCH_DT
    ,MARKETING_CHANNEL,WAVE_ID,CAMPAIGN_NAME,CAMPAIGN_ID,BILLING_ACCOUNT_KEY,'''||Product_type ||''','''||tbl_lookup_value ||''',sysdate
    ,sysdate FROM( '||Mrd_Inner_query2 ||' ) ab,  (select random_val, rownum rn from W_GEN_RESP_Y_RANDOM_VAL_TABLE,W_GEN_RESP_Y_MAIN_COUNT_TABLE where RESP_LOOKUP_VALUE ='||tbl_lookup_value||') b, W_GEN_RESP_Y_TEMP_TABLE c
    WHERE
        b.rn > '||l_random2 || ' and b.rn <= '||l_random || '
        and ab.row_num = b.random_val 
        and ab.BILLING_ACCOUNT_KEY =c.bac_id(+)
        and c.bac_id is null) ';
    
        Main_insert_sql:=Pre_Insert_sql||Mrd_sql;
    
    execute immediate Main_insert_sql;
    commit;
    
    SELECT COUNT(1) INTO Verified_Count FROM W_GEN_RESP_Y_SAMPLE_TABLE where  EDW_LOOKUP_KEY=tbl_lookup_value;
    remaining_count :=verified_count -l_random1 ;
            if remaining_count >0 then 
            delete from W_GEN_RESP_Y_SAMPLE_TABLE where edw_lookup_key =tbl_lookup_value and  rownum < (remaining_count+1);
            end if;
    
    l_random2:=l_random1;
    commit;
    
    end if;
    end loop;
    
    execute immediate Trunc_Random_tbl;
    execute immediate Trunc_Temp_tbl;
    
    insert into W_GEN_RESP_Y_TEMP_TABLE (select BILLING_ACCOUNT_KEY FROM W_GEN_RESP_Y_SAMPLE_TABLE);
    
    commit;
    P_RETURN_CODE:=0;
            IF P_RETURN_CODE =0 THEN 
            UPDATE NBO_EXECUTION_DATE SET LAST_EXECUTION_DATE=LAST_EXECUTION_DATE+7;
            commit;
            END IF;
    nbo_campaign_data .nbo_audit_log(sysdate,'RANDOM_SAMPLING_DATA','Success');
    exception when others then 
    v_message:='Error:'||sqlerrm;
    nbo_campaign_data .nbo_audit_log(sysdate,'RANDOM_SAMPLING_DATA',v_message);
    P_RETURN_CODE:=1;
    END RANDOM_SAMPLING_DATA;
    END NBO_COLLOSSUS_DATA;