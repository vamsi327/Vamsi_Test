CREATE OR REPLACE PROCEDURE PKG_REPERMISSIONIG_FEED IS 
    TYPE type_campaign_response IS
        TABLE OF clm_base_extract_con%rowtype;
    edw_campaign_response   type_campaign_response;
    
    output                  utl_file.file_type;
    filename                VARCHAR2(100);
    fhandle                 utl_file.file_type;
    v_file_name             VARCHAR2(2000);
    v_file_control_rec      VARCHAR2(2000);
    v_sql                   VARCHAR2(32767);
    v_partition_to_drop     VARCHAR2(30);
    v_partition_drop_sql    VARCHAR2(100);
    v_max_partition_psn     NUMBER;
    v_drop_table            VARCHAR2(150) := 'DROP TABLE CLM_BASE_EXTRACT_CON';
    v_procedure_name        VARCHAR2(30) := 'PKG_REPERMISSIONIG_FEED';
    v_table_name            VARCHAR2(32);
    v_schema_name           VARCHAR2(30);
    v_row_affected          NUMBER := 0;
    v_success_message       VARCHAR2(1000);
    v_err_msg               VARCHAR2(4000);
    v_success               CONSTANT VARCHAR2(10) := 'SUCCESS';
    v_failure               CONSTANT VARCHAR2(10) := 'FAILED';
    v_start_date            DATE;
    v_end_date              DATE;
begin

/*****************************Creation of feed table Part ********************************/
BEGIN
v_table_name:='CLM_BASE_EXTRACT_CON';

EXECUTE IMMEDIATE V_DROP_TABLE;
ALTER SESSION SET NLS_DATE_FORMAT = 'DD-MON-YYYY HH24:MI:SS';

V_SQL :='create table CLM_BASE_EXTRACT_CON as
select /*+ parallel(a,8) */
Cast(NULL AS Varchar2(10)) AS	ACXIOM_URN	,
Cast(NULL AS Varchar2(10)) AS	ACXIOM_AKEY	,
Cast(NULL AS Varchar2(10)) AS	ACXIOM_IKEY	,
Cast(NULL AS Varchar2(10)) AS	URN	,
Cast(NULL AS Varchar2(10)) AS	TITLE	,
Cast(NULL AS Varchar2(10)) AS	FIRST_NAME	,
Cast(NULL AS Varchar2(10)) AS	SURNAME	,
''TM_REPERMISSION'' as	BT_ACTIVITY	,
Cast(NULL AS Varchar2(10)) AS	ADDRESS_FIELD_1	,
Cast(NULL AS Varchar2(10)) AS	ADDRESS_FIELD_2	,
Cast(NULL AS Varchar2(10)) AS	ADDRESS_FIELD_3	,
Cast(NULL AS Varchar2(10)) AS	ADDRESS_FIELD_4	,
Cast(NULL AS Varchar2(10)) AS	ADDRESS_FIELD_5	,
Cast(NULL AS Varchar2(10)) AS	POSTCODE	,
Cast(NULL AS Varchar2(10)) AS	SUPPLIER_NAME	,
Cast(NULL AS Varchar2(10)) AS	SUPLIER_CODE	,
a.CONTACT_KEY AS	CONTACT_KEY	,
a.ADDRESS_KEY AS	ADDRESS_KEY	,
a.TELEPHONE_NO AS	TELEPHONE_NUMBER	,
Cast(NULL AS Varchar2(10)) AS	EMAIL_FLAG	,
Cast(NULL AS Varchar2(10)) AS	EMAIL_ADDRESS	,
Cast(NULL AS Varchar2(10)) AS	COMPETITOR_1	,
Cast(NULL AS Varchar2(10)) AS	COMPETITOR_2	,
Cast(NULL AS Varchar2(10)) AS	COMPETITOR_3	,
Cast(NULL AS Varchar2(10)) AS	EXPIRY_DATE	,
Cast(NULL AS Varchar2(10)) AS	LICENCED_USAGE	,
Cast(NULL AS Varchar2(10)) AS	NUMBER_OF_USES	,
a.CALL_CONSENT_VALIDATE_FLG AS	SPARE_1	,
a.CALL_CONSENT_OUTCOME_CD AS	SPARE_2	,
Cast(NULL AS Varchar2(10)) AS	SPARE_3	,
Cast(NULL AS Varchar2(10)) AS	SPARE_DATE_1,
SYSDATE AS EDW_UPDATE_DATE ,
SYSDATE AS EDW_CREATED_DATE  
from int_owner_il.L_EDW_CON_CAMPAIGN_response a
where CALL_CONSENT_OUTCOME_CD = ''OC080''';

v_start_date := SYSDATE;
EXECUTE IMMEDIATE V_SQL;
v_end_date   := sysdate;
v_success_message:='Table created Successfully';
    INSERT INTO OPS$EDWPROD.A_TAB_GENERIC_PROC_AUDIT (ID,
													  TABLE_NAME,
													  PROCEDURE_NAME,
													  STATUS,
													  START_DATE,
													  END_DATE,
													  NUMBER_OF_RECORDS,
													  STEP)
           VALUES (OPS$EDWPROD.SEQ_GENERIC_PROC_AUDIT.NEXTVAL,
                   v_table_name,
                   v_procedure_name,
                   v_success,
                   v_start_date,
                   v_end_date,
                   NULL,
                   v_success_message);
				   
	   COMMIT;
    EXCEPTION
      WHEN OTHERS THEN
        raise;
    END;
/*****************************Backup of Feed Part ********************************/
BEGIN
v_start_date := SYSDATE;
v_table_name:='CLM_BASE_EXTRACT_CON_H';
INSERT INTO CLM_BASE_EXTRACT_CON_H  SELECT * FROM CLM_BASE_EXTRACT_CON;
v_row_affected     := SQL%ROWCOUNT;
COMMIT;
v_end_date   := sysdate;
v_success_message:='Data laoded into History table Successfully';
    INSERT INTO OPS$EDWPROD.A_TAB_GENERIC_PROC_AUDIT (ID,
													  TABLE_NAME,
													  PROCEDURE_NAME,
													  STATUS,
													  START_DATE,
													  END_DATE,
													  NUMBER_OF_RECORDS,
													  STEP)
           VALUES (OPS$EDWPROD.SEQ_GENERIC_PROC_AUDIT.NEXTVAL,
                   v_table_name,
                   v_procedure_name,
                   v_success,
                   v_start_date,
                   v_end_date,
                   v_row_affected,
                   v_success_message);
				   
	   COMMIT;
    EXCEPTION
      WHEN OTHERS THEN
        raise;
    END;
/***************************** Writing a file into server part ********************************/
BEGIN
v_table_name:='REPERMISSIONED';
v_start_date := SYSDATE;
select * bulk   collect into edw_campaign_response from   CLM_BASE_EXTRACT_CON ;
v_row_affected :=edw_campaign_response.count;
V_FILE_NAME := 'EIP_EX00000_OBREPERMISSIONED_DNH_1_1_' || sysdate ||'.dat';

fHandle := UTL_FILE.FOPEN('ACXIOM_TM_REPERMISSION_DIR',V_FILE_NAME , 'w');
V_FILE_CONTROL_REC:= 'ACXIOM_URN,ACXIOM_AKEY,ACXIOM_IKEY,URN,TITLE,FIRST_NAME,SURNAME,BT_ACTIVITY,ADDRESS_FIELD_1,ADDRESS_FIELD_2,ADDRESS_FIELD_3,ADDRESS_FIELD_4,ADDRESS_FIELD_5,POSTCODE,SUPPLIER_NAME,SUPLIER_CODE,CONTACT_KEY,ADDRESS_KEY,TELEPHONE_NUMBER,EMAIL_FLAG,EMAIL_ADDRESS,COMPETITOR_1,COMPETITOR_2,COMPETITOR_3,EXPIRY_DATE,LICENCED_USAGE,NUMBER_OF_USES,SPARE_1,SPARE_2,SPARE_3,SPARE_DATE_1';  
UTL_FILE.PUT_LINE(fHandle, V_FILE_CONTROL_REC);
 for i in 1 .. edw_campaign_response.count loop
  
  utl_file.put_line ( fHandle,edw_campaign_response(i).ACXIOM_URN||','||
edw_campaign_response(i).ACXIOM_AKEY||','||
edw_campaign_response(i).ACXIOM_IKEY||','||
edw_campaign_response(i).URN||','||
edw_campaign_response(i).TITLE||','||
edw_campaign_response(i).FIRST_NAME||','||
edw_campaign_response(i).SURNAME||','||
edw_campaign_response(i).BT_ACTIVITY||','||
edw_campaign_response(i).ADDRESS_FIELD_1||','||
edw_campaign_response(i).ADDRESS_FIELD_2||','||
edw_campaign_response(i).ADDRESS_FIELD_3||','||
edw_campaign_response(i).ADDRESS_FIELD_4||','||
edw_campaign_response(i).ADDRESS_FIELD_5||','||
edw_campaign_response(i).POSTCODE||','||
edw_campaign_response(i).SUPPLIER_NAME||','||
edw_campaign_response(i).SUPLIER_CODE||','||
edw_campaign_response(i).CONTACT_KEY||','||
edw_campaign_response(i).ADDRESS_KEY||','||
edw_campaign_response(i).TELEPHONE_NUMBER||','||
edw_campaign_response(i).EMAIL_FLAG||','||
edw_campaign_response(i).EMAIL_ADDRESS||','||
edw_campaign_response(i).COMPETITOR_1||','||
edw_campaign_response(i).COMPETITOR_2||','||
edw_campaign_response(i).COMPETITOR_3||','||
edw_campaign_response(i).EXPIRY_DATE||','||
edw_campaign_response(i).LICENCED_USAGE||','||
edw_campaign_response(i).NUMBER_OF_USES||','||
edw_campaign_response(i).SPARE_1||','||
edw_campaign_response(i).SPARE_2||','||
edw_campaign_response(i).SPARE_3||','||
edw_campaign_response(i).SPARE_DATE_1 );
  end loop;
  UTL_FILE.FCLOSE(fHandle);
 v_end_date   := sysdate; 
  v_success_message:='Data written into '||V_FILE_NAME||' file Successfully';
     INSERT INTO OPS$EDWPROD.A_TAB_GENERIC_PROC_AUDIT (ID,
													  TABLE_NAME,
													  PROCEDURE_NAME,
													  STATUS,
													  START_DATE,
													  END_DATE,
													  NUMBER_OF_RECORDS,
													  STEP)
           VALUES (OPS$EDWPROD.SEQ_GENERIC_PROC_AUDIT.NEXTVAL,
                   'REPERMISSIONED',
                   v_procedure_name,
                   v_success,
                   v_start_date,
                   v_end_date,
                   v_row_affected,
                   v_success_message);
				   
	   COMMIT;
    EXCEPTION
      WHEN OTHERS THEN
        raise;
    END;
/*****************************House keeping Part ********************************/
BEGIN
SELECT MAX(PARTITION_POSITION) INTO V_MAX_PARTITION_PSN 
      FROM ALL_TAB_PARTITIONS AB
      WHERE TABLE_NAME = 'CLM_BASE_EXTRACT_CON_H'
	  AND TABLE_OWNER = 'EDW_LOAD_STAGE';
      
SELECT PARTITION_NAME INTO V_PARTITION_TO_DROP
      FROM ALL_TAB_PARTITIONS AB
      WHERE TABLE_NAME = 'CLM_BASE_EXTRACT_CON_H'
	  AND TABLE_OWNER = 'EDW_LOAD_STAGE'
      AND PARTITION_POSITION =2;

V_PARTITION_DROP_SQL :='ALTER TABLE EDW_LOAD_STAGE.CLM_BASE_EXTRACT_CON_H DROP PARTITION '|| V_PARTITION_TO_DROP ||' UPDATE GLOBAL INDEXES';     

IF V_MAX_PARTITION_PSN >3 THEN 
EXECUTE IMMEDIATE V_PARTITION_DROP_SQL;
v_success_message:='Partition position is '||V_MAX_PARTITION_PSN||' , '||V_PARTITION_TO_DROP||' dropped Successfully';	
     INSERT INTO OPS$EDWPROD.A_TAB_GENERIC_PROC_AUDIT (ID,
													  TABLE_NAME,
													  PROCEDURE_NAME,
													  STATUS,
													  START_DATE,
													  END_DATE,
													  NUMBER_OF_RECORDS,
													  STEP)
           VALUES (OPS$EDWPROD.SEQ_GENERIC_PROC_AUDIT.NEXTVAL,
                   'CLM_BASE_EXTRACT_CON_H',
                   v_procedure_name,
                   v_success,
                   v_start_date,
                   v_end_date,
                   v_row_affected,
                   v_success_message);
	v_end_date   := sysdate; 
  		   
	   COMMIT;
else
v_success_message:='No Partitions Dropped';
    INSERT INTO OPS$EDWPROD.A_TAB_GENERIC_PROC_AUDIT (ID,
													  TABLE_NAME,
													  PROCEDURE_NAME,
													  STATUS,
													  START_DATE,
													  END_DATE,
													  NUMBER_OF_RECORDS,
													  STEP)
           VALUES (OPS$EDWPROD.SEQ_GENERIC_PROC_AUDIT.NEXTVAL,
                   'CLM_BASE_EXTRACT_CON_H',
                   v_procedure_name,
                   v_success,
                   v_start_date,
                   v_end_date,
                   v_row_affected,
                   v_success_message);
	v_end_date   := sysdate; 
	   COMMIT;
       END IF;
    EXCEPTION
      WHEN OTHERS THEN
        raise;
    END;
/*****************************Exception Part ********************************/
  EXCEPTION
  WHEN OTHERS THEN
    ROLLBACK;
    v_end_date := sysdate;
    v_err_msg := SUBSTR ( SQLCODE || ' : ' || SQLERRM || ' : ' || DBMS_UTILITY.format_error_backtrace, 1, 2000);
   										
	INSERT INTO OPS$EDWPROD.A_TAB_GENERIC_PROC_AUDIT (ID,
													  TABLE_NAME,
													  PROCEDURE_NAME,
													  STATUS,
													  START_DATE,
													  END_DATE,
													  NUMBER_OF_RECORDS,
													  STEP)
           VALUES (OPS$EDWPROD.SEQ_GENERIC_PROC_AUDIT.NEXTVAL,
                   v_table_name,
                   v_procedure_name,
                   v_failure,
                   v_start_date,
                   v_end_date,
                   v_row_affected,
                   v_err_msg);	
    COMMIT;
	
END PKG_REPERMISSIONIG_FEED;
/

EXEC PKG_REPERMISSIONIG_FEED