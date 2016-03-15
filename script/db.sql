CREATE OR REPLACE TYPE typ_dq_tbl_vc2 as table of varchar2(4000);
/

CREATE OR REPLACE TYPE typ_dq_tbl_tbl_vc2 as table of typ_dq_tbl_vc2;
/

/**
   * Object parameters.
   *
*/
CREATE OR REPLACE TYPE "DQ_TYP_PARAMS" AS OBJECT (
   id integer,
   label    VARCHAR2(255),
   name    VARCHAR2(255),
   original_text VARCHAR2(255),
   bool_yes    VARCHAR2(255),
   bool_no    VARCHAR2(255)
 );
/

/**
   * Object columns.
   *
*/
CREATE OR REPLACE TYPE "DQ_TYP_COLUMNS" AS OBJECT (
   id integer,
   name    VARCHAR2(255)
 );
/


CREATE OR REPLACE TYPE "DQ_TBL_PARAMS" IS TABLE OF dq_typ_params;
/


CREATE OR REPLACE TYPE "DQ_TBL_COLUMNS" IS TABLE OF dq_typ_columns;
/




CREATE TABLE "DQ_GROUP_QUERIES" 
   (	"GROUP_QUERY_ID" NUMBER(*,0) NOT NULL ENABLE, 
	"GROUP_QUERY_PARENT_ID" NUMBER(*,0) NOT NULL ENABLE, 
	"GROUP_QUERY_NAME" VARCHAR2(70) NOT NULL ENABLE, 
	"GROUP_QUERY_NUMBER" NUMBER(*,0) NOT NULL ENABLE
   ) ;

CREATE TABLE "DQ_QUERIES" 
   (	"QUERY_ID" NUMBER(*,0) NOT NULL ENABLE, 
	"GROUP_QUERY_ID" NUMBER(*,0) NOT NULL ENABLE, 
	"QUERY_NAME" VARCHAR2(70) NOT NULL ENABLE, 
	"QUERY_COMMENT" VARCHAR2(2000), 
	"QUERY_SQL" CLOB, 
	"QUERY_NUMBER" NUMBER(*,0) NOT NULL ENABLE, 
	"QUERY_RESULT_FORMAT_ID" VARCHAR2(100), 
	"QUERY_SQL_DB" CLOB, 
	"QUERY_COLUMNS" "DQ_TBL_COLUMNS" , 
	"QUERY_PARAMS" "DQ_TBL_PARAMS" , 
	"QUERY_PATTERN" BLOB
   ) 
 NESTED TABLE "QUERY_COLUMNS" STORE AS "NESTED_QUERY_COLUMNS"
 RETURN AS VALUE
 NESTED TABLE "QUERY_PARAMS" STORE AS "NESTED_QUERY_PARAMS"
 RETURN AS VALUE;

CREATE TABLE "DQ_QUERY_RESULT_FORMATS" 
   (	"QUERY_RESULT_FORMAT_ID" VARCHAR2(100), 
	"QUERY_RESULT_FORMAT_NAME" VARCHAR2(255)
   ) ;

CREATE TABLE "DQ_QUERY_STATISTICS" 
   (	"QUERY_STATISTICS_USER_LOGIN" VARCHAR2(255), 
	"QUERY_ID" NUMBER(*,0), 
	"QUERY_STATISTICS_RUN_COUNT" NUMBER(*,0) DEFAULT 0
   ) ;

CREATE TABLE DQ_QUERY_PARAMS
(
  QUERY_PARAM_NAME     VARCHAR2(255 BYTE),
  QUERY_PARAM_CAPTION  VARCHAR2(255 BYTE),
  QUERY_PARAM_QRY      CLOB
);

   
   
   
 CREATE SEQUENCE  "SEQ_DQ_GROUP_QUERIES"  MINVALUE 1 MAXVALUE 9999999999999999999999999999 INCREMENT BY 1 START WITH 5 NOCACHE  NOORDER  NOCYCLE ;

 CREATE SEQUENCE  "SEQ_DQ_QUERIES"  MINVALUE 1 MAXVALUE 9999999999999999999999999999 INCREMENT BY 1 START WITH 50 NOCACHE  NOORDER  NOCYCLE ;

 
CREATE OR REPLACE PACKAGE utl
AS
   FUNCTION isEqual (a IN NUMBER, b IN NUMBER)
      RETURN BOOLEAN;

   FUNCTION isEqual (a IN VARCHAR2, b IN VARCHAR2)
      RETURN BOOLEAN;

   FUNCTION isEqual (a IN DATE, b IN DATE)
      RETURN BOOLEAN;

   FUNCTION blob_to_clob (blob_in IN BLOB) RETURN CLOB;
   
    end;
/

CREATE OR REPLACE PACKAGE BODY utl
AS
   FUNCTION isEqual (a IN NUMBER, b IN NUMBER)
      RETURN BOOLEAN
   IS
   BEGIN
      RETURN    (a IS NOT NULL AND b IS NOT NULL AND a = b)
             OR (a IS NULL AND b IS NULL);
   END;

   FUNCTION isEqual (a IN VARCHAR2, b IN VARCHAR2)
      RETURN BOOLEAN
   IS
   BEGIN
      RETURN    (a IS NOT NULL AND b IS NOT NULL AND a = b)
             OR (a IS NULL AND b IS NULL);
   END;

   FUNCTION isEqual (a IN DATE, b IN DATE)
      RETURN BOOLEAN
   IS
   BEGIN
      RETURN    (a IS NOT NULL AND b IS NOT NULL AND a = b)
             OR (a IS NULL AND b IS NULL);
   END;



   FUNCTION blob_to_clob (blob_in IN BLOB)
      RETURN CLOB
   AS
      v_clob      CLOB;
      v_varchar   VARCHAR2 (32767);
      v_start     PLS_INTEGER := 1;
      v_buffer    PLS_INTEGER := 32767;
   BEGIN
      DBMS_LOB.CREATETEMPORARY (v_clob, TRUE);

      FOR i IN 1 .. CEIL (DBMS_LOB.GETLENGTH (blob_in) / v_buffer)
      LOOP
         v_varchar :=
            UTL_RAW.CAST_TO_VARCHAR2 (
               DBMS_LOB.SUBSTR (blob_in, v_buffer, v_start));

         DBMS_LOB.WRITEAPPEND (v_clob, LENGTH (v_varchar), v_varchar);
         v_start :=
            v_start +
            v_buffer;
      END LOOP;

      RETURN v_clob;
   END blob_to_clob;
END;
/ 
 

CREATE OR REPLACE PACKAGE DQ
AS
   /**
   * Project:         DynamiQ (<a href="https://github.com/VladMl/DynamiQ</a>)<br/>
   * Description:     User definad query management for Oracle APEX<br/>
   * DB impact:       YES<br/>
   * Commit inside:   NO<br/>
   * Rollback inside: NO<br/>
   * @headcom
   */


   /**
   * Record of query data.
   *
   * @param parameters parameters list
   * @param text  query text
   */
   TYPE t_query IS RECORD
   (
      parameters dq_tbl_params,
      text VARCHAR2 (32767 BYTE)
   );



   /**
   * Get query data
   *
   * @param pi_query_id                       Query id  dq_queries.query_id
   * @param pv_values                         List of params delimiter |
   * @param pv_query_result_format_id         Output format
   * @return                                  result Blob
   */
   FUNCTION Get_Query_Result (pi_query_id IN INTEGER,
                              pv_values IN VARCHAR2,
                              pv_query_result_format_id VARCHAR2)
      RETURN BLOB;

   /**
   * Prepare collection for IR
   *
   * @param pi_query_id       Query id  dq_queries.query_id
   * @param pv_query_result_format_id         Output format -- not used in this procedure
   * @param pv_values         List of params delimiter |
   */
   PROCEDURE Get_Query_Result_Ir (
      pi_query_id IN DQ_QUERIES.QUERY_ID%TYPE,
      pv_values IN VARCHAR2,
      pv_query_result_format_id DQ_QUERIES.QUERY_RESULT_FORMAT_ID%TYPE);



   /**
   * Parse query on Insert or Update. Save columns and parameters
   *
   * @param pi_query_id       Query id  dq_queries.query_id
   */
   PROCEDURE Parse_Query (pv_query_id NUMBER);



END;
/
 


 
 CREATE OR REPLACE PACKAGE BODY DQ
/**
* Project:         DynamiQ (<a href="https://github.com/VladMl/DynamiQ</a>)<br/>
* Description:     User definad query management for Oracle APEX<br/>
* DB impact:       YES<br/>
* Commit inside:   NO<br/>
* Rollback inside: NO<br/>
* @headcom
*/
AS
   /**
   * Parse query text and get parameters and modified text.
   *
   * @param pi_query_id       Query id dq_queries.query_id
   * @return          list of parameters, modified query text t_query
   */
   FUNCTION Get_Parameters_From_Text (
      pi_query_id IN DQ_QUERIES.QUERY_ID%TYPE)
      RETURN t_query
   IS
      lc_qry CLOB;
      li_length PLS_INTEGER;
      lv_curr_param VARCHAR2 (4000);
      lv_curr_param_name VARCHAR2 (4000);
      li_start_param PLS_INTEGER;
      li_end_param PLS_INTEGER;
      lt_query t_query;

      ltbl_parameters dq_tbl_params;

      li_count_params INTEGER;
   BEGIN
      SELECT query_sql
        INTO lc_qry
        FROM dq_queries
       WHERE query_id = pi_query_id;


      ltbl_parameters := dq_tbl_params ();

      li_length := DBMS_LOB.getlength (lc_qry);

      FOR i IN 1 .. li_length
      LOOP
         IF SUBSTR (lc_qry, i, 1) = '['
         THEN
            li_start_param := i;
         END IF;


         IF SUBSTR (lc_qry, i, 1) = ']'
         THEN
            li_end_param := i;
            lv_curr_param :=
               SUBSTR (lc_qry,
                       li_start_param + 1,
                       li_end_param - li_start_param - 1);
            lv_curr_param_name :=
               SUBSTR (lv_curr_param, 1, INSTR (lv_curr_param, ':') - 1);



            SELECT COUNT (*)
              INTO li_count_params
              FROM TABLE (ltbl_parameters)
             WHERE name = lv_curr_param_name;


            IF li_count_params = 0
            THEN
               ltbl_parameters.EXTEND;
               ltbl_parameters (ltbl_parameters.COUNT) :=
                  dq_typ_params (
                     1,
                     SUBSTR (lv_curr_param,
                             INSTR (lv_curr_param, ':') + 1,
                             LENGTH (lc_qry)),
                     lv_curr_param_name,
                     SUBSTR (lc_qry,
                             li_start_param,
                             li_end_param + 1 - li_start_param),
                     '',
                     '');
            END IF;
         END IF;
      END LOOP;



      FOR indx IN 1 .. ltbl_parameters.COUNT
      LOOP
         lc_qry :=
            REPLACE (lc_qry,
                     ltbl_parameters (indx).original_text,
                     ':' || ltbl_parameters (indx).name);
      END LOOP;



      lt_query.parameters := ltbl_parameters;
      lt_query.text := lc_qry;

      RETURN lt_query;
   END Get_Parameters_From_Text;

   /**
   * Replace template value for Excel (XML) file. Mask ## , #!
   *
   * @param pi_query_id       Query id dq_queries.query_id
   * @pv_source_str           Source string
   * @ptbl_values             Values collection
   * @return          list of parameters, modified query text t_query
   */
   FUNCTION Replace_Template_Value (pi_query_id DQ_QUERIES.QUERY_ID%TYPE,
                                    pv_source_str VARCHAR2,
                                    ptbl_values typ_dq_tbl_vc2)
      RETURN VARCHAR2
   IS
      li_pos_mark_symb NUMBER;
      li_column_id NUMBER;
      lv_ret_str VARCHAR2 (32000);
   BEGIN
      lv_ret_str := pv_source_str;

      li_pos_mark_symb := INSTR (pv_source_str, '##');

      IF li_pos_mark_symb = 0
      THEN
         li_pos_mark_symb := INSTR (pv_source_str, '#!');
      END IF;

      IF li_pos_mark_symb > 0
      THEN
         BEGIN
            SELECT id
              INTO li_column_id
              FROM dq_queries, TABLE (dq_queries.query_columns)
             WHERE     query_id = pi_query_id
                   AND name =
                          SUBSTR (
                             pv_source_str,
                             li_pos_mark_symb + 2,
                             INSTR (pv_source_str, '<', li_pos_mark_symb) -
                             li_pos_mark_symb - 2);

            lv_ret_str :=
               REPLACE (
                  pv_source_str,
                  SUBSTR (pv_source_str,
                          li_pos_mark_symb,
                          INSTR (pv_source_str, '<', li_pos_mark_symb) -
                          li_pos_mark_symb),
                  ptbl_values (li_column_id));
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               NULL;
         END;
      END IF;

      RETURN lv_ret_str;
   END;


   /**
   * Get XLS  for Excel (XML) data
   *
   * @param pi_query_id       Query id  dq_queries.query_id
   * @param ptbl_data_set           Query dataset dq_queries.query_id
   * @return                  XLS Blob
   */
   FUNCTION Get_XLS (pi_query_id IN DQ_QUERIES.QUERY_ID%TYPE,
                     ptbl_data_set typ_dq_tbl_tbl_vc2)
      RETURN BLOB
   IS
      lb_pattern BLOB;
      lc_pattern CLOB;
      lb_result BLOB;
      lv_cur_str VARCHAR2 (32000);
      lv_cur_row_str VARCHAR2 (32000);
      li_line_count PLS_INTEGER;
      li_pos_row_symb INTEGER;
      li_pos_expanded_row INTEGER;
      li_data_row_index INTEGER := 0;
      li_row_index INTEGER := 0;
      lv_row_first VARCHAR2 (32000);
      lv_row_last VARCHAR2 (32000);
      ltbl_row APEX_APPLICATION_GLOBAL.VC_ARR2;
   BEGIN
      -- Get pattern
      SELECT query_pattern
        INTO lb_pattern
        FROM dq_queries
       WHERE query_id = pi_query_id;

      DBMS_LOB.createtemporary (lb_result, TRUE);
      DBMS_LOB.open (lb_result, DBMS_LOB.lob_readwrite);

      IF lb_pattern IS NOT NULL
      THEN
         -- Convert BLOB to CLOB
         lc_pattern := utl.blob_to_clob (lb_pattern);

         -- Read CLOB
         li_line_count :=
            (LENGTH (REGEXP_REPLACE (lc_pattern,
             '^.*',
             NULL,
             1,
             0,
             'm')) + 1);

         FOR i IN 1 .. li_line_count
         LOOP
            lv_cur_str :=
               REGEXP_SUBSTR (lc_pattern,
                              '^.*',
                              1,
                              i,
                              'm');

            li_pos_expanded_row := INSTR (lv_cur_str, 'ss:ExpandedRowCount');

            IF li_pos_expanded_row > 0
            THEN
               lv_cur_str :=
                  REPLACE (
                     lv_cur_str,
                     SUBSTR (
                        lv_cur_str,
                        li_pos_expanded_row,
                        INSTR (lv_cur_str,
                        '"',
                        INSTR  (lv_cur_str, '"', li_pos_expanded_row) + 1) -
                        li_pos_expanded_row + 1),
                     '');
            END IF;

            IF INSTR (lv_cur_str, '<Row') > 0
            THEN
               lv_row_first := lv_cur_str;
            END IF;

            IF INSTR (lv_cur_str, '</Row>') > 0 AND ltbl_row.COUNT > 0
            THEN
               lv_row_last := lv_cur_str;
               li_data_row_index := li_data_row_index + 1;

               DBMS_LOB.append (lb_result,
                                UTL_RAW.cast_to_raw (lv_cur_str || CHR (10)));


               FOR i IN 2 .. ptbl_data_set.COUNT
               LOOP
                  DBMS_LOB.append (
                     lb_result,
                     UTL_RAW.cast_to_raw (lv_row_first || CHR (10)));

                  FOR j IN 1 .. ltbl_row.COUNT
                  LOOP
                     lv_cur_row_str :=
                        Replace_Template_Value (pi_query_id,
                                                ltbl_row (j),
                                                ptbl_data_set (i));
                     DBMS_LOB.append (
                        lb_result,
                        UTL_RAW.cast_to_raw (lv_cur_row_str || CHR (10)));
                  END LOOP;

                  DBMS_LOB.append (
                     lb_result,
                     UTL_RAW.cast_to_raw (lv_row_last || CHR (10)));
               END LOOP;

               CONTINUE;
            END IF;


            li_pos_row_symb := INSTR (lv_cur_str, '#!');

            IF li_pos_row_symb > 0
            THEN
               li_row_index := li_row_index + 1;
               ltbl_row (li_row_index) := lv_cur_str;

               lv_cur_str :=
                  Replace_Template_Value (pi_query_id,
                                          lv_cur_str,
                                          ptbl_data_set (1));
            ELSE
               lv_cur_str :=
                  Replace_Template_Value (pi_query_id,
                                          lv_cur_str,
                                          ptbl_data_set (1));
            END IF;

            DBMS_LOB.append (lb_result,
                             UTL_RAW.cast_to_raw (lv_cur_str || CHR (10)));
         END LOOP;
      END IF;

      RETURN lb_result;
   END;

   /**
   * Get plain  for Word (XML), Text data
   *
   * @param pi_query_id       Query id  dq_queries.query_id
   * @param ptbl_data_set           Query dataset dq_queries.query_id
   * @return                  XLS Blob
   */
   FUNCTION Get_Plain (pi_query_id IN DQ_QUERIES.QUERY_ID%TYPE,
                       ptbl_data_set typ_dq_tbl_tbl_vc2)
      RETURN BLOB
   IS
      lb_pattern BLOB;
      lc_pattern CLOB;
      lb_result BLOB;
      dest_offset INTEGER;
      src_offset INTEGER;
      lang_context INTEGER;
      warning VARCHAR2 (1000);
   BEGIN
      -- Get pattern
      SELECT query_pattern
        INTO lb_pattern
        FROM dq_queries
       WHERE query_id = pi_query_id;


      DBMS_LOB.createtemporary (lb_result, TRUE);
      DBMS_LOB.open (lb_result, DBMS_LOB.lob_readwrite);

      IF lb_pattern IS NOT NULL
      THEN
         -- Convert BLOB to CLOB
         lc_pattern := utl.blob_to_clob (lb_pattern);


         FOR cur IN (SELECT id, name
                       FROM dq_queries, TABLE (dq_queries.query_columns)
                      WHERE query_id = pi_query_id)
         LOOP
            lc_pattern :=
               REPLACE (lc_pattern,
                        '##' || cur.name,
                        TO_CHAR (ptbl_data_set (1) (cur.id)));
         END LOOP;


         dest_offset := 1;
         src_offset := 1;
         lang_context := 0;
         DBMS_LOB.converttoblob (lb_result,
                                 lc_pattern,
                                 DBMS_LOB.getlength (lc_pattern),
                                 dest_offset,
                                 src_offset,
                                 0,
                                 lang_context,
                                 warning);
      END IF;

      RETURN lb_result;
   END;

   /**
   * Get query data
   *
   * @param pi_query_id                       Query id  dq_queries.query_id
   * @param pv_values                         List of params delimiter |
   * @param pv_query_result_format_id         Output format
   * @return                                  result Blob
   */

   FUNCTION Get_Query_Result (pi_query_id IN INTEGER,
                              pv_values IN VARCHAR2,
                              pv_query_result_format_id VARCHAR2)
      RETURN BLOB
   IS
      TYPE typ_cur_qry IS REF CURSOR;

      lt_cur_qry typ_cur_qry;


      ln_cur_cols NUMBER;
      ln_cur_qry NUMBER;
      ln_cur_qry_exe NUMBER;

      l_columns_desc DBMS_SQL.desc_tab;
      ln_column_cnt NUMBER;
      lv_stmt_txt VARCHAR2 (4000);
      lv_header_str VARCHAR2 (32000);
      lv_cur_str VARCHAR2 (32000);
      ltbl_result typ_dq_tbl_tbl_vc2;
      lt_params wwv_flow_global.vc_arr2;
      lt_values wwv_flow_global.vc_arr2;
      lb_result BLOB;

      lc_query_sql CLOB;
   BEGIN
      SELECT name
        BULK COLLECT INTO lt_params
        FROM dq_queries, TABLE (dq_queries.query_params)
       WHERE query_id = pi_query_id;

      SELECT query_sql_db
        INTO lc_query_sql
        FROM dq_queries
       WHERE query_id = pi_query_id;



      lt_values := APEX_UTIL.string_to_table (pv_values, '|');

      ln_cur_cols := DBMS_SQL.open_cursor;

      DBMS_SQL.parse (ln_cur_cols, lc_query_sql, DBMS_SQL.native);
      DBMS_SQL.describe_columns (ln_cur_cols, ln_column_cnt, l_columns_desc);
      DBMS_SQL.close_cursor (ln_cur_cols);


      lv_stmt_txt := 'select typ_dq_tbl_vc2(';

      FOR i IN 1 .. ln_column_cnt
      LOOP
         lv_header_str :=
               lv_header_str
            || REPLACE (l_columns_desc (i).col_name, '_', ' ')
            || ';';

         IF i != 1
         THEN
            lv_stmt_txt := lv_stmt_txt || ',';
         END IF;

         lv_stmt_txt := lv_stmt_txt || l_columns_desc (i).col_name;
      END LOOP;

      lv_stmt_txt := lv_stmt_txt || ') from ' || '(' || lc_query_sql || ')';


      ln_cur_qry := DBMS_SQL.OPEN_CURSOR;

      DBMS_SQL.PARSE (ln_cur_qry, lv_stmt_txt, DBMS_SQL.NATIVE);

      FOR iParams IN 1 .. lt_params.COUNT
      LOOP
         DBMS_SQL.bind_variable (ln_cur_qry,
                                 TRIM (lt_params (iParams)),
                                 TRIM (lt_values (iParams)));
      END LOOP;


      ln_cur_qry_exe := DBMS_SQL.EXECUTE (ln_cur_qry);

      lt_cur_qry := DBMS_SQL.TO_REFCURSOR (ln_cur_qry);

      FETCH lt_cur_qry BULK COLLECT INTO ltbl_result;

      -- CSV
      IF pv_query_result_format_id = 'CSV'
      THEN
         DBMS_LOB.createtemporary (lb_result, TRUE);
         DBMS_LOB.open (lb_result, DBMS_LOB.lob_readwrite);


         DBMS_LOB.append (lb_result,
                          UTL_RAW.cast_to_raw (lv_header_str || CHR (10)));

         FOR i IN 1 .. ltbl_result.COUNT
         LOOP
            lv_cur_str := '';

            FOR j IN 1 .. ltbl_result (i).COUNT
            LOOP
               lv_cur_str := lv_cur_str || ltbl_result (i) (j) || ';';
            END LOOP;


            DBMS_LOB.append (lb_result,
                             UTL_RAW.cast_to_raw (lv_cur_str || CHR (10)));

            NULL;
         END LOOP;
      END IF;



      IF    pv_query_result_format_id = 'XLS'
         OR pv_query_result_format_id = 'XHTML'
      THEN
         lb_result := Get_XLS (pi_query_id, ltbl_result);
      END IF;

      IF    pv_query_result_format_id = 'DOC'
         OR pv_query_result_format_id = 'WHTML'
      THEN
         lb_result := Get_Plain (pi_query_id, ltbl_result);
      END IF;

      CLOSE lt_cur_qry;

      MERGE INTO dq_query_statistics dqs
           USING DUAL
              ON (    dqs.query_statistics_user_login = v ('APP_USER')
                  AND dqs.query_id = pi_query_id)
      WHEN NOT MATCHED
      THEN
         INSERT     (query_statistics_user_login,
                     query_id,
                     query_statistics_run_count)
             VALUES (v ('APP_USER'), pi_query_id, 1)
      WHEN MATCHED
      THEN
         UPDATE SET
            query_statistics_run_count = query_statistics_run_count + 1;

      RETURN lb_result;
   END;


   /**
   * Prepare collection for IR
   *
   * @param pi_query_id       Query id  dq_queries.query_id
   * @param pv_query_result_format_id         Output format -- not used in this procedure
   * @param pv_values         List of params delimiter |
   */

   PROCEDURE Get_Query_Result_Ir (pi_query_id IN DQ_QUERIES.QUERY_ID%TYPE,
                                  pv_values IN VARCHAR2,
                                  pv_query_result_format_id DQ_QUERIES.QUERY_RESULT_FORMAT_ID%TYPE)
   IS
      lt_params wwv_flow_global.vc_arr2;
      lc_query_sql CLOB;
   BEGIN
      SELECT name
        BULK COLLECT INTO lt_params
        FROM dq_queries, TABLE (dq_queries.query_params)
       WHERE query_id = pi_query_id;



      SELECT query_sql_db
        INTO lc_query_sql
        FROM dq_queries
       WHERE query_id = pi_query_id;



      IF apex_collection.collection_exists (
            p_collection_name => v ('APP_USER'))
      THEN
         apex_collection.delete_collection (
            p_collection_name => v ('APP_USER'));
      END IF;



      apex_collection.CREATE_COLLECTION_FROM_QUERY_B (
         p_collection_name => v ('APP_USER'),
         p_query => lc_query_sql,
         p_names => lt_params,
         p_values => APEX_UTIL.string_to_table (pv_values, '|'),
         p_max_row_count => 10000);

      MERGE INTO dq_query_statistics dqs
           USING DUAL
              ON (    dqs.query_statistics_user_login = v ('APP_USER')
                  AND dqs.query_id = pi_query_id)
      WHEN NOT MATCHED
      THEN
         INSERT     (query_statistics_user_login,
                     query_id,
                     query_statistics_run_count)
             VALUES (v ('APP_USER'), pi_query_id, 1)
      WHEN MATCHED
      THEN
         UPDATE SET
            query_statistics_run_count = query_statistics_run_count + 1;
   END;

   /**
   * Parse query on Insert or Update. Save columns and parameters
   *
   * @param pi_query_id       Query id  dq_queries.query_id
   */

   PROCEDURE Parse_Query (pv_query_id NUMBER)
   IS
      ln_cur_cols NUMBER;
      ln_column_cnt NUMBER;
      l_columns_desc DBMS_SQL.desc_tab;

      lt_query t_query;


      ltt_params DQ_TBL_PARAMS;
      ltt_columns DQ_TBL_COLUMNS;
   BEGIN
      lt_query := Get_Parameters_From_Text (pv_query_id);

      BEGIN
         DELETE FROM TABLE (SELECT query_params
                              FROM dq_queries
                             WHERE query_id = pv_query_id);

         DELETE FROM TABLE (SELECT query_columns
                              FROM dq_queries
                             WHERE query_id = pv_query_id);
      EXCEPTION
         WHEN OTHERS
         THEN
            NULL;
      END;



      ltt_params := dq_tbl_params ();

      FOR indx IN 1 .. lt_query.parameters.COUNT
      LOOP
         ltt_params.EXTEND;
         ltt_params (ltt_params.COUNT) := lt_query.parameters (indx);
      END LOOP;



      ln_cur_cols := DBMS_SQL.open_cursor;

      DBMS_SQL.parse (ln_cur_cols, lt_query.text, DBMS_SQL.native);
      DBMS_SQL.describe_columns (ln_cur_cols, ln_column_cnt, l_columns_desc);
      DBMS_SQL.close_cursor (ln_cur_cols);


      ltt_columns := DQ_TBL_COLUMNS ();

      FOR i IN 1 .. ln_column_cnt
      LOOP
         ltt_columns.EXTEND;
         ltt_columns (ltt_columns.COUNT) :=
            dq_typ_columns (i, l_columns_desc (i).col_name);
      END LOOP;

      UPDATE dq_queries
         SET query_params = ltt_params,
             query_columns = ltt_columns,
             query_sql_db = lt_query.text
       WHERE query_id = pv_query_id;
   END;



   FUNCTION Get_Columns (pv_query_id NUMBER)
      RETURN CLOB
   IS
      l_json CLOB;
   BEGIN
      apex_json.initialize_clob_output (DBMS_LOB.CALL, TRUE, 2);
      apex_json.open_object ();
      apex_json.open_array ('cols');

      FOR i IN (  SELECT dqc.name
                    FROM dq_queries dq, TABLE (dq.query_columns) dqc
                   WHERE dq.query_id = 6
                ORDER BY dqc.id)
      LOOP
         apex_json.open_object ();
         apex_json.write ('name', i.name);
         apex_json.close_object;
      END LOOP;

      apex_json.close_array;
      apex_json.close_object;
      RETURN apex_json.get_clob_output;
   END;
END;
/
 
 
 
 
CREATE OR REPLACE PROCEDURE "DQ_RUN_QRY" (pi_query_id INTEGER, pv_query_values  VARCHAR2 DEFAULT NULL, pi_query_result_format_id INTEGER DEFAULT NULL)
IS
   lb_excel BLOB;
   lv_query_name VARCHAR2(255);
   lc_query_sql CLOB;
   
BEGIN



   SELECT
   query_name
   INTO 
   lv_query_name
   FROM 
   dq_queries 
   WHERE query_id = pi_query_id;

 
   lb_excel := dq.Get_Query_Result(pi_query_id,pv_query_values,pi_query_result_format_id);
   
   htp.p('Content-Type: application/csv; charset=UTF-8');
   htp.p('Content-Disposition: inline; filename="'||lv_query_name ||'.csv";');
   
   htp.p('Content-Length: ' || dbms_lob.getlength(lb_excel));
   htp.p('Cache-Control: "no-cache"');  
   htp.p('Set-Cookie: fileDownload=true; path=/');
   htp.p('');       
  
   
   
   wpg_docload.download_file(lb_excel);
   htmldb_application.g_unrecoverable_error := true;
 
END;
/



