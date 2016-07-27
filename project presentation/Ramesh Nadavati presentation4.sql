---table functions
CREATE TABLE stocktable (
ticker       VARCHAR2(4),
open_price   NUMBER(10),
close_price  NUMBER(10));
---entering values
INSERT INTO stocktable VALUES ('ORCL', 13, 16);
INSERT INTO stocktable VALUES ('MSFT', 35, 29);
INSERT INTO stocktable VALUES ('SUNW', 7, 11);
COMMIT;

---DROP TABLE stocktable;
---creating type function
CREATE OR REPLACE TYPE TickerType AS OBJECT (
ticker    VARCHAR2(4),
pricetype VARCHAR2(1),
price     NUMBER(10));
/

CREATE OR REPLACE TYPE TickerTypeSet AS TABLE OF TickerType;
/

----package creation

CREATE OR REPLACE PACKAGE refcur_pkg IS

TYPE refcur_t IS REF CURSOR RETURN StockTable%ROWTYPE;

END refcur_pkg;
/
------stockpivot function and pipeling

CREATE OR REPLACE FUNCTION stockpivot(
p refcur_pkg.refcur_t)
RETURN TickerTypeSet PIPELINED IS

out_rec  TickerType := TickerType(NULL,NULL,NULL);
in_rec   p%ROWTYPE;

BEGIN
  LOOP
    FETCH p INTO in_rec;
    EXIT WHEN p%NOTFOUND;

    out_rec.ticker := in_rec.Ticker;
    out_rec.pricetype := 'O';
    out_rec.price := in_rec.Open_Price;
    PIPE ROW(out_rec);

    out_rec.PriceType := 'C';
    out_rec.price := in_rec.Close_Price;
    PIPE ROW(out_rec);
  END LOOP;
  CLOSE p;
  RETURN;
END stockpivot;
/

desc stockpivot

set linesize 121 
col pipelined format a10

SELECT object_name, pipelined, authid
FROM user_procedures;

-------sample query 
SELECT *
FROM TABLE(stockpivot(CURSOR(SELECT * FROM StockTable)));

-------generate data list

CREATE OR REPLACE TYPE date_array AS TABLE OF DATE;
/

CREATE OR REPLACE FUNCTION date_table(sdate DATE, edate DATE)
RETURN date_array PIPELINED AS

BEGIN
  FOR i IN 0 .. (edate - sdate) LOOP
    PIPE ROW(sdate + i);
  END LOOP;
  RETURN;
END date_table;
/

desc date_table

SELECT object_name, pipelined, authid
FROM user_procedures;

SELECT *
FROM TABLE(CAST(date_table(TRUNC(SYSDATE-30), TRUNC(SYSDATE))
AS date_array));

-- joined with another table

CREATE TABLE testdata (
datecol DATE,
someval NUMBER);

INSERT INTO testdata VALUES (TRUNC(SYSDATE-25), 25);
INSERT INTO testdata VALUES (TRUNC(SYSDATE-20), 20);
INSERT INTO testdata VALUES (TRUNC(SYSDATE-15), 15);
INSERT INTO testdata VALUES (TRUNC(SYSDATE-10), 10);
INSERT INTO testdata VALUES (TRUNC(SYSDATE-5), 5);
COMMIT;

SELECT * FROM testdata;

SELECT da.column_value AS DATECOL, td.someval
FROM TABLE(CAST(date_table(TRUNC(SYSDATE-30), TRUNC(SYSDATE))
AS date_array)) da, testdata td
WHERE da.COLUMN_VALUE = td.datecol(+);

----Note: A SQL alternative would be:

--SELECT iv.datecol, td.someval
--FROM (
--WITH dates AS (SELECT SYSDATE-30 dt_start, SYSDATE dt_end FROM dual)
--SELECT dt_start+rownum-1 AS "DATECOL"
--FROM dates
--CONNECT BY LEVEL <= dt_end-dt_start) iv, testdata td
--WHERE TRUNC(iv.datecol) = TRUNC(td.datecol (+))
--ORDER BY datecol;

-----row by row output

CREATE OR REPLACE TYPE str_array AS TABLE OF VARCHAR2(10);
/
----ptf function
CREATE OR REPLACE FUNCTION ptf(stringin VARCHAR2) RETURN str_array PIPELINED IS
 i   PLS_INTEGER;
 str VARCHAR2(100);
 tab sys.dbms_utility.uncl_array;
BEGIN
  str := '"' || REPLACE(stringin, ',', '","') || '"';
  sys.dbms_utility.comma_to_table(str, i, tab);

  FOR j IN 1 .. 5 LOOP
    PIPE ROW(TRANSLATE(tab(j),'A"','A'));
  END LOOP;
  RETURN;
END ptf;
/

SELECT *
FROM TABLE(CAST(ptf('1001,1002,1003,1004,1005')
AS str_array));

-----

CREATE OR REPLACE TYPE virtual_table_type AS TABLE OF number;
/

CREATE OR REPLACE FUNCTION virtual_table(p_num_rows IN NUMBER) 
RETURN virtual_table_type PIPELINED IS
BEGIN
  FOR i IN 1 .. p_num_rows LOOP
    dbms_output.put_line('going to pipe');
    PIPE ROW( i );
    dbms_output.put_line('done pipeing');
  END LOOP;
  RETURN;
END virtual_table;
/
---pl/sql procedure query

SELECT * FROM TABLE(virtual_table(5));
SELECT * FROM TABLE(virtual_table(10));

set serveroutput on;

begin
  FOR x IN (SELECT * FROM TABLE(virtual_table(10))) LOOP
    dbms_output.put_line('Fetching.... ' || x.column_value);
  END LOOP;
END;
/

----drop type myScalarType;

CREATE OR REPLACE TYPE myScalarType validate AS OBJECT (
c1 VARCHAR2(9), 
c2 VARCHAR2(9),
c3 VARCHAR2(9),
c4 VARCHAR2(9),
c5 VARCHAR2(9),
c6 VARCHAR2(9),
c7 VARCHAR2(9));
/

desc myScalarType

CREATE OR REPLACE TYPE myArrayType AS TABLE OF myScalarType;
/

desc myArrayType

CREATE OR REPLACE FUNCTION pivot(p_cur IN sys_refcursor) 
RETURN myArrayType PIPELINED IS

 l_c1 varchar2(4000);
 l_c2 varchar2(4000);
 l_last varchar2(4000);
 l_cnt number ;
 l_data myScalarType;
BEGIN
  LOOP
    FETCH p_cur INTO l_c1, l_c2;
    EXIT WHEN p_cur%NOTFOUND;

    IF (l_last IS NULL OR l_c1 <> l_last) THEN
      IF (l_data IS NOT NULL) THEN
        pipe row(l_data);
      END IF;

      l_data := myScalarType(l_c1, l_c2, NULL, NULL, NULL, NULL, NULL);
      l_cnt := 3;
      l_last := l_c1;
    ELSE
      CASE l_cnt
      WHEN 3 THEN l_data.c3 := l_c2;
      WHEN 4 THEN l_data.c4 := l_c2;
      WHEN 5 THEN l_data.c5 := l_c2;
      WHEN 6 THEN l_data.c6 := l_c2;
      WHEN 7 THEN l_data.c7 := l_c2;
      ELSE raise program_error;
      END CASE;

      l_cnt := l_cnt+1;
    END IF;
  END LOOP;

  IF (l_data IS NOT NULL) THEN
    PIPE ROW(l_data);
  END IF;
  CLOSE p_cur;
  RETURN;
END pivot;
/

SELECT *
FROM TABLE(pivot(CURSOR(SELECT deptno, ename FROM scott.emp ORDER BY deptno)));

SELECT *
FROM TABLE(pivot(
CURSOR(SELECT deptno, hiredate FROM scott.emp ORDER BY deptno)));


--DROP FUNCTION ptf;

--DROP FUNCTION pivot;

--DROP FUNCTION stockpivot;