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

SELECT * FROM TABLE(virtual_table(5));
SELECT * FROM TABLE(virtual_table(10));

begin
  FOR x IN (SELECT * FROM TABLE(virtual_table(10))) LOOP
    dbms_output.put_line('Fetching.... ' || x.column_value);
  END LOOP;
END;
/

CREATE OR REPLACE TYPE myScalarType AS OBJECT (
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