-- parameters and results of various types
CREATE TABLE TT (
    C0 INT PRIMARY KEY,
    C1 BIGINT,
    C2 REAL,
    C3 DOUBLE,
    C4 DECIMAL(5, 3),
    C5 VARCHAR(100),
    C6 DATE,
    C7 TIME,
    C8 TIMESTAMP
);
\prepare ins c0:INT c1:BIGINT c2:REAL c3:DOUBLE c4:DECIMAL c5:VARCHAR c6:DATE c7:TIME c8:TIMESTAMP
    AS INSERT INTO TT VALUES (:c0, :c1, :c2, :c3, :c4, :c5, :c6, :c7, :c8);
\execute ins c0=1 c1=1 c2=1.5 c3=1.5 c4=1.234 c5='a b' c6=2024-01-02 c7=12:34:56 c8=2024-01-02T03:04:05.123;
\execute ins c0=2 c1=NULL c2=NULL c3=NULL c4=NULL c5=NULL c6=NULL c7=NULL c8=NULL;
\execute ins c0=3 c1=99999999999 c2=-1 c3=-1e10 c4=-0.001 c5='it''s' c6=1999-12-31 c7=00:00:00.000000001 c8=1999-12-31T23:59:59;
SELECT * FROM TT ORDER BY C0;

-- invalid parameter values are reported as errors
--\expect_error
\execute ins c0=4 c1=1 c2=1 c3=1 c4=1 c5=x c6=bad-date c7=00:00:00 c8=2024-01-01T00:00:00;
--\expect_error
\execute ins c0=4 c1=abc c2=1 c3=1 c4=1 c5=x c6=2024-01-01 c7=00:00:00 c8=2024-01-01T00:00:00;
