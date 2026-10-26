-- basic usage: implicit/explicit transactions, prepared statements and queries
CREATE TABLE T0 (C0 BIGINT PRIMARY KEY, C1 DOUBLE);

\prepare ins c0:BIGINT c1:DOUBLE AS INSERT INTO T0 (C0, C1) VALUES (:c0, :c1);
\prepare sel c0:BIGINT c1:DOUBLE AS SELECT * FROM T0 WHERE C0 = :c0 AND C1 = :c1;

BEGIN;
\execute ins c0=0 c1=0;
INSERT INTO T0 (C0, C1) VALUES (1, 1);
COMMIT;

SELECT * FROM T0 ORDER BY C0;
\execute sel c0=1 c1=1;

BEGIN;
INSERT INTO T0 (C0, C1) VALUES (2, 2);
ROLLBACK;
SELECT count(*) FROM T0;
