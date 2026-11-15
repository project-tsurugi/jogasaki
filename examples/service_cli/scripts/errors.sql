-- errors and transaction states after errors
CREATE TABLE qa_t1 (c_pk INT PRIMARY KEY, c_i4 INT NOT NULL);
INSERT INTO qa_t1 VALUES (1, 10);

-- implicit transaction is rolled back on error
--\expect_error
INSERT INTO qa_t1 VALUES (1, 10);
--\expect_error
INSERT INTO no_such_table VALUES (1);

-- explicit transaction becomes inactive on error, and commit fails
BEGIN;
INSERT INTO qa_t1 VALUES (2, 20);
--\expect_error
INSERT INTO qa_t1 VALUES (1, 10);
--\expect_error
COMMIT;
SELECT * FROM qa_t1 ORDER BY c_pk;

-- syntax error
--\expect_error
SELEC * FROM qa_t1;
