-- queries with secondary indices
CREATE TABLE qa_t1 (c_pk INT PRIMARY KEY, c_i4 INT NOT NULL, c_i8 BIGINT NOT NULL, c_f4 REAL NOT NULL, c_f8 DOUBLE NOT NULL, c_ch VARCHAR(*) NOT NULL);
CREATE INDEX qa_t1_i4_idx ON qa_t1(c_i4);
CREATE TABLE qa_t2 (c_pk1 INT NOT NULL, c_pk2 VARCHAR(*) NOT NULL, c_id1 INT NOT NULL, c_id2 VARCHAR(*) NULL, c_jk1 INT NOT NULL, c_jk2 VARCHAR(*) NULL, PRIMARY KEY (c_pk1, c_pk2));
CREATE INDEX qa_t2_idc_idx ON qa_t2(c_id1, c_id2);
CREATE INDEX qa_t2_jk1_idx ON qa_t2(c_jk1);

INSERT INTO qa_t1 (c_pk, c_i4, c_i8, c_f4, c_f8, c_ch) VALUES (1, 10, 100, 11, 111, '11111');
INSERT INTO qa_t1 (c_pk, c_i4, c_i8, c_f4, c_f8, c_ch) VALUES (2, 20, 200, 22, 222, '22222');
INSERT INTO qa_t1 (c_pk, c_i4, c_i8, c_f4, c_f8, c_ch) VALUES (3, 30, 300, 33, 333, '33333');
INSERT INTO qa_t1 (c_pk, c_i4, c_i8, c_f4, c_f8, c_ch) VALUES (4, 40, 400, 44, 444, '44444');
INSERT INTO qa_t2 (c_pk1, c_pk2, c_id1, c_id2, c_jk1, c_jk2) VALUES (1, '10', 11, '111', 11, '111');
INSERT INTO qa_t2 (c_pk1, c_pk2, c_id1, c_id2, c_jk1, c_jk2) VALUES (2, '20', 22, '222', 22, '222');

SELECT * FROM qa_t1 WHERE c_pk = 2;
SELECT * FROM qa_t1 WHERE c_i4 = 20;
SELECT * FROM qa_t1 WHERE c_ch = '33333';
UPDATE qa_t1 SET c_i4 = 200 WHERE c_i8 = 400;
DELETE FROM qa_t1 WHERE c_i4 = 200;
SELECT * FROM qa_t1 ORDER BY c_pk;
SELECT * FROM qa_t2 WHERE c_pk1 = 1 AND c_pk2 = '10';
SELECT * FROM qa_t2 WHERE c_id1 = 22;
SELECT t1.c_pk, t2.c_pk2 FROM qa_t1 t1 JOIN qa_t2 t2 ON t1.c_pk = t2.c_pk1 ORDER BY t1.c_pk;
