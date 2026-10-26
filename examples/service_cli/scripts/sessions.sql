-- concurrent transactions on multiple sessions (tgsqlmux compatible)
CREATE TABLE tb1 (pk INT PRIMARY KEY);
INSERT INTO tb1 VALUES (1);
-- wait for the setup on T0 to complete before other sessions start
--\wait

Tx1:
 BEGIN LONG TRANSACTION WRITE PRESERVE tb1;
 SELECT * FROM tb1;
 INSERT INTO tb1 VALUES (2);
Tx2:
 BEGIN LONG TRANSACTION WRITE PRESERVE tb1;
 SELECT * FROM tb1;
 INSERT INTO tb1 VALUES (3);
 -- this commit waits for Tx1, and fails after Tx1 commits since Tx2 read the range Tx1 wrote
 --\expect_error
 COMMIT;
Tx1:
 COMMIT;
--\wait
T0:
SELECT * FROM tb1 ORDER BY pk;
