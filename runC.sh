#!/bin/bash

# 检查客户端程序是否存在
if [ ! -f "rmdb_client/build/rmdb_client" ]; then
    echo "错误: rmdb_client 不存在，请先编译项目"
    exit 1
fi


# create table warehouse (id int, name char(8), score float);
# create index warehouse(id, name, score);
# insert into warehouse values (1, 'abcd', 99.0);
# insert into warehouse values (2, 'efgh', 88.0);
# insert into warehouse values (3, 'ijkl', 77.0);
# select * from warehouse where id = 1 and name = 'abcd' and score = 99.0;
# select * from warehouse where id = 1 and name = 'abcd' and score > 90.0;
# select * from warehouse where id = 1 and name = 'abcd';
# select * from warehouse where name = 'abcd' and id = 1;
# select * from warehouse where id = 1;
# select * from warehouse where id > 1;
# exit;
# 创建测试SQL文件
cat > test_commands.sql << EOF

CREATE TABLE s1_tbl (id INT, name CHAR(10), value INT);
SHOW INDEX FROM s1_tbl;
CREATE INDEX s1_tbl (id);
SHOW INDEX FROM s1_tbl;
CREATE INDEX s1_tbl (name, value);
SHOW INDEX FROM s1_tbl;
CREATE INDEX s1_tbl (id, name);
SHOW INDEX FROM s1_tbl;
DROP INDEX s1_tbl (name, value);
SHOW INDEX FROM s1_tbl;
DROP INDEX s1_tbl (id);
SHOW INDEX FROM s1_tbl;
DROP INDEX s1_tbl (id, name);
SHOW INDEX FROM s1_tbl; 
DROP TABLE s1_tbl;
CREATE TABLE s2_products (pid INT,pname CHAR(15),category CHAR(15),price FLOAT);
INSERT INTO s2_products VALUES (1, 'Laptop', 'Electronics', 1200.00);
INSERT INTO s2_products VALUES (2, 'Mouse', 'Electronics', 25.00);
INSERT INTO s2_products VALUES (3, 'Keyboard', 'Electronics', 75.00);
INSERT INTO s2_products VALUES (4, 'Desk', 'Furniture', 150.00);
INSERT INTO s2_products VALUES (5, 'Chair', 'Furniture', 80.00);
INSERT INTO s2_products VALUES (6, 'Monitor', 'Electronics', 300.00);
INSERT INTO s2_products VALUES (7, 'Webcam', 'Electronics', 50.00);
INSERT INTO s2_products VALUES (8, 'Desk Lamp', 'Furniture', 30.00);
CREATE INDEX s2_products (category, pname, pid);
SHOW INDEX FROM s2_products;
SELECT * FROM s2_products WHERE category = 'Electronics' AND pname = 'Laptop' AND pid = 1;
SELECT * FROM s2_products WHERE category = 'Electronics' AND pname = 'Mouse';
SELECT * FROM s2_products WHERE category = 'Furniture';
SELECT * FROM s2_products WHERE pname = 'Keyboard' AND category = 'Electronics';
SELECT * FROM s2_products WHERE category > 'Electronics';
SELECT * FROM s2_products WHERE category = 'Electronics' AND pname > 'Mouse';
SELECT * FROM s2_products WHERE pname = 'Desk';
DROP INDEX s2_products (category, pname, pid);
CREATE INDEX s2_products (price);
SHOW INDEX FROM s2_products;
SELECT * FROM s2_products WHERE price = 300.00;
SELECT * FROM s2_products WHERE price > 100.00 AND price < 500.00;
DROP INDEX s2_products (price);
DROP TABLE s2_products;
CREATE TABLE s2_warehouse (w_id INT, name CHAR(8));
INSERT INTO s2_warehouse VALUES (10 , 'qweruiop');
INSERT INTO s2_warehouse VALUES (534, 'asdfhjkl');
INSERT INTO s2_warehouse VALUES (100,'qwerghjk');
INSERT INTO s2_warehouse VALUES (500,'bgtyhnmj');

CREATE INDEX s2_warehouse(w_id,name);
SHOW INDEX FROM s2_warehouse;

SELECT * FROM s2_warehouse WHERE w_id = 100 AND name = 'qwerghjk';
SELECT * FROM s2_warehouse WHERE w_id < 600 AND name > 'bztyhnmj';


SELECT * FROM s2_warehouse WHERE w_id = 100;

SELECT * FROM s2_warehouse WHERE name = 'qweruiop' AND w_id = 10;

SELECT * FROM s2_warehouse WHERE w_id > 100;


DROP INDEX s2_warehouse(w_id,name);
DROP TABLE s2_warehouse;

CREATE TABLE s3_employees (empid INT,dept CHAR(10),salary INT);

CREATE INDEX s3_employees (dept, salary);
SHOW INDEX FROM s3_employees;

INSERT INTO s3_employees VALUES (1, 'HR', 5000);
INSERT INTO s3_employees VALUES (2, 'IT', 6000);
INSERT INTO s3_employees VALUES (3, 'HR', 6000);

SELECT * FROM s3_employees WHERE dept='HR';

INSERT INTO s3_employees VALUES (4, 'IT', 5000);
SELECT * FROM s3_employees WHERE dept='IT' AND salary=5000;



INSERT INTO s3_employees VALUES (5, 'HR', 5000);
SELECT * FROM s3_employees WHERE empid=5;

SELECT * FROM s3_employees WHERE dept='HR' AND salary=5000;

DELETE FROM s3_employees WHERE empid = 1; 
SELECT * FROM s3_employees WHERE dept='HR' AND salary=5000;


SELECT * FROM s3_employees WHERE dept='HR';



UPDATE s3_employees SET salary = 6500 WHERE empid = 2;
SELECT * FROM s3_employees WHERE dept='IT' AND salary=6000;

SELECT * FROM s3_employees WHERE dept='IT' AND salary=6500; 



UPDATE s3_employees SET dept = 'HR', salary = 6000 WHERE empid = 4;


SELECT * FROM s3_employees WHERE empid=4;

SELECT * FROM s3_employees WHERE dept='HR' AND salary=6000;


INSERT INTO s3_employees VALUES (10, 'Sales', 7000);
CREATE INDEX s3_employees (empid);
SHOW INDEX FROM s3_employees;


UPDATE s3_employees SET salary = 7500 WHERE empid = 10;
SELECT * FROM s3_employees WHERE dept='Sales' AND salary=7500;

SELECT * FROM s3_employees WHERE empid=10;


DROP INDEX s3_employees (dept, salary);
DROP INDEX s3_employees (empid);
DROP TABLE s3_employees;


CREATE TABLE s4_warehouse (w_id INT, name CHAR(8));
INSERT INTO s4_warehouse VALUES (10 , 'qweruiop');
INSERT INTO s4_warehouse VALUES (534, 'asdfhjkl');

SELECT * FROM s4_warehouse WHERE w_id = 10;

SELECT * FROM s4_warehouse WHERE w_id < 534 AND w_id > 100;


CREATE INDEX s4_warehouse(w_id);
SHOW INDEX FROM s4_warehouse;


INSERT INTO s4_warehouse VALUES (500, 'lastdanc');

SELECT * FROM s4_warehouse WHERE w_id = 500;


UPDATE s4_warehouse SET w_id = 507 WHERE w_id = 534;

SELECT * FROM s4_warehouse WHERE w_id = 534;

SELECT * FROM s4_warehouse WHERE w_id = 507;



SELECT * FROM s4_warehouse WHERE w_id = 10;

SELECT * FROM s4_warehouse WHERE w_id < 534 AND w_id > 100;

INSERT INTO s4_warehouse VALUES (10, 'another');
SELECT * FROM s4_warehouse WHERE name = 'another';

INSERT INTO s4_warehouse VALUES (20, 'original');
UPDATE s4_warehouse SET w_id = 10 WHERE name = 'original';
SELECT * FROM s4_warehouse WHERE name = 'original';

DROP INDEX s4_warehouse(w_id);
DROP TABLE s4_warehouse;

EXIT;
EOF

echo "正在启动数据库客户端并执行测试命令..."
echo "=========================================="

# 运行客户端并传入测试命令
orb rmdb_client/build/rmdb_client < test_commands.sql

echo "=========================================="
echo "测试完成"

# 清理临时文件
rm -f test_commands.sql