create table t(a int, b int);
insert into t values(10, 1.0);
insert into t values(20, 1.0);
insert into t values(20, 2.0);
select AVG(a) from t;