drop database if exists testdb;
create database if not exists testdb;
drop table if exists `testdb`.`mocktbl`;
create table if not exists testdb.mocktbl(
    id int,
    fname varchar(100),
    lname varchar(100),
    email varchar(100),
    gender varchar(10),
    ipaddr varchar(100)
);
set global local_infile=1;
load data local infile '/home/samar/Downloads/MOCK_DATA.csv' into table testdb.mocktbl fields terminated by ',' lines terminated by '\n'  ignore 1 rows;