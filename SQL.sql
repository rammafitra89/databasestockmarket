--step 0 : enter to postgres on terminal via 
-- sudo -u postgres psql
-- step 1 : create database
CREATE DATABASE dataloratech;
-- setp 2 : move to database 
\c dataloratech
--step 3 : execute file temp_project.sql
\i temp_project.sql
-- step 4 : back to database stockmarket
\c dataloratech
-- step 5 : check database
\dt
-- step 6 : create table to store average_price
CREATE TABLE sql (month VARCHAR, average_price VARCHAR, index VARCHAR);
-- step 7 : check new table (resultsql)
\dt

-- step 8 : count avarage price of all the stock in each market/index


----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


-- [ THIS IS FOR INDEX CSI300 ] -- 

INSERT INTO sql(month, average_price) SELECT day, AVG(price) FROM (SELECT * FROM imported_closes INNER JOIN monthly_members ON monthly_members.month=imported_closes.day WHERE index='CSI300') AS price  GROUP by day ORDER BY day;
--update index
UPDATE sql SET index = 'CSI300' WHERE index IS NULL;
--show result
-- select * from sql;

-- [ THIS IS FOR INDEX TWSE ] --
INSERT INTO sql(month, average_price) SELECT day, AVG(price) FROM (SELECT* from imported_closes INNER JOIN monthly_members ON monthly_members.month=imported_closes.day WHERE index='TWSE') AS price  GROUP by day ORDER BY day;
-- update index for TWSE
UPDATE sql SET index = 'TWSE' WHERE index IS NULL;
--show result
-- select * from sql where index='TWSE';

-- [ THIS IS FOR INDEX HK ] --

INSERT INTO sql(month, average_price) SELECT day, AVG(price) FROM (SELECT * FROM imported_closes INNER JOIN monthly_members ON monthly_members.month=imported_closes.day WHERE index='HK') AS price  GROUP by day ORDER BY day;
-- update index for HK
UPDATE sql SET index = 'HK' WHERE index IS NULL;
-- show result
-- select * from sql where index='HK';

--[ THIS IS FOR INDEX KOSPI2 ] --


INSERT INTO sql(month, average_price) SELECT day, AVG(price) FROM (SELECT * FROM imported_closes INNER JOIN monthly_members ON monthly_members.month=imported_closes.day WHERE index='KOSPI2') AS price  GROUP by day ORDER BY day;
-- update index for KOSPI2
UPDATE sql SET index = 'KOSPI2' WHERE index IS NULL;
-- show result
-- select * from sql where index='KOSPI2';

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------



-- [EXPORT TO FORMAT.csv]  
-- [if we need copy data, we need make black data in format csv on our home directory with write touch SQL csv in our terminal and  write permission access with "sudo chmod 777 SQL.csv on our terminal too"]

\COPY sql TO SQL.csv WITH (FORMAT CSV, HEADER);


-- [AUTETIFICATION]-- 

-- CREATE USER loratech WITH ENCRYPTED PASSWORD "loratech";
-- ALTER ROLE loratech WITH PASSWORD 'loratech';
-- grant all privileges on database dataloratech TO loratech 



