sp_configure 'show advanced options', 1;
GO
RECONFIGURE;
go
sp_configure 'max server memory', 4096;
go
sp_configure 'max worker threads', 512;
go
RECONFIGURE;
go
CREATE DATABASE test;
go
CREATE LOGIN scott with password='tiger^5HHH';
GRANT CONTROL SERVER TO scott;
USE test;
go
CREATE SCHEMA test_schema;
go
ALTER DATABASE test SET ALLOW_SNAPSHOT_ISOLATION ON
ALTER DATABASE test SET READ_COMMITTED_SNAPSHOT ON
go
