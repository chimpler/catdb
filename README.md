**UNDER HEAVY CONSTRUCTION DO NOT USE**

# CatDB

CatDB allows to migrate data from various databases.

### Configuration

Create a file `$HOME/.catdb` with the list of databases to use:

	databases {
	    testdb: ${defaults.mysql} {
			hostname: localhost
			database: testdb
			type: postgres
	    }
	}
	    
At the moment the following databases are supported:

- Postgres / Redshift

### Usage

Generate DDL:

Create table from DDL:

Dump data:

Create data from dump:

### TODO

Items                                  | Status
-------------------------------------- | :-----:
Postgres DDL                           | :x:
Postgres Data                          | :x:
MySQL DDL                              | :x:
SQLite DDL                             | :x:
Dynamo DDL                             | :x:
Data CSV import/export                 | :x:                 
Support Hive                           | :x:
Support SOLR                           | :x:
Export to S3                           | :x:
Import from S3                         | :x:
Common console                         | :x:
