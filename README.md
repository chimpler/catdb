**UNDER HEAVY CONSTRUCTION DO NOT USE**

# CatDB

CatDB allows to migrate data from various databases.

### Configuration

Create a file `$HOME/.catdb` with the list of databases to use:

	databases {
	    testdb: ${defaults.postgres} {
			hostname: localhost
			database: testdb
			type: postgres
	    }
	    my_testdb: ${defaults.credentials} {
			hostname: localhost
			database: testdb
			type: mysql
	    }
	}
	    
	defaults {
		postgres {
			username: scott
			password: tiger
		}
	}

The file uses the [HOCON](https://github.com/typesafehub/config/blob/master/HOCON.md) format.
	
At the moment the following databases are supported:

- Postgres / Redshift

### Usage

List tables:

	catdb list -d <database>

Generate DDL:

	catdb ddl -d <database> -t <table> -e <file or ->

Create table from DDL:

	catdb ddl -d <database> -t <table> -i <file or ->

Dump data:

	catdb data -d <database> -t <table> -e <file or ->

Create data from dump:

	catdb data -d <database> -t <table> -i <file or ->

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
