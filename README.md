**UNDER HEAVY CONSTRUCTION DO NOT USE**

# CatDB

[![Build Status](https://travis-ci.org/chimpler/catdb.svg)](https://travis-ci.org/chimpler/catdb)
[![Codacy Badge](https://www.codacy.com/project/badge/9475572095844dc7832e36444cc71b78)](https://www.codacy.com/app/francois-dangngoc/catdb)
[![Join the chat at https://gitter.im/chimpler/catdb](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/chimpler/catdb?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

CatDB allows to migrate data from various databases.

### Configuration

Create a file `$HOME/.catdb` with the list of databases to use:

	databases {
	    testdb: ${defaults.credentials} {
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
		credentials {
			username: scott
			password: tiger
		}
	}

The file uses the [HOCON](https://github.com/typesafehub/config/blob/master/HOCON.md) format.
	
At the moment the following databases are supported:

- Postgres / Redshift
- MySQL

### Usage

#### List tables

	catdb list -d <database> -t <table filter>

Options:

- database: database alias set in .catdb
- table filter: table filter (e.g., `doc%`)

#### Generate DDL

	catdb ddl -d <database> -t <table filter> -e <file>

Options:

- database: database alias set in .catdb
- table filter: table filter (e.g., `doc%`)
- file: file to export the DDL to. If `-` is used, standard output is used

#### Create tables from DDL

	catdb ddl -d <database> -i <file> [-dr]

Options:

- database: database alias set in .catdb
- file: file to import the DDL from. If `-` is used, standard input is used
- dr: dry-run to print the statements that would be run on the database

#### Export data

	catdb data -d <database> -t <table> -e <file>

Options:

- database: database alias set in .catdb
- table: table to dump the data
- file: file to export the data to. If `-` is used, standard output is used

#### Import data

	catdb data -d <database> -t <table> -i <file or ->

Options:

- database: database alias set in .catdb
- table: table that will receive the data
- file: file to import the data from. If `-` is used, standard input is used

### TODO

Items                                  | Status
-------------------------------------- | :-----:
DDL export                             | :white_check_mark:
DDL import                             | :white_check_mark:
Data export                            | :white_check_mark:
Data import                            | :white_check_mark:
Constraints export                     | :x:
Constraints import                     | :x:
Postgres                               | :x:
MySQL                                  | :x:
Oracle                                 | :x:
Vertica                                | :x:
SQLite                                 | :x:
Dynamo                                 | :x:
Hive                                   | :x:
SOLR                                   | :x:
Cassandra                              | :x:
Elastic Search                         | :x:
MongoDB                                | :x:
Export to S3                           | :x:
Import from S3                         | :x:
Common console                         | :x:
