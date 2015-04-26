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

### Usage

List tables:

	catdb list -d <database>

Generate DDL:

	catdb ddl -d <database> -t <table> -e <file or ->

Create table from DDL:

	catdb ddl -d <database> -t <table> -i <file or ->

Dump data:

	catdb data -d <database> -t <table> -e <file or ->

Import data from dump:

	catdb data -d <database> -t <table> -i <file or ->

### TODO

Items                                  | Status
-------------------------------------- | :-----:
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
