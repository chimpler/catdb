# CatDB

[![pypi](http://img.shields.io/pypi/v/catdb.png)](https://pypi.python.org/pypi/catdb)
[![pypi downloads](http://img.shields.io/pypi/dm/catdb.png)](https://pypi.python.org/pypi/catdb)
[![Build Status](https://travis-ci.org/chimpler/catdb.svg)](https://travis-ci.org/chimpler/catdb)
[![Codacy Badge](https://www.codacy.com/project/badge/9475572095844dc7832e36444cc71b78)](https://www.codacy.com/app/francois-dangngoc/catdb)
[![Coverage Status](https://coveralls.io/repos/chimpler/catdb/badge.svg)](https://coveralls.io/r/chimpler/catdb)
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

	catdb data -d <database> -t <table> -i <file>

Options:

- database: database alias set in .catdb
- table: table that will receive the data
- file: file to import the data from. If `-` is used, standard input is used

### Example

**Create a table in Mysql and populate it**

```sql
mysql> CREATE TABLE employee(
    id INT NOT NULL,
    name VARCHAR(20) DEFAULT '(no name)' NOT NULL, 
    dept CHAR(2), 
    age INT, 
    height DOUBLE(2,1),
    created_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

mysql> INSERT INTO employee(id, name, dept, age, height) VALUES (1, 'John Doe', 'IT', 28, 6.3),(1, 'Mary Gray', 'IT', 30, 6.8);
```

**Export the DDL definition**
    
```json
$ catdb ddl -d my_testdb -t employee -e /tmp/employee.json
$ cat /tmp/employee.json
{
    "database": "testdb",
    "schema": null,
    "tables": [
        {
            "columns": [
                {
                    "column": "id",
                    "nullable": false,
                    "size": 11,
                    "type": "integer"
                },
                {
                    "column": "name",
                    "default": "(no name)",
                    "nullable": false,
                    "size": 20,
                    "type": "varchar"
                },
                {
                    "column": "dept",
                    "nullable": true,
                    "size": 2,
                    "type": "char"
                },
                {
                    "column": "age",
                    "nullable": true,
                    "size": 11,
                    "type": "integer"
                },
                {
                    "column": "height",
                    "nullable": true,
                    "scale": 1,
                    "size": 2,
                    "type": "real"
                },
                {
                    "column": "created_on",
                    "default": "current_timestamp",
                    "nullable": false,
                    "type": "timestamp"
                }
            ],
            "name": "employee"
        }
    ]
}
```
    
**Convert DDL definition to CREATE TABLE statement for Postgres**

```sql
$ catdb ddl -d pg_testdb -t employee -i /tmp/employee.json -dr

CREATE TABLE employee (
    id integer,
    name character varying(20) DEFAULT '(no name)',
    dept character(2),
    age integer,
    height real,
    created_on timestamp without time zone DEFAULT now()
);
```
    
**Export data**

    $ catdb data -d my_testdb -t employee -e /tmp/export.csv
    $ cat /tmp/export.csv
    id|name|dept|age|height|created_on
    1|John Doe|IT|28|6.3|2015-04-28 22:17:57
    1|Mary Gray|IT|30|6.8|2015-04-28 22:17:57        

**Import data (dry-run)**

```sql
$ catdb data -d pg_testdb -t employee -i /tmp/export.csv -dr
INSERT INTO employee (id,name,dept,age,height,created_on)
VALUES('1','John Doe','IT','28','6.3','2015-04-28 22:17:57'),
('1','Mary Gray','IT','30','6.8','2015-04-28 22:17:57');
```

### TODO

Items                                  | Status
-------------------------------------- | :-----:
DDL export                             | :white_check_mark:
DDL import                             | :white_check_mark:
Data export                            | :white_check_mark:
Data import                            | :white_check_mark:
Constraints export                     | :x:
Constraints import                     | :x:
Postgres                               | :white_check_mark:
MySQL                                  | :white_check_mark:
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
Compatible with Turbine XML format     | :x:
Common console                         | :x:
