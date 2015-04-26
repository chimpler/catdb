import argparse
import json
from pyhocon import ConfigFactory
import sys
from catdb.db import DbManager


def main():

    parent_parser = argparse.ArgumentParser(add_help=False)
    parent_parser.add_argument('-d', '--database', help='database', required=True, action='store')
    parent_parser.add_argument('-s', '--schema', help='schema', required=False, action='store', default=None)
    parent_parser.add_argument('-t', '--table', help='table filter (using % as a wildcard)', required=False, action='store')
    parent_parser.add_argument('-dr', '--dry-run', dest='dry_run', help='dry run', required=False, action='store_true')

    argparser = argparse.ArgumentParser(description='export')
    subparsers = argparser.add_subparsers(help='sub-command help', dest='subparser_name')
    ddl_parser = subparsers.add_parser('ddl', help='ddl', parents=[parent_parser])
    ddl_parser.add_argument('-e', '--export', dest='export_file', help='export', required=False, action='store')
    ddl_parser.add_argument('-i', '--import', dest='import_file', help='import', required=False)
    data_parser = subparsers.add_parser('data', help='data', parents=[parent_parser])
    data_parser.add_argument('-e', '--export', dest='export_file', help='export', required=False, action='store')
    data_parser.add_argument('-i', '--import', dest='import_file', help='import', required=False)
    list_parser = subparsers.add_parser('list', help='list', parents=[parent_parser])

    args = argparser.parse_args()
    config = ConfigFactory.parse_file('.catdb')
    db_config = config['databases.' + args.database]

    db = DbManager.get_db(db_config['type'], db_config)
    if args.subparser_name == 'list':
        print '\n'.join(db.list_tables(args.schema, args.table))
    elif args.subparser_name == 'ddl':
        if args.export_file:
            ddl_str = json.dumps(db.get_ddl(args.schema, args.table), sort_keys=True, indent=4, separators=(',', ': '))
            if args.export_file == '-':
                print ddl_str
            else:
                with open(args.export_file, 'w') as fd:
                    fd.write(ddl_str)
        elif args.import_file:
            if args.import_file == '-':
                ddl = json.loads(sys.stdin)
            else:
                with open(args.import_file, 'r') as fd:
                    ddl = json.loads(fd.read())

            table_statement = db.create_database_statement(ddl, args.database, args.schema)
            if args.dry_run:
                print table_statement
            else:
                db.execute(table_statement)
    elif args.subparser_name == 'data':
        if args.export_file:
            if args.export_file == '-':
                db.export_to_file(sys.stdout, args.schema, args.table)
            else:
                with open(args.export_file, 'w') as fd:
                    db.export_to_file(fd, args.schema, args.table)
        elif args.import_file:
            if args.import_file == '-':
                db.import_to_file(sys.stdin, args.schema, args.table, args.dry_run)
            else:
                with open(args.import_file, 'r') as fd:
                    db.import_from_file(fd, args.schema, args.table, args.dry_run)

if __name__ == '__main__':
    main()
