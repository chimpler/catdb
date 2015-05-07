import argparse
import json
import os
from pyhocon import ConfigFactory
import sys
from catdb import open_output_file, open_input_file
from catdb.db import DbManager


def main():
    parent_parser = argparse.ArgumentParser(add_help=False)
    parent_parser.add_argument('-d', '--database', help='database', required=True, action='store')
    parent_parser.add_argument('-s', '--schema', help='schema', required=False, action='store', default=None)
    parent_parser.add_argument('-t', '--table', help='table filter (using % as a wildcard)', required=False,
                               action='store')
    parent_parser.add_argument('-dr', '--dry-run', dest='dry_run', help='dry run', required=False, action='store_true')

    argparser = argparse.ArgumentParser(description='export')
    subparsers = argparser.add_subparsers(help='sub-command help', dest='subparser_name')
    ddl_parser = subparsers.add_parser('ddl', help='ddl', parents=[parent_parser])
    ddl_parser.add_argument('-e', '--export', dest='export_file', help='export', required=False, action='store')
    ddl_parser.add_argument('-i', '--import', dest='import_file', help='import', required=False)
    data_parser = subparsers.add_parser('data', help='data', parents=[parent_parser])
    data_parser.add_argument('-e', '--export', dest='export_file', help='export', required=False, action='store')
    data_parser.add_argument('-i', '--import', dest='import_file', help='import', required=False)
    subparsers.add_parser('list', help='list', parents=[parent_parser])

    args = argparser.parse_args()
    home_dir = os.environ['HOME']
    config_path = os.path.join(home_dir, '.catdb')
    if not os.path.exists(config_path):
        sys.stderr.write(
            'File {config_path} not found. Go to https://github.com/chimpler/catdb for more details\n'.format(
                config_path=config_path))
        sys.exit(1)

    config = ConfigFactory.parse_file(config_path)
    db_config = config['databases.' + args.database]

    db = DbManager.get_db(db_config['type'], db_config)
    if args.subparser_name == 'list':
        print '\n'.join(db.list_tables(args.table, args.schema))
    elif args.subparser_name == 'ddl':
        if args.export_file:
            ddl_str = json.dumps(db.get_ddl(args.table, args.schema), sort_keys=True,
                                 indent=config['ddl-format.indent'], separators=(',', ': '))
            with open_output_file(args.export_file) as fd:
                fd.write(ddl_str)
        elif args.import_file:
            with open_input_file(args.import_file) as fd:
                ddl = json.loads(fd.read())
                table_statement = db.create_database_statement(ddl, args.database, args.schema)
                if args.dry_run:
                    print table_statement
                else:
                    db.execute(table_statement)
    elif args.subparser_name == 'data':
        if args.export_file:
            db.export_to_file(args.export_file, args.table, args.schema, config['data-format.delimiter'],
                              config['data-format.null'])

        elif args.import_file:
            db.import_from_file(args.import_fileport_file, args.table, args.schema, config['data-format.delimiter'],
                                config['data-format.null'])


if __name__ == '__main__':
    main()
