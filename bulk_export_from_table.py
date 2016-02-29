__author__ = 'janos'

import csv
import argparse
import sqlalchemy as sa


def bulk_export_from_table(connection_uri, file_name_to_write_to, table_name, schema=None, restrictions=None):
    """
        We only allow simple restrictions to be specified
         {"field_name1": "value", "field_nam2": "value"}
    """
    try:
        engine = sa.create_engine(connection_uri)
    except IOError:
        print "Database could not be connected to"
        raise

    meta = sa.MetaData(engine, schema=schema)
    meta.reflect(schema=schema)

    tables = meta.tables.keys()

    if table_name in tables:

        table_obj = meta.tables[table_name]

        select_statement = table_obj.select()

        if restrictions is not None:
            and_statements = []

            for restriction in restrictions:
                and_statements += [table_obj.columns[restriction] == restrictions[restriction]]

            for and_statement in and_statements:
                select_statement = select_statement.where(and_statement)

        cursor = engine.execute(select_statement.compile())
        print(select_statement.compile())
        print(select_statement.compile().params)
        header = [c.name for c in table_obj.columns]

        i = 0
        with open(file_name_to_write_to, "wb") as fw:
            csv_writer = csv.writer(fw)
            csv_writer.writerow(header)

            for row in cursor:
                row_to_write = []
                for cell in row:
                    if cell is not None:
                        if cell.__class__ == u"".__class__:
                            cell = cell.encode("ascii", errors="replace")
                    row_to_write += [cell]
                csv_writer.writerow(row_to_write)

                i += 1

                if i % 10000 == 0:
                    print("Exported %s lines" % i)
    else:
        raise RuntimeError, "Table '%s' could not be found" % table_name

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Export a table to CSV')
    parser.add_argument("--table", dest="table_name", action="store")
    parser.add_argument("--schema", dest="schema", action="store", default=None)
    parser.add_argument("--restrictions", dest="restrictions", action="store", default=None)
    parser.add_argument("--filename", dest="file_name", action="store", default=None)
    parser.add_argument("--connection_uri", dest="connection_uri", action="store")
    args = parser.parse_args()

    connection_uri = args.connection_uri
    table_name = args.table_name
    schema = args.schema
    restrictions = args.restrictions

    if restrictions is not None:
        split_restrictions = restrictions.split(",")
        split_dict = {}
        for i in range(len(split_restrictions)/2):
            split_dict[split_restrictions[i*2]] = split_restrictions[i*2 + 1]
        restrictions = split_dict

    if args.file_name is None:
        file_name = table_name + ".csv"
    else:
        file_name = args.file_name

    bulk_export_from_table(connection_uri, file_name, table_name, restrictions=restrictions, schema=schema)