import sqlalchemy as sa
import json
import json


def main(connection_uri, schema, new_table_space, file_name="move_table_space.sql"):

    engine = sa.create_engine(connection_uri)
    connection = engine.connect()

    meta_data = sa.MetaData(connection, schema=schema)
    meta_data.reflect()

    with open(file_name, "w") as fw:

        for table in meta_data.tables:
            schema, table_name = table.split(".")
            full_table_name = '"%s"."%s"' % (schema, table_name)

            fw.write("alter table %s set tablespace  %s;\n" % (full_table_name, new_table_space))

            table_obj = meta_data.tables[table]
            indexes = table_obj.indexes

            for index_obj in indexes:
                index_name =  index_obj.name
                full_index_name = '"%s"."%s"' % (schema, index_name)

                fw.write("alter index %s set tablespace  %s;\n" % (full_index_name, new_table_space))

            fw.write("\n")


if __name__ == "__main__":

    with open("config.json") as f:
        config = json.load(f)

    connection_uri = config["connection_uri"]
    schema = config["schema"]
    new_table_space = config["new_tablespace"]

    main(connection_uri, schema, new_table_space)
