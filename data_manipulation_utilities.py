__author__ = 'janos'

import sqlalchemy as sa
import re


def get_base_pattern(field_name_pattern):
    if field_name_pattern[-1] == " " or field_name_pattern[-1] == "_":
        base_field_name = field_name_pattern[:-1]
    else:
        base_field_name = field_name_pattern

    return base_field_name


def create_table_that_normalize_repeated_column(table_name, field_name_pattern, new_table_name, engine,
                                                identifier_column="id", mapped_identifier_column="mapped_id",
                                                sequence_field_name="sequence_id", schema=None):

    metadata = reflect_metadata(engine, schema)

    columns = get_column_names_from_table(table_name, metadata)
    columns_dict = get_column_names_with_types_dict(table_name, metadata)

    columns_that_repeat = get_columns_that_appear_to_repeat(columns, field_name_pattern)

    data_type = columns_dict[columns_that_repeat[0][0]]

    data_type_id_field = columns_dict[identifier_column]

    columns_to_add = []
    columns_to_add += [sa.Column(mapped_identifier_column, data_type_id_field)]

    base_field_name = get_base_pattern(field_name_pattern)

    columns_to_add += [sa.Column(sequence_field_name, sa.Integer())]

    columns_to_add += [sa.Column(base_field_name, data_type)]

    new_table = sa.Table(new_table_name, metadata, *columns_to_add)
    metadata.create_all()

    return new_table


def create_insert_for_column_that_repeats(new_table, old_table, metadata, identifier_to_map, repeated_column,
                                          sequence_id, mapped_identifier, mapped_column_that_repeats,
                                          sequence_column_name):
    """

    """

    new_table = metadata.tables[new_table]
    old_table = metadata.tables[old_table]

    identifier_column_obj = old_table.columns[identifier_to_map]
    column_that_repeats = old_table.columns[repeated_column]

    select_sql_expr = sa.sql.select([identifier_column_obj, column_that_repeats, sa.sql.literal_column(str(sequence_id))]).where(column_that_repeats != None)
    insert_sql_expr = new_table.insert().from_select([mapped_identifier, sequence_column_name, mapped_column_that_repeats], select_sql_expr)

    return insert_sql_expr


def normalize_columns_that_repeat(table_name, new_table_name, pattern_to_match, engine, identifier_column="id",
                                 identifier_column_that_maps="mapped_id", sequence_field_name="sequence_id", schema=None):

    """Normalize a table by looking for patterns that repeat in the table columns based on a pattern"""
    #TODO: pattern_to_match is not robust to ["dx_" or "dx "] need to strip these characters
    metadata = reflect_metadata(engine, schema=schema)
    column_names = get_column_names_from_table(table_name, metadata)
    column_names_that_repeat = get_columns_that_appear_to_repeat(column_names, search_pattern=pattern_to_match)
    new_table_obj = create_table_that_normalize_repeated_column(table_name, pattern_to_match, new_table_name, engine,
                                                                identifier_column, identifier_column_that_maps,
                                                                sequence_field_name, schema)
    metadata = reflect_metadata(engine, schema=schema)
    sql_to_execute = []
    for repeat_column in column_names_that_repeat:
        sql_expr = create_insert_for_column_that_repeats(new_table_name, table_name, metadata, identifier_column,
                                                         repeat_column[0], repeat_column[1], identifier_column_that_maps,
                                                         pattern_to_match, sequence_field_name)

        sql_to_execute += [str(sql_expr)]

    connection = engine.connect()

    for sql_statement in sql_to_execute:
        print(sql_statement)
        engine.execute(sql_statement)

    return sql_to_execute


def get_data_table(table_name, metadata):
    return metadata.table[table_name]


def reflect_metadata(engine, schema=None):
    meta = sa.MetaData(engine)
    meta.reflect()
    return meta


def get_column_names_from_table(table_name, metadata):
    table = metadata.tables[table_name]
    columns = list(table.columns)
    column_list = [c.name for c in columns]
    return column_list


def get_column_names_with_types_dict(table_name, metadata):
    table = metadata.tables[table_name]
    columns = list(table.columns)
    column_data_type_dict = {}
    for column in columns:
        column_data_type_dict[column.name] = column.type
    return column_data_type_dict


def get_columns_that_appear_to_repeat(list_of_fields, search_pattern):
    """Matches columns that have a regular pattern that has a number suffix"""
    string_to_suffix = r"([0-9]*$)"

    re_search_pattern_string = search_pattern + string_to_suffix
    re_search_pattern = re.compile(re_search_pattern_string)

    list_of_fields_match = []
    for field in list_of_fields:
        match_result = re.match(re_search_pattern, field)
        if match_result:
            matched_results = match_result.groups()
            number_matched = matched_results[0]

            list_of_fields_match.append((field, int(number_matched), search_pattern))

    list_of_fields_match.sort(key=lambda x: x[1])
    return list_of_fields_match

__author__ = 'janos'

def stitch_two_tables(table_1, id_field_1, table_2, id_field_2, new_table, engine, columns_to_exclude_1=[], columns_to_exclude_2=[], prefixes_column_names=("t1", "t2")):
    """This program is used to stitch / join two tables together into a third table.
As this is a common operation the goal is to with several annoyances in SQL

Handling fields that repeat
Excluding certain fields
Chaining together multiple joins

The script using the introspective /relfective ability of SQLAlchemy to

    """
    metadata = reflect_metadata(engine, schema=None)
    t1_obj = metadata.tables[table_1]
    t2_obj = metadata.tables[table_2]

    id_field_1_obj = t1_obj.columns[id_field_1]
    id_field_2_obj = t2_obj.columns[id_field_2]

    column_names_that_repeat = []

    column_to_include_t1 = []
    column_to_include_t2 = []

    column_types_dict1 = get_column_names_with_types_dict(table_1, metadata)
    column_types_dict2 = get_column_names_with_types_dict(table_2, metadata)
    column_name_maps_t1 = {}
    column_name_maps_t2 = {}

    column_names_t1 = t1_obj.columns.keys()
    column_names_t2 = t2_obj.columns.keys()

    s_t1 = set(column_names_t1)
    s_t2 = set(column_names_t2)

    s_intersect_tc = s_t1.intersection(s_t2)
    column_names_that_repeat = list(s_intersect_tc)

    for column_name in column_names_t1:
        if column_name not in columns_to_exclude_1:
            column_to_include_t1 += [column_name]
            if column_name in column_names_that_repeat:
                column_name_maps_t1[column_name] = prefixes_column_names[0] + column_name

    for column_name in column_names_t2:
        if column_name not in columns_to_exclude_2:
            column_to_include_t2 += [column_name]
            if column_name in column_names_that_repeat:
                column_name_maps_t2[column_name] = prefixes_column_names[1] + column_name

    new_columns_table = []
    for column_name in column_to_include_t1:
        if column_name in column_name_maps_t1:
            new_column_name = column_name_maps_t1[column_name]
        else:
            new_column_name = column_name

        new_type = column_types_dict1[column_name]
        new_columns_table += [sa.Column(new_column_name, new_type)]

    for column_name in column_to_include_t2:
        if column_name in column_name_maps_t2:
            new_column_name = column_name_maps_t2[column_name]
        else:
            new_column_name = column_name

        new_type = column_types_dict2[column_name]
        new_columns_table += [sa.Column(new_column_name,  new_type)]

    new_table = sa.Table(new_table, metadata, *new_columns_table)
    metadata.create_all()

    #TODO: Add the select into statement; and the join statements