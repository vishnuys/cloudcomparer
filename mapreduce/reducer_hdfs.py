#!/usr/bin/env python

import sys

# inputs from parser
where_table = sys.argv[1]
where_col = sys.argv[2]
where_op = sys.argv[3]
where_val = sys.argv[4]

# common to a key
current_key = None
current_output_dict = {}

# local key values for each tuple
key = None
values = None


def where_condition(row):
    # implements the where condition of the query
    val = row[int(where_col)]
    if where_op == '=':
        return val == type(val)(where_val)
    elif where_op == '>':
        return val > type(val)(where_val)
    elif where_op == '<':
        return val < type(val)(where_val)
    elif where_op == '>=':
        return val >= type(val)(where_val)
    elif where_op == '<=':
        return val <= type(val)(where_val)
    elif where_op == '!=' or where_op == '<>':
        return val != type(val)(where_val)


def print_cartesian_product():
    # inner join implies both table tuples should have common attribute
    if len(current_output_dict.keys()) == 2:
        # dictionary stores tables as keys, tuples as values
        table1, table2 = current_output_dict.keys()
        if table1 == where_table:
            table1, table2 = table2, table1
        for row1 in current_output_dict[table1]:
            for row2 in current_output_dict[table2]:
                if where_condition(row2):
                    print(row1 + row2)


for line in sys.stdin:
    # input preprocessing
    line.strip()
    key, values = line.split('\t', 1)
    values = values.strip().split(',')

    # dictionary has seen the key before
    if current_key == key:
        if values[-1] not in current_output_dict.keys():
            current_output_dict[values[-1]] = []
        current_output_dict[values[-1]].append(values[:-1])
    else:
        # end of current key tuples
        if current_key:
            print_cartesian_product()
        # add this new key to a new dictionary
        current_key = key
        current_output_dict = {}
        current_output_dict[values[-1]] = []
        current_output_dict[values[-1]].append(values[:-1])
