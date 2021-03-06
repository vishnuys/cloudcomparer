#!/usr/bin/env python3

# reducer for query 2:
# 
# reducer input: <group_by_cols, all_cols>
# reducer output: <group_by_cols, aggregate function output>
#
#

import sys

# input parameters : function name, parameter column, having value
func_name = (sys.argv[1]).upper()
func_param = int(sys.argv[2])
having_val_ = sys.argv[3]

# keep track of current key and its values
current_key = None
row_list = []

# apply aggregate function and print output
def transform_and_print(key):
    func_output_type = 'int'
    output_row = key.split(',')

    if func_name == 'COUNT':
        output_row.append(len(row_list))
    elif func_name == 'MAX':
        try:
            output_row.append(max(list(map(int, [ r[func_param] for r in row_list ]))))
        except:
            func_output_type = 'string'
            output_row.append(max([ r[func_param] for r in row_list ]))
    elif func_name == 'MIN':
        try:
            output_row.append(min(list(map(int, [ r[func_param] for r in row_list ]))))
        except:
            func_output_type = 'string'
            output_row.append(min([ r[func_param] for r in row_list ]))
    elif func_name == 'SUM':
        try:
            output_row.append(sum(list(map(int,[ r[func_param] for r in row_list ]))))
        except:
            print("DATA_MISMATCH")
            quit()

    if func_output_type == 'int':
        having_val = int(having_val_)
    else:
        having_val = having_val_
    if output_row[-1] > having_val:
        print(output_row)

for line in sys.stdin:
    # input preprocessing
    line = line.strip()
    key, value_list = line.split('\t',1)
    value_list = value_list.strip().split(',')

    if current_key == key:
        row_list.append(value_list)
    else:
        if current_key:
            transform_and_print(current_key)
        current_key = key
        row_list = []
        row_list.append(value_list)

# process the last key
transform_and_print(current_key)
        



