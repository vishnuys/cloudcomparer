#!/usr/bin/env python

import sys
import os

# inputs from parser
t1 = sys.argv[1]
t2 = sys.argv[2]
t1_key = int(sys.argv[3])
t2_key = int(sys.argv[4])

# local key for each tuple
key = None

for line in sys.stdin:
    # input preprocessing
    line = line.strip().replace("\'", '').replace('\"', '')
    values = line.split(',')
    filename = os.getenv('map_input_file')

    # append table name to end of each tuple
    values.append(filename.split('/')[-1].split('.', 1)[0])

    # decide key on the basis of table
    if t1 == values[-1]:
        key = values[t1_key]
    else:
        key = values[t2_key]

    print(key, end='\t')
    print(*values, sep=',')
