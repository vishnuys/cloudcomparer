#!/usr/bin/env python3
import sys

#
#   process query 2
#
#   map output =>  <group_by_cols(tuple), tuple>
#


# get list of group_by columns
try:
    group_by_cols = list(map(int, sys.argv[1:]))
except:
    print("DATA_MISMATCH")
    quit()

for line in sys.stdin:
    # input preprocessing 
    line = line.strip().replace('\"','').replace("\'",'')
    line = line.split(',')
    
    key = []
    for col in group_by_cols:
        # forming key
        key.append(line[ col ])
    print (*key,sep=',',end='\t')
    print (*line,sep=',')
