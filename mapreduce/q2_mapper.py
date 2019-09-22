#!/usr/bin/env python3
import sys

group_by_cols = sys.argv[1:]

for line in sys.stdin:
    line = line.strip().replace('\"','').replace("\'",'')
    line = line.split(',')
    
    key = []
    for col in group_by_cols:
        key.append(line[int(col)])
    print (*key,sep=',',end='\t')
    print (*line,sep=',')
