#!/usr/bin/python3

import json
import os

with open('tables.json') as f:
	data = json.load(f)

tables = data['data']


table_types = set()
for t in tables:
    table_type = t['engine']
    if table_type not in table_types:
        table_types.add(table_type)

print(table_types)

def get_dir(t):
    return f'dump/{t["engine"]}/{t["name"]}.sql'

for table_type in table_types:
    try:
        os.makedirs(f'dump/{table_type}')
    except FileExistsError:
        pass

with open('tables.txt', 'w') as w:
	for t in tables:
		w.write(f"{t['engine']}:{t['name']}:{t['create_table_query'][:50]}\n")

for t in tables:
    with open(get_dir(t), 'w') as w:
        w.write(t['create_table_query'])