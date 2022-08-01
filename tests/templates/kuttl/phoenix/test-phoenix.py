import subprocess


print('''
Create, populate, and query a phoenix table
===========================================''')
output = subprocess.check_output(['/stackable/phoenix/bin/psql.py',
                                  '/stackable/phoenix/examples/WEB_STAT.sql',
                                  '/stackable/phoenix/examples/WEB_STAT.csv',
                                  '/stackable/phoenix/examples/WEB_STAT_QUERIES.sql'])

result = {}
for row in output.decode('utf-8').split('\n'):
    s = row.split()
    if len(s) > 1:
        print({s[0]:[str(y) for y in s[1:]]})
        result[s[0]] = [str(y) for y in s[1:]]

print('''
Check parsed results
====================''')
assert result['EU'][0] == '150'
assert result['NA'][0] == '1'
