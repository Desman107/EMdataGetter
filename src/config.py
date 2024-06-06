import os

project_root = ''

csv_db_path = ''

duck_db_path = ''



if project_root == '':
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

if csv_db_path == '':
    csv_db_path = os.path.join(project_root, 'data', 'csvdb')

if duck_db_path == '':
    duck_db_path = os.path.join(project_root, 'data', 'duckdb')



if not os.path.exists(csv_db_path):
    os.makedirs(csv_db_path)
    print(f'create csvdb dir: {csv_db_path}')
if not os.path.exists(duck_db_path):
    os.makedirs(duck_db_path)
    print(f'create duckdb dir: {duck_db_path}')
