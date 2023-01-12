import psycopg2, yaml, json
from crud import CrudOperation

def test_database_created():
    con = psycopg2.connect(conn_text)
    con.autocommit = True
    cur = con.cursor()

    with open(config_file, 'r') as f:
        d = f.read()
    obj = json.loads(d)
    db_name = obj['dbname'][0]
    CrudOperation().create_database(config_file, conn_text)
    cur.execute("select datname from pg_database")
    list_all_database = cur.fetchall()
    if (db_name,) in list_all_database:
        assert 1 == 1
    else:
        assert 0 == 1
    ##cur.execute("drop database if exists " + db_name)
    con.close()

def test_table_creation():
    con = psycopg2.connect(conn_text)
    con.autocommit = True
    cur = con.cursor()
    with open(config_file, 'r') as f:
        d = f.read()
    obj = json.loads(d)
    table_name = obj['tbname'][0]
    print(table_name)

    CrudOperation().create_table(config_file, conn_text)

    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE'")
    list_all_table = cur.fetchall()
    print(list_all_table)

    if (table_name,) in list_all_table:
        assert 1 == 1
    else:
        assert 0 == 1
    ##cur.execute("drop table if exists " + table_name)
    con.close()

def test_insertion_in_table():
    con = psycopg2.connect(conn_text)
    con.autocommit = True
    cur = con.cursor()

    with open(config_file, 'r') as f:
        d = f.read()
    obj = json.loads(d)
    table_name = obj['tbname'][0]

    cur.execute("select * from " + table_name)
    bef = cur.fetchall()
    CrudOperation().insert_into_table(config_file, query_file, conn_text)
    cur.execute("select * from " + table_name)
    aft = cur.fetchall()

    if len(bef) < len(aft):
        assert 1 == 1
    else:
        assert 0 == 1
    #cur.execute("drop table if exists temporary")
    con.close()

def test_deletion_from_table():
    con = psycopg2.connect(conn_text)
    con.autocommit = True
    cur = con.cursor()

    with open(config_file, 'r') as f:
        d = f.read()
    obj = json.loads(d)
    table_name = obj['tbname'][0]

    cur.execute("select * from " + table_name)
    bef = cur.fetchall()
    CrudOperation().delete_from_table(config_file, query_file, conn_text)
    cur.execute("select * from " + table_name)
    aft = cur.fetchall()

    if len(bef) > len(aft):
        assert 1 == 1
    else:
        assert 0 == 1
    
    con.close()



f = open("credential.yaml", 'r')
dic = yaml.load(f, Loader = yaml.FullLoader)


config_file = dic['config_file']
query_file = dic['query_file']
user = dic['user']
password = dic['password']
port = dic['port']
database = dic['database']
host = dic['host']
conn_text = f"postgresql://{user}:{password}@{host}:{port}/{database}"
