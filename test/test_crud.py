import psycopg2, yaml

def test_database_created():
    con = psycopg2.connect(conn_text)
    con.autocommit = True
    cur = con.cursor()

    cur.execute("create database temporary")
    cur.execute("select datname from pg_database")
    list_all_database = cur.fetchall()

    if ('temporary',) in list_all_database:
        assert 1 == 1
    else:
        assert 0 == 1
    cur.execute("drop database if exists temporary")
    con.close()

def test_table_creation():
    con = psycopg2.connect(conn_text)
    con.autocommit = True
    cur = con.cursor()
    cur.execute("drop table if exists temporary")
    cur.execute("create table temporary (name varchar)")
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE'")
    list_all_table = cur.fetchall()
    if ('temporary',) in list_all_table:
        assert 1 == 1
    else:
        assert 0 == 1
    cur.execute("drop table if exists temporary")
    con.close()

def test_insertion_in_table():
    con = psycopg2.connect(conn_text)
    con.autocommit = True
    cur = con.cursor()
    cur.execute("drop table if exists temporary")
    cur.execute("create table temporary (name varchar)")
    cur.execute("select * from temporary")
    bef = cur.fetchall()
    cur.execute("insert into temporary values ('temp_name')")
    cur.execute("select * from temporary")
    aft = cur.fetchall()

    if len(bef) + 1 == len(aft):
        assert 1 == 1
    else:
        assert 0 == 1
    cur.execute("drop table if exists temporary")
    con.close()

def test_deletion_from_table():
    con = psycopg2.connect(conn_text)
    con.autocommit = True
    cur = con.cursor()
    cur.execute("drop table if exists temporary")
    cur.execute("create table temporary (name varchar)")
    cur.execute("insert into temporary values ('temp_name')")
    cur.execute("select * from temporary")
    bef = cur.fetchall()
    cur.execute("delete from temporary where name = 'temp_name'")
    cur.execute("select * from temporary")
    aft = cur.fetchall()

    if len(bef) == 1 + len(aft):
        assert 1 == 1
    else:
        assert 0 == 1
    cur.execute("drop table if exists temporary")
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
