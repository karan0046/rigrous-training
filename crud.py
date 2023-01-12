import psycopg2, json, yaml, array as arr

class CrudOperation:
    def __init__(self) -> None:
        pass

    #database creation
    def create_database(self, config_file, conn_text):
        conn = psycopg2.connect(conn_text)
        cursor = conn.cursor()
        conn.autocommit = True
        with open(config_file, 'r') as f:
            d = f.read()
        obj = json.loads(d)
        for i in obj['dbname']:
            cursor.execute("create database " + i)
        conn.close()
    
    #table creation
    def create_table(self, config_file, conn_text):
        conn = psycopg2.connect(conn_text)
        cursor = conn.cursor()
        conn.autocommit = True
        with open(config_file, 'r') as f:
            d = f.read()
        obj = json.loads(d)

        #query for creating table 
        table_name = obj['tbname'][0]
        column = obj['col']
        list_type = []
        list_col_name = []
        for i in obj['col']:
            for j in i['type']:
                list_type.append(j)
        for i in obj['col']:
            for j in i['name']:
                list_col_name.append(j)

        #query
        query = "create table " + table_name + "(" 
        for i in range(len(list_col_name)):
            query += list_col_name[i] + " " + list_type[i]
            if i+1 == len(list_col_name):
                continue
            else:
                query += ','
        query += ");"
        
        cursor.execute(query)
        conn.close()

    #read_table
    def read_table(self, config_file, conn_text):
        conn = psycopg2.connect(conn_text)
        cursor = conn.cursor()
        conn.autocommit = True

        with open(config_file, 'r') as f:
            d = f.read()
        obj = json.loads(d)
        table_name = obj['tbname'][0]
        cursor.execute("select * from " + table_name)
        t = cursor.fetchall()
        print(t)
        conn.close()

    def list_to_string(self,s):
        str1 = ""
        i = 0
        for ele in s:
            i += 1
            str1 += "'"
            str1 += ele
            if i != len(s):
                str1 += "', "
            if i == len(s):
                str1 += "'"
        return str1
        
    #insertion in table
    def insert_into_table(self, config_file, query_file, conn_text):
        conn = psycopg2.connect(conn_text)
        cursor = conn.cursor()
        conn.autocommit = True

        with open(config_file, 'r') as f:
            d = f.read()
        obj = json.loads(d)
        table_name = obj['tbname'][0]

        with open(query_file, 'r') as f:
            d = f.read()
        obj = json.loads(d)

        query = "insert into " + table_name + " values ("
        for i in obj['insert']:
            tmp = query
            tmp += self.list_to_string(i)
            tmp += ");"
            cursor.execute(tmp)

        conn.close()

    #deletion_from_table
    def delete_from_table(self, config_file, query_file, conn_text):
        conn = psycopg2.connect(conn_text)
        cursor = conn.cursor()
        conn.autocommit = True

        with open(config_file, 'r') as f:
            d = f.read()
        obj = json.loads(d)
        table_name = obj['tbname'][0]

        with open(query_file, 'r') as f:
            d = f.read()
        obj = json.loads(d)
        
        query = "delete from " + table_name + " where "
        for i in obj['delete']:
            tmp = query
            for j in i:
                tmp += j
                tmp += " = '"
                for k in i[j]:
                    tmp += k
                tmp += "';"
                
            # print(tmp)
            cursor.execute(tmp)

        conn.close()
        

if __name__ == "__main__":

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
    
    obj = CrudOperation()
    #obj.create_database(config_file, conn_text)
    #obj.create_table(config_file, conn_text)
    obj.read_table(config_file, conn_text)
    #obj.insert_into_table(config_file, query_file, conn_text)
    #obj.delete_from_table(config_file, query_file, conn_text)
