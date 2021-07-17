import os
import configparser
import psycopg2
import pymysql.cursors
import pandas as pd
import cx_Oracle
from sqlalchemy import create_engine
inifile = configparser.ConfigParser()
inifile.read(os.path.join(os.path.dirname(os.path.abspath(__file__)), "./../config.ini"), "utf-8")


class PGSQL:
    # PostgreSQL接続用クラス

    def __init__(self, db):
        # config.iniファイルをもとに初期化
        self.host = inifile.get("postgres", "host")
        self.user = inifile.get("postgres", "user")
        self.password = inifile.get("postgres", "password")
        self.port = inifile.get("postgres", "port")
        self.db = db
        self.conn = None
        self.cur = None
    
    def connect(self):
        # データベース接続
        self.conn = psycopg2.connect("dbname={} host={} user={} password={}".format(self.db, self.host, self.user, self.password))
        self.cur = self.conn.cursor()
        print("connect {}".format(self.db))

    def disconnect(self):
        # データベース接続断
        self.conn.close()
        self.cur.close()
        print("disconnect {}".format(self.db))

    def select(self,sql):
        self.cur.execute(sql)
        result = self.cur.fetchall()
        return result
    def execute(self, sql):
        # SQL実行
        self.cur.execute(sql)
        self.conn.commit()



class MySQL:
    # MySQL接続用クラス

    def __init__(self, db):
        # config.iniファイルをもとに初期化
        self.host = inifile.get("mysql", "host")
        self.user = inifile.get("mysql", "user")
        self.password = inifile.get("mysql", "password")
        self.port = inifile.get("mysql", "port")
        self.db = db
        self.conn = None
        self.cur = None
    
    def connect(self):
        # データベース接続
        self.conn = pymysql.connect(host=self.host,
                                    user=self.user,
                                    password=self.password,
                                    db=self.db,
                                    charset="utf8mb4",
                                    cursorclass=pymysql.cursors.DictCursor)
        self.cur = self.conn.cursor()
        print("connect {}".format(self.db))
    
    def disconnect(self):
        # データベース接続断
        self.conn.close()
        self.cur.close()
        print("disconnect {}".format(self.db))

    def execute(self, sql):
        # SQL実行
        self.cur.execute(sql)    
        result = self.cur.fetchall()
        self.conn.commit()
        return result
    def write_to_csv(self,sql,file):
        results = pd.read_sql_query(sql,self.conn)
        results.to_csv(file+".csv",index=False)



# class Sqlalchemy:
#     # Oracle接続用クラス
#
#     def __init__(self):
#         # config.iniファイルをもとに初期化
#         self.host = inifile.get("oracle","host")
#         self.user = inifile.get("oracle", "user")
#         self.password = inifile.get("oracle", "password")
#         self.port = inifile.get("oracle","port")
#         self.service_name = inifile.get("oracle","service_name")
#
#     def connect(self):
#         # データベース接続
#         # mydb = create_engine('mysql+pymysql://' + self.user + ':' + self.password + '@' + self.host + ':' + str(self.port) + '/' + self.db,
#         #                      echo=False)
#         oracle_connection_string = (
#                 'oracle+cx_oracle://{username}:{password}@' +
#                 cx_Oracle.makedsn('{hostname}', '{port}', service_name='{service_name}')
#         )
#
#         engine = create_engine(
#             oracle_connection_string.format(
#                 username=self.user,
#                 password=self.password,
#                 hostname=self.host,
#                 port=self.port,
#                 service_name = self.service_name
#             )
#         )
#         return engine

class Oracle:
    # Oracle接続用クラス

    def __init__(self):
        self.user = inifile.get("oracle","user")
        self.password = inifile.get("oracle","password")
        self.dsn = inifile.get("oracle","dsn")
        self.conn = None
        self.cur = None

    def connect(self):
        self.conn = cx_Oracle.connect(user=self.user,
                                      password=self.password,
                                      dsn=self.dsn)
        self.cur = self.conn.cursor()
        print("connect to oracle")

    def execute(self, sql):
        # SQL実行
        self.cur.execute(sql)
        # result = self.cur.fetchall()
        self.conn.commit()
        # return result

    def select(self,sql):
        self.cur.execute(sql)
        result = self.cur.fetchall()
        return result

    def disconnect(self):
        # データベース接続断
        self.conn.close()
        self.cur.close()
        print("disconnect {}".format(self.db))





def main():
    pass
    # sql = "select osm_id from public.planet_osm_point where amenity='bench'"
    # sql = "alter table public.planet_osm_point add column lon numeric"
    # sql = "alter table public.planet_osm_point drop column lat"
    # pgsql = PGSQL("osm_kanto_test")
    # pgsql.connect()
    # pgsql.execute(sql)
    # pgsql.disconnect()
    # mysql = MySQL("osmdb")
    # mysql.connect()
    # mysql.disconnect()
    # oracle = Oracle()
    # oracle.connect()
    # sqlalchemy = Sqlalchemy()
    # sqlalchemy.connect()

if __name__ == "__main__":
    main()
