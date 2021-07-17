#!/usr/bin/env bash
# coding=UTF-8
import os
import configparser
import pandas as pd
from database import db_manager as dbm, table_manager as tm
from pymysql import IntegrityError
from worldmesh import worldmesh
import cx_Oracle
from sqlalchemy import types, create_engine
from sqlalchemy import exc
from multiprocessing import Process
import  time
from datetime import date
import datetime
import logging
import warnings
warnings.simplefilter("ignore")

today = date.today()
now = datetime.datetime.now()
today_string = today.strftime("%Y%m%d")
logging.basicConfig(filename="../log/process_{}_us_merge_tag_mapping.log".format(today_string),
                            filemode='a',
                            format='%(asctime)s - %(levelname)s  \n%(message)s',
                            datefmt='%H:%M:%S',
                            level=logging.INFO)

inifile = configparser.ConfigParser()
inifile.read(os.path.join(os.path.dirname(os.path.abspath(__file__)), "./config.ini"), "utf-8")

PROJROOT = inifile.get("project", "root")


def make_spot_geometry():
    try:
        oracle = dbm.Oracle()
        oracle.connect()
        oracle.execute("INSERT INTO propre_develop.spot_geometry(spot_id,geo) SELECT spot_id,SDO_UTIL.FROM_GEOJSON(geojson) FROM propre_develop.spot")
        oracle.disconnect()
    except Exception as e:
        logging.error(e)

def make_spot_name():
    oracle = dbm.Oracle()
    oracle.connect()
    oracle.execute("UPDATE spot s "
                   "SET s.spot_name = (SELECT max(def_table.tag_ja) as f2 "
                   "FROM def_table "
                   "INNER JOIN tag_mapping_spot "
                   "ON tag_mapping_spot.tag_id = def_table.tag_id "
                   "WHERE tag_mapping_spot.SPOT_ID = s.SPOT_ID) "
                   "WHERE s.spot_id in (select spot_id from tag_mapping_spot)")
    oracle.disconnect()


def insert_into_csv(df, table):
    # データフレームをcsvに書き出す関数
    df.to_csv(os.path.join(PROJROOT, "csv", "{}.csv".format(table)), index=False, header=True, mode="a")


def insert_into_db(df, table):
    # データフレームをMySQLのテーブルに書き出す関数 (未実装)
    # sqlalchemy = dbm.Sqlalchemy()
    # conn = sqlalchemy.connect()
    try:
        conn = create_engine("oracle+cx_oracle://propre_develop:Welcome12345$$@prdatp_tp")
        df.to_sql(name=table, con=conn, if_exists='append', index=False)
    except exc.IntegrityError:
        pass


def get_meshcode6(file):
    mysql = dbm.MySQL("osm_test_luong")
    mysql.connect()
    mysql.write_to_csv("SELECT meshcode6 FROM meshcode6_only",file)
    mysql.disconnect()


def cal_geo_text_to_mysql(file,table):
    df = pd.read_csv(file +".csv",encoding='utf-8')
    field = "meshcode6"
    df.columns = ['meshcode6']
    df["ne_lat"] = df[field].apply(lambda x: worldmesh.meshcode_to_latlong_NE(x)["lat"])
    df["ne_lon"] = df[field].apply(lambda x: worldmesh.meshcode_to_latlong_NE(x)["long"])
    df["sw_lat"] = df[field].apply(lambda x: worldmesh.meshcode_to_latlong_SW(x)["lat"])
    df["sw_lon"] = df[field].apply(lambda x: worldmesh.meshcode_to_latlong_SW(x)["long"])
    df["center_lat"] = (df["ne_lat"] + df["sw_lat"]) / 2
    df["center_lon"] = (df["ne_lon"] + df["sw_lon"]) / 2
    df = df[['meshcode6', 'center_lat', 'center_lon']]
    insert_into_db(df,table)
    # df.to_csv("meshcode6.csv",index=False,header=True)


def make_tag_mapping_meshcode6():
    try:
        oracle = dbm.Oracle()
        oracle.connect()
        meshcode6_number = oracle.select("SELECT count(*) from meshcode6")
        meshcode6_total = meshcode6_number[0][0]
        loop_time = round(meshcode6_number[0][0] / 100)
        logging.info("Loop Time {}".format(loop_time))
        time = 1
        start = 1
        rangee = 100
        for i in range(loop_time +1):
            logging.info("Start {} to {} ".format(start, rangee))
            oracle.execute("INSERT /*+ ignore_row_on_dupkey_index(tag_mapping_meshcode6, TAG_MAPPING_MESHCODE6_PK) */ INTO tag_mapping_meshcode6(tag_id,meshcode6) "
                           "SELECT a.tag_id,b.meshcode6 "
                           "FROM meshcode6 b,tag_mapping_spot a "
                           "INNER JOIN spot_geometry c ON c.spot_id = a.spot_id "
                           "WHERE SDO_WITHIN_DISTANCE(c.geo,b.geo,'distance = 1 unit=KM') = 'TRUE' "
                           "AND b.id between {} and {}".format(start, rangee))
            logging.info("Finished {} to {}, Times {} ".format(start, rangee,time))
            start += 100
            rangee += 100
            time += 1
            if time == loop_time :
                rangee = meshcode6_total
        oracle.disconnect()
    except Exception as e:
        logging.error(e)



def merge_tag_mapping_meshcode6():
    try:
        oracle = dbm.Oracle()
        oracle.connect()
        meshcode6_number = oracle.select("SELECT count(*) from tag_mapping_meshcode6")
        meshcode6_total = meshcode6_number[0][0]
        loop_time = round(meshcode6_total / 100)
        logging.info("Loop Time {}".format(loop_time))
        time = 1
        start = 1
        rangee = 100
        for i in range(loop_time +1):
            logging.info("Start {} to {} ".format(start, rangee))
            oracle.execute("INSERT /*+ ignore_row_on_dupkey_index(tag_mapping_meshcode6, TAG_MAPPING_MESHCODE6_PK) */ INTO tag_mapping_meshcode6(meshcode6,tag_id,tag_level) " 
                           "SELECT meshcode6,tag_id,tag_level from tag_mapping_meshcode6 a "
                           "WHERE a.id between {} and {}".format(start, rangee))
            logging.info("Finished {} to {}, Times {} ".format(start, rangee,time))
            start += 100
            rangee += 100
            time += 1
            if time == loop_time :
                rangee = meshcode6_total
        oracle.disconnect()
    except Exception as e:
        logging.error(e)


if __name__ == "__main__":
    make_tag_mapping_meshcode6()
