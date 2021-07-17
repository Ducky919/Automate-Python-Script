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
inifile = configparser.ConfigParser()
inifile.read(os.path.join(os.path.dirname(os.path.abspath(__file__)), "./config.ini"), "utf-8")
PROJROOT = inifile.get("project", "root")

# ---------------------------------------------------------------------------------------------
#初設定
country_number = 221 # 国の先頭ID(OSM_inforのExcelファイル参考)
process_number = 200 # Process数(限界290まで)
table = 'line'    # Postgresqlのテーブルタイプ
database_name = 'osm_gb' # Postgresqlのデータベース名
# ログの設定
logging.basicConfig(filename="../log/process_{}_{}_{}.log".format(today_string,database_name,table),
                            filemode='a',
                            format='%(asctime)s - %(levelname)s  \n%(message)s',
                            datefmt='%H:%M:%S',
                            level=logging.INFO)
#----------------------------------------------------------------------------------------------

# PostgreSQLで重心点を計算する関数
def calculate_centroid(dbobj):
    print("calculate_centroid")
    tbls = ["planet_osm_point", "planet_osm_polygon", "planet_osm_line"]

    # osmのテーブルに追加するフィールド
    geomtxtfld = "geom_text"
    latfld = "center_lat"
    lonfld = "center_lon"

    # ポイント, ポリゴン, ラインテーブルそれぞれに実行
    for tbl in tbls:
        # フィールドリストを取得し追加フィールドが無ければフィールド追加
        dbobj.execute("select column_name from information_schema.columns where table_name = '{}'".format(tbl))
        flds = [fld[0] for fld in dbobj.cur.fetchall()]
        if geomtxtfld not in flds:
            dbobj.execute("alter table public.{} add column {} text".format(tbl, geomtxtfld))
        if latfld not in flds:
            dbobj.execute("alter table public.{} add column {} numeric".format(tbl, latfld))
        if lonfld not in flds:
            dbobj.execute("alter table public.{} add column {} numeric".format(tbl, lonfld))

        # 重心点geometryを計算、webメルカトルからwgs84に変換しテキストで格納
        dbobj.execute("update {} set {} = ST_AsText(ST_Transform(ST_Centroid(way), 4326))".format(tbl, geomtxtfld))
        # lat, lonフィールドに格納
        dbobj.execute("update {0} set {1} = to_number(trim(split_part(replace(replace({2}, 'POINT(',''), ')',''), ' ',2)),'999999999999999D999999') where {2} <> 'POINT EMPTY';".format(tbl, latfld, geomtxtfld))
        dbobj.execute("update {0} set {1} = to_number(trim(split_part(replace(replace({2}, 'POINT(',''), ')',''), ' ',1)),'999999999999999D999999') where {2} <> 'POINT EMPTY';".format(tbl, lonfld, geomtxtfld))
    print("calculate_centroid done")

# 各osmテーブルにgeojsonフィールドを追加する関数
def add_geojson(dbobj):
    print("add_geojson")
    tbls = ["planet_osm_point", "planet_osm_polygon", "planet_osm_line"]
    geojsonfld = "geojson"
    for tbl in tbls:
        dbobj.execute("select column_name from information_schema.columns where table_name = '{}'".format(tbl))
        flds = [fld[0] for fld in dbobj.cur.fetchall()]
        # フィールド追加
        if geojsonfld not in flds:
            dbobj.execute("alter table public.{} add column {} text".format(tbl, geojsonfld))
        # geojsonを格納
        # if tbl == "planet_osm_polygon":
        #     dbobj.execute("update {} set {} = ST_AsGeoJSON(ST_Simplify(ST_Transform(way, 4326),0.005))".format(tbl, geojsonfld))
        # else:
        dbobj.execute("update {} set {} = ST_AsGeoJSON(ST_Transform(way, 4326))".format(tbl, geojsonfld))
    print("add_geojson done")


# 各テーブルのフィールドリストを返す関数
def get_cols(dbobj):
    cols = {}
    tbls = ["planet_osm_point", "planet_osm_polygon", "planet_osm_line"]
    for tbl in tbls:
        dbobj.execute("select column_name from information_schema.columns where table_name = '{}'".format(tbl))
        flds = [fld[0] for fld in dbobj.cur.fetchall()]
        cols[tbl] = flds
    return cols



# データ処理の関数
def make_spot(dbobj, cols, tagid_definition,country_number,process_number,start_offset,finish_range):
    try:
        tbls = ["planet_osm_{}".format(table)]
        # レコードを10000件ごとに取得
        limit = 10000
        offset = start_offset
        for tbl in tbls:
            # 反復回数を計算
            iteration = int((finish_range - offset) / limit) + 1
            logging.info("Process number:{},loop_count:{}".format(process_number,iteration))
            time = 1
            for i in range(iteration):
                if time == iteration:
                    limit = finish_range - offset + 1
                else:
                    limit = 10000
                logging.info(
                    "Start Process number:{},Table:{},Offset:{},Finish Range:{},Times:{}".format(process_number, tbl, offset,
                                                                                        finish_range,time))
                # spotテーブル作成用のデータフレーム
                spot_df = pd.DataFrame(columns=[
                    "spot_id",
                    "display_type",
                    "category_cd",
                    "spot_name",
                    "zoom",
                    "center_lat",
                    "center_lon",
                    "max_lat",
                    "max_lon",
                    "min_lat",
                    "min_lon",
                    "geojson",
                    "meshcode3",
                    "source_id",
                    "display_flg"
                ])
                # tag_mapping_spotテーブル作成用データフレーム
                tag_mapping_spot_df = pd.DataFrame(columns=[
                    "spot_id", "tag_id"
                ])
                # 指定のlimitとoffsetでデータを取得
                dbobj.execute("select * from {} order by osm_id asc limit {} offset {}".format(tbl, limit, offset))
                cursor = dbobj.cur.fetchall()
                # レコードごとにspotテーブル, tag_mapping_spotテーブル
                for row in cursor:
                    attributes = {cols[tbl][i]:val for i, val in enumerate(row)}
                    # spotテーブル用のレコードを取得
                    spotobj = tm.Spot()
                    spotobj.set_attributes_from_osm(attributes,country_number,process_number)
                    spot_df = pd.concat([spot_df, spotobj.get_spot_as_dataframe()])

                    # tag_mapping_spotテーブル用レコードを取得しデータフレーム化
                    obj = spotobj.only_id()
                    tag_mapping_spot_df = pd.concat([tag_mapping_spot_df, make_tag_mapping_spot(obj, attributes, tagid_definition)])
                    tag_mapping_spot_df = tag_mapping_spot_df.drop_duplicates()
                # Oracleテーブルに書き出し (本番用)
                insert_into_db(spot_df, "spot_line")
                insert_into_db(tag_mapping_spot_df, "tag_mapping_spot_line")
                logging.info(
                    "Finish put Dataframe to Oracle , Process number:{}, Table:{}".format(process_number, tbl))
                time += 1
                # offsetを更新
                offset += limit
    except Exception as e:
        logging.error(e)

# データフレームをMySQLのテーブルに書き出す関数
def insert_into_db(df, table):
    try:
        conn = create_engine("oracle+cx_oracle://propre_develop:Welcome12345$$@prdatp_tp")
        df.to_sql(name=table, con=conn, if_exists='append', index=False)
        conn.dispose()
    except exc.IntegrityError:
        pass

# tag_mapping_spotデータフレーム作成関数
def make_tag_mapping_spot(spot_id, attributes, definition):
    tag_mapping_spot = tm.TagMappingSpot(spot_id, attributes, definition)
    tag_mapping_spot_df = tag_mapping_spot.get_tag_mapping_spot_as_dataframe()
    return tag_mapping_spot_df    


# keyとvalueの組み合わせによるtag_id定義の読み込み関数
def make_tagid_definition(filename):
    tagid_def = {}
    with open(os.path.join(PROJROOT, "definition", filename), encoding="utf-8") as f:
        for line in f:
            splitline = line.strip().split("\t")
            key = splitline[0]
            value = splitline[1]
            tagid = splitline[2]
            if key not in tagid_def.keys():
                tagid_def.update({key:{}})
            tagid_def[key].update({value:tagid})
    return tagid_def


# # データフレームをcsvに書き出す関数
# def insert_into_csv(df, table):
#     df.to_csv(os.path.join(PROJROOT, "csv", "{}.csv".format(table)), index=False, header=True, mode="a")


# def make_spot_name():
#     oracle = dbm.Oracle()
#     oracle.connect()
#     oracle.execute("UPDATE spot s "
#                    "SET s.spot_name = (SELECT max(def_table.tag_ja) as f2 "
#                    "FROM def_table "
#                    "INNER JOIN tag_mapping_spot "
#                    "ON tag_mapping_spot.tag_id = def_table.tag_id "
#                    "WHERE tag_mapping_spot.SPOT_ID = s.SPOT_ID) "
#                    "WHERE s.spot_id in (select spot_id from tag_mapping_spot)")
#     oracle.disconnect()




# Meshcode6のリストから重心点計算、OracleにImportの関数
def cal_geo_text_to_mysql(file,table):
    df = pd.read_csv(file +".csv",encoding='utf-8')
    field = "meshcode6"
    df["ne_lat"] = df[field].apply(lambda x: worldmesh.meshcode_to_latlong_NE(x)["lat"])
    df["ne_lon"] = df[field].apply(lambda x: worldmesh.meshcode_to_latlong_NE(x)["long"])
    df["sw_lat"] = df[field].apply(lambda x: worldmesh.meshcode_to_latlong_SW(x)["lat"])
    df["sw_lon"] = df[field].apply(lambda x: worldmesh.meshcode_to_latlong_SW(x)["long"])
    df["center_lat"] = (df["ne_lat"] + df["sw_lat"]) / 2
    df["center_lon"] = (df["ne_lon"] + df["sw_lon"]) / 2
    df = df[['meshcode6', 'center_lat', 'center_lon']]
    insert_into_db(df,table)



#posgresqlからOracleに取り込む関数
def postgresql_to_oracle(country,country_number,process_number,offset,finish_range):
    tagid_def = make_tagid_definition("SAMPLE_def_tagid.tsv")
    pgsql = dbm.PGSQL(country)
    pgsql.connect()
    cols = get_cols(pgsql)
    make_spot(pgsql, cols, tagid_def,country_number,process_number,offset,finish_range)
    pgsql.disconnect()


# Multi用リスト作成関数：
def create_list(database_name, process_number, table):
    country_list = []
    country_list_clone = [country_list.append('{}'.format(database_name)) for x in range(process_number)]

    country_number_list = []
    country_number_list_clone = [country_number_list.append(country_number) for x in range(process_number)]

    process_list = ["{0:03}".format(x) for x in range(1, process_number + 1)]

    offset_list = []
    finish_range_list = []
    count_table = 0
    pgsql = dbm.PGSQL('{}'.format(database_name))
    pgsql.connect()
    point = pgsql.select("SELECT COUNT(*) FROM planet_osm_point")
    line = pgsql.select("SELECT COUNT(*) FROM planet_osm_line")
    polygon = pgsql.select("SELECT COUNT(*) FROM planet_osm_polygon")
    point = point[0][0]
    line = line[0][0]
    polygon = polygon[0][0]
    pgsql.disconnect()
    if table == 'line':
        count_table = line
    elif table == 'point':
        count_table = point
    elif table == 'polygon':
        count_table = polygon
    divine = round(count_table / process_number)
    for i in range(process_number + 1):
        offset_list.append(divine * i + 1)
    for a in offset_list[1:]:
        finish_range_list.append(a - 1)
    offset_list[0] = 0
    offset_list.pop(-1)
    finish_range_list[-1] = count_table
    return offset_list, finish_range_list, country_list, country_number_list, process_list


if __name__ == "__main__":

    # pgsql = dbm.PGSQL("{}".format(database_name))
    # pgsql.connect()
    # calculate_centroid(pgsql)
    # add_geojson(pgsql)
    # pgsql.disconnect()


# Postgresql to Oracle (Multi Process)
    starttime = time.time()
    result = create_list(database_name, process_number, table)
    offset_list = result[0]
    finish_range_list = result[1]
    country_list = result[2]
    country_number_list = result[3]
    process_list = result[4]
    
    logging.info(offset_list)
    logging.info(finish_range_list)
    logging.info(country_list)
    logging.info(country_number_list)
    logging.info(process_list)


    procs = []
    proc = Process(target=postgresql_to_oracle)
    procs.append(proc)
    proc.start()
    for a,b,c,d,e in zip(country_list,country_number_list,process_list,offset_list,finish_range_list):
        proc = Process(target=postgresql_to_oracle,args=(a,b,c,d,e,))
        procs.append(proc)
        proc.start()
    for proc in procs:
        proc.join()
    print('That took {} seconds'.format(time.time() - starttime))
    logging.info('That took {} seconds'.format(time.time() - starttime))