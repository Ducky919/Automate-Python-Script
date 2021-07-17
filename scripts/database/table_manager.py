import json
import pandas as pd
from worldmesh import worldmesh


class Spot:
    # spotテーブルの定義クラス
    # とりあえず連番でspot_idを振る。※ ここのロジックは良多さん, 大森さんと検討する必要あり。
    spot_id = 0
    def __init__(self):
        # フィールドごとに値を初期化
        Spot.spot_id += 1
        self.spot_id = Spot.spot_id
        self.category_cd = 0
        self.display_type = 0
        self.spot_name = ""
        self.zoom = 0
        self.center_lat = 0
        self.center_lon = 0
        self.max_lat = 0
        self.max_lon = 0
        self.min_lat = 0
        self.min_lon = 0
        self.geojson = None
        self.meshcode3 = 0
        self.source_id = None
        self.display_flg = 1
    def set_attributes_from_osm(self, attr,country_number,process_number):
        # postgresqlのosmテーブルをspotテーブルの属性に変換するロジック実装メソッド
        country_number = str(country_number)
        process_number = str(process_number)
        self.spot_id = int(country_number + process_number + str(self.spot_id))


        geometry = json.loads(attr["geojson"])
        # set display_type
        if geometry["type"] == "Point": self.display_type = 0
        elif geometry["type"] == "Polygon": self.display_type = 1
        else: self.display_type = 2
        
        # set geographic information
        self.center_lat = float(attr["center_lat"])
        self.center_lon = float(attr["center_lon"])
        self.geojson = attr["geojson"]
        self.meshcode3 = int(worldmesh.cal_meshcode3(self.center_lat, self.center_lon))


        # set osm_id as source_id
        self.source_id = attr["osm_id"]

        # set max_lat and max_lon for point, linestring and polygon
        if geometry["type"] == "Point":
            self.max_lat = attr["center_lat"]
            self.min_lat = attr["center_lat"]
            self.min_lon = attr["center_lon"]
            self.max_lon = attr["center_lon"]
        elif geometry["type"] == "LineString":
            lat_list = []
            lon_list = []
            for i in range(len(geometry['coordinates'])):
                lon_list.append(geometry['coordinates'][i][0])
                lat_list.append(geometry['coordinates'][i][1])
            self.max_lat = max(lat_list)
            self.min_lat = min(lat_list)
            self.max_lon = max(lon_list)
            self.min_lon = min(lon_list)
        elif geometry["type"] == "Polygon":
            lat_list = []
            lon_list = []
            for i in range(len(geometry['coordinates'])):
                for i1 in geometry['coordinates'][i]:
                    lon_list.append(i1[0])
                    lat_list.append(i1[1])
            self.max_lat = max(lat_list)
            self.min_lat = min(lat_list)
            self.max_lon = max(lon_list)
            self.min_lon = min(lon_list)

            
    def make_list(self):
        # 各フィールドの値を代入後, このメソッドで行化(もしくは行列化)
        # ※未実装!!!
        return [self.spot_id, self.display_type, self.category_cd,self.spot_name,self.zoom, self.center_lat, self.center_lon, self.max_lat, self.max_lon, self.min_lat, self.min_lon, self.geojson, self.meshcode3, self.source_id, self.display_flg]
    def only_id(self):
        return self.spot_id
    def get_spot_as_dataframe(self):
        # リストをデータフレームとして返すメソッド
        return pd.DataFrame([self.make_list()], columns=[
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

class TagMappingSpot:
    # tag_mapping_spotテーブルの定義クラス
    def __init__(self, spot_id, attr, definition):
        # 初期化メソッド
        self.spot_id = spot_id
        self.attr = attr
        self.definition = definition
        self.tag_mapping_spot_list = []
    def make_list(self):
        # tag化するkeyとvalueの組み合わせリストを返すメソッド
        for key in self.definition:
            if key in self.attr.keys():
                if self.attr[key] is not None and self.attr[key] in self.definition[key].keys():
                    self.tag_mapping_spot_list.append([self.spot_id, int(self.definition[key][self.attr[key]])])
                    # print(self.spot_id, self.definition[key][self.attr[key]])
        return self.tag_mapping_spot_list

    def get_tag_mapping_spot_as_dataframe(self):
        # リストをデータフレームとして返すメソッド
        return pd.DataFrame(self.make_list(), columns=["spot_id", "tag_id"])







