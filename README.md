# tag_mapping


## 概要
本プロジェクトのゴールは以下の2つです。

1. PostgreSQLに読み込んだOSMデータをもとにspotテーブル、spot_geometryテーブル、tag_mapping_spotテーブルを構築。（テーブルはMySQL環境に構築予定）テーブル定義はreference/V2テーブル案_201907279.xlsx DB定義シート参照。

2. 6次メッシュとspot情報を紐づけ、tag_mapping_meshcode6
テーブルを作成。（これまでルオン君スクリプトでArcGISで処理していたくだりをPostGIS（or MySQL）で処理する）

#### データ共有用ドライブ
https://drive.google.com/drive/u/0/folders/1kf4f0rzuW1DZ0c4SP_QfcrJdA7k98xS8


## スポットタグ処理スクリプト

#### 1. 環境
python3.7のvenvを使用します。

#### 2. venvのセットアップ
cmdでプロジェクトフォルダに移動し、以下のコマンドでpython環境を構築します。

`$ {python3.7のexeのパス} -m venv {環境名}`

venvは以下でactivateします。
pipインストールは環境をactivateしてから行ってください。

`$ .\{環境名}\Scripts\activate`

もしくは

`$ {構築した環境へのフルパス}\Scripts\activate`

必要なモジュールのインストール

`$ pip install -r requirements.txt`

外部モジュールを新たに追加した場合は以下でrequirements.txtを更新しgitにコミット＆プッシュしてください。

`$ pip freeze > requirements.txt`

venvのdeactivate

`$ deactivate`

#### 3. configファイルの設定
/scripts/config.ini の [project] の中身をそれぞれのPCの環境に合わせてください


環境構築後test.pyを実行してテーブルの一覧が出力されれば設定は大丈夫です。


## 開発用サーバー
#### マシン
ubuntu

#### IP
都度コンソールで確認

#### rootユーザー
ubuntu

#### 鍵
gis.pem


## 開発用OSMデータベース
#### RDBMS
PostgreSQL + PostGIS

#### ユーザー
postgres

#### パスワード
postgres

#### DB
osm_kanto_test

## 開発用MySQL
#### RDBMS
MySQL

#### ユーザー
gis

#### パスワード
mysql

#### DB
未構築