# 提案ソフトウェア
# container-logs-to-metrics

## 構成
```
/home/c0a22166/teian
├── log_test.py
├── log_syuukei.cxv
```

## 実行する環境
- Python verion：3.12.3

## プログラムの概要

### `log_test.py`
**説明**:このファイルをPythonで動かすことで、Elasticsearchのbeats indexにあるログをコンテナ名別に30分ごとに時間帯を分けて出力されます。出力されるログはプログラム実行時の6時間前から現在時刻までのログであり，log_test.csvにコンテナ名別に30分ごとのログ件数が集計され出力されます。
- 入力は，Elasticsearchのbeats indexのログとログ検索を終了したい時間帯です。出力はプログラム実行時の6時間前から現在時刻までのログです。
- 出力では、ログの時間帯、コンテナ名、ログ件数、5分前のログ件数からの変化を出力します。
  
**使い方**:

<img width="844" height="223" alt="image" src="https://github.com/user-attachments/assets/33cb16a4-d47e-43e6-85fa-806dd9c3c5d9" />



**実行結果**
log_test.csvにコンテナ名別に30分ごとのログ件数が集計される

<img width="761" height="528" alt="image" src="https://github.com/user-attachments/assets/58b77f82-5a22-46ec-baf7-a6774a3a6e8b" />

### log_test2.py
**説明**:このファイルを動かすことで、log_test.csvの結果から30分ごとのログ件数の中央値を求め、最新の時間帯のログ件数と比較を行います。最新のログ件数が中央値より多く、差が1000件以上の場合に異常のある（障害に関連する）コンテナ名を出力します。

**使い方**:

<img width="765" height="216" alt="image" src="https://github.com/user-attachments/assets/90c60c4a-3904-41e8-bbb8-f8284849024e" />

**実行結果**

<img width="761" height="866" alt="image" src="https://github.com/user-attachments/assets/f6e8c7a8-7bc3-4253-b14e-d266721c88b1" />



**注意**
- elasticserchモジュールをインストールする必要があります。
  - インストールされていない場合は下記のコマンドを入力して、コマンドを実行して下さい。
 
```
# 仮想環境の作成
python3 -m venv venv

# 仮想環境の有効化
source venv/bin/activate

# elasticsearchパッケージのインストール
pip install elasticsearch
```
- 集計したいログのindexを変更するには、log_summary.pyの以下の部分を変更してください。（log_summary.pyの14行目）
  - 'beats-*'の部分を変更することで集計するindexを変更することができます。
```
ES_INDEX_PATTERN = 'beats-*'
```
- 集計の期間を変更するには、log_summary.pyの以下の部分を変更してください。（log_summary.pyの22行目）
  - ()の中をweek=1にすることで1週間、minutes=30にすることで30分間というように変更できます。
```
start_time = end_time - timedelta(hours=1)
```
- 集計の頻度を変更するには、log_summary.pyの以下の部分を変更してください。（log_summary.pyの88、89行目）
  - minute = timestamp.minuteの状態だと1分ごと集計されます。
  - minute = (timestamp.minute // 15) * 15にすることで15分ごとに集計されます。
  - hour = (timestamp.hour // 2) * 2にすることで2時間ごとに集計されます。
```
minute = (timestamp.minute // 5) * 5
rounded_time = timestamp.replace(minute=minute, second=0, microsecond=0)
```
