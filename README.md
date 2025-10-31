# 提案ソフトウェア
# container-logs-to-metrics

## 構成
```
/home/c0a22166/teian
├── log_test.py
├── log_syukei.cxv
```

## 実行する環境
- Python verion：3.12.3

## プログラムの概要

### `log_syukei.py`
**説明**:このファイルをPythonで動かすことで、Elasticsearchのbeats indexにあるログをコンテナ名別に30分ごとに時間帯を分けて出力されます。出力されるログはプログラム実行時の6時間前から現在時刻までのログであり，log_test.csvにコンテナ名別に1分ごとのログ件数が集計され出力されます。
- 入力は，Elasticsearchのbeats indexのログとログ検索を終了したい時間帯です。出力はプログラム実行時の6時間前から現在時刻までのログです。
- 出力では、ログの時間帯、コンテナ名、ログ件数、1分前のログ件数からの変化を出力します。
  
**使い方**:

<img width="851" height="185" alt="image" src="https://github.com/user-attachments/assets/3e8a29e6-10a6-4e9e-bcbe-a47dd250ff16" />




**実行結果**
log_test.csvにコンテナ名別に1分ごとのログ件数が集計される

<img width="628" height="406" alt="image" src="https://github.com/user-attachments/assets/d90d6034-998a-4428-8a8b-5f020f70fe36" />


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
- 集計したいログのindexを変更するには、log_syukei.pyの以下の部分を変更してください。（log_syukei.pyの19行目）
  - 'beats-*'の部分を変更することで集計するindexを変更することができます。
```
ES_INDEX_PATTERN = 'beats-*'
```
- 集計の期間を変更するには、log_syukei.pyの以下の部分を変更してください。（log_syukei.pyの100行目）
  - ()の中をweek=1にすることで1週間、minutes=30にすることで30分間というように変更できます。
```
start_time = end_time - timedelta(hours=24)
```
- 集計の頻度を変更するには、log_syukei.pyの以下の部分を変更してください。（log_syukei.pyの120、121行目）
  - minute = timestamp.minuteの状態だと1分ごと集計されます。
  - minute = (timestamp.minute // 15) * 15にすることで15分ごとに集計されます。
  - hour = (timestamp.hour // 2) * 2にすることで2時間ごとに集計されます。
```
minute = (timestamp.minute // 5) * 5
rounded_time = timestamp.replace(minute=minute, second=0, microsecond=0)
```
