import csv
from datetime import datetime, timedelta
from collections import defaultdict
import os
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
import pytz
import sys

# Elasticsearch接続設定
# 🚨 IPアドレスとポートはあなたの環境に合わせてください
ES_HOST = '192.168.100.192'
ES_PORT = 30092
ES_USER = None
ES_PASSWORD = None

# 検索対象のElasticsearchインデックスパターン
# 🚨 あなたの環境に合わせて 'syslog-*' に変更
ES_INDEX_PATTERN = 'beats-*'

# 出力ファイル名
output_file = 'log_test.csv'

# 日本標準時 (JST) タイムゾーンを定義
JST = pytz.timezone('Asia/Tokyo')

def get_logs_from_elasticsearch(start_time, end_time):
    """
    指定された期間のログをElasticsearchから取得します。
    """
    # 🚨 Elasticsearchクライアントをバージョン8系に合わせた設定
    es = Elasticsearch(
        hosts=[{'host': ES_HOST, 'port': ES_PORT, 'scheme': 'http'}],
        http_auth=(ES_USER, ES_PASSWORD) if ES_USER and ES_PASSWORD else None,
        request_timeout=60,
        verify_certs=False,
    )

    # JSTで指定された時間をUTCに変換してElasticsearchに渡します
    start_time_utc = start_time.astimezone(pytz.utc)
    end_time_utc = end_time.astimezone(pytz.utc)

    query = {
        "query": {
            "range": {
                "@timestamp": {
                    "gte": start_time_utc.isoformat(timespec='milliseconds').replace('+00:00', 'Z'),
                    "lte": end_time_utc.isoformat(timespec='milliseconds').replace('+00:00', 'Z')
                }
            }
        },
        "sort": [
            {"@timestamp": {"order": "asc"}}
        ]
    }

    print(f"✅ Elasticsearch ({ES_HOST}:{ES_PORT}, Index: {ES_INDEX_PATTERN}) からログを取得中...")

    logs = []
    try:
        # scanヘルパーを使用して大量のログを効率的に取得
        for hit in scan(es,
                         query=query,
                         index=ES_INDEX_PATTERN,
                         scroll='2m',
                         timeout='60s'
                         ):
            source = hit['_source']
            
            # 'kubernetes.container.name' フィールドを取得
            container_name = source.get('kubernetes', {}).get('container', {}).get('name')
            timestamp = source.get('@timestamp')
            
            # コンテナ名とタイムスタンプが存在する場合のみ処理
            if container_name and timestamp:
                logs.append({
                    'container_name': container_name,
                    '@timestamp': timestamp
                })
    except Exception as e:
        print(f"❌ Elasticsearchからのログ取得中にエラーが発生しました: {e}", file=sys.stderr)
        return []

    print(f"✅ {len(logs)} 件のログを取得しました。")
    return logs

if __name__ == "__main__":
    counter = defaultdict(int)

    # ユーザーに入力を求める
    end_input_str = input("終了時刻を入力してください（例: 20250904-1400）: ")

    try:
        # 'YYYYMMDD-HH:MM' 形式で入力された時刻をパースしてナイーブなdatetimeオブジェクトを作成
        end_time_naive = datetime.strptime(end_input_str, '%Y%m%d-%H%M')
        # ナイーブなdatetimeオブジェクトにJSTタイムゾーンを明示的に付与
        end_time_jst = JST.localize(end_time_naive)
        
        # 終了時刻から24時間前の時間を開始時刻として計算 (元のコードのまま)
        start_time_jst = end_time_jst - timedelta(hours=24)
        print(f"✅ ログ検索期間: {start_time_jst.strftime('%Y-%m-%d %H:%M:%S')} から {end_time_jst.strftime('%Y-%m-%d %H:%M:%S')}")
    except ValueError:
        print("❌ 入力された時刻の形式が正しくありません。'YYYYMMDD-HHMM'の形式で入力してください。", file=sys.stderr)
        sys.exit(1)

    log_entries = get_logs_from_elasticsearch(start_time_jst, end_time_jst)

    for row in log_entries:
        timestamp_str = row.get('@timestamp')
        container_name = row.get('container_name')

        if not timestamp_str or not container_name:
            continue

        try:
            timestamp_utc = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00')).astimezone(pytz.utc)
            timestamp_jst = timestamp_utc.astimezone(JST)

            # 1分単位に切り捨て (元のコードのロジックを維持)
            minute = (timestamp_jst.minute // 1) * 1
            rounded_time_jst = timestamp_jst.replace(minute=minute, second=0, microsecond=0)

            key = f"{rounded_time_jst.strftime('%Y-%m-%d %H:%M')} {container_name}"
            counter[key] += 1

        except ValueError as e:
            print(f"エラー: タイムスタンプのパースに失敗しました - {e} → {timestamp_str}", file=sys.stderr)
            continue

    last_interval_counts = {}

    with open(output_file, mode='w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['時間帯', 'コンテナ名', 'ログ件数', '前時間帯からの変化'])

        for key, current_count in sorted(counter.items()):
            time_str, container_name = key.rsplit(' ', 1)

            change_str = "-"
            if container_name in last_interval_counts:
                previous_count = last_interval_counts[container_name]
                change = current_count - previous_count
                change_str = f"{change:+d}"

            last_interval_counts[container_name] = current_count

            writer.writerow([time_str, container_name, current_count, change_str])

    print(f"✅ 集計が完了しました → {output_file}")
