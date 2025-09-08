import csv
from collections import defaultdict
from datetime import datetime
from statistics import median
import os

# 入力ファイル名
INPUT_FILE = 'log_test.csv'

def calculate_and_compare_logs():
    """
    ログデータの中央値を計算し、各コンテナの最新のログ件数と比較して、結果を「異常あり」と「異常なし」に分けて出力します。
    """
    if not os.path.exists(INPUT_FILE):
        print(f"エラー: ファイル '{INPUT_FILE}' が見つかりませんでした。")
        return

    # データをコンテナ名でグループ化し、各コンテナのタイムスタンプとログ件数を保存
    container_data = defaultdict(list)
    
    try:
        with open(INPUT_FILE, mode='r', newline='', encoding='utf-8') as f:
            reader = csv.reader(f)
            header = next(reader)  # ヘッダーをスキップ

            for row in reader:
                if len(row) < 4:
                    continue  # 不正な行をスキップ
                
                time_str, container_name, log_count_str, _ = row
                log_count = int(log_count_str)
                
                time_obj = datetime.strptime(time_str, '%Y-%m-%d %H:%M')
                
                # コンテナ名ごとにタイムスタンプとログ件数のタプルを保存
                container_data[container_name].append((time_obj, log_count))

    except Exception as e:
        print(f"エラー: ファイルの読み込み中に問題が発生しました - {e}")
        return

    abnormal_containers = []
    normal_containers = []

    # 各コンテナの中央値を計算し、最新のログ件数と比較
    for container_name, timeseries_list in container_data.items():
        # タイムスタンプでソートして、最新のデータポイントを特定
        timeseries_list.sort(key=lambda x: x[0], reverse=True)
        
        # 過去のデータが1つもない場合は比較できない
        if len(timeseries_list) < 2:
            continue
            
        # 最新のログ件数を取得
        latest_timestamp, latest_count = timeseries_list[0]
        
        # 最新のデータポイントを除いた過去のログ件数を取得
        past_counts = [count for _, count in timeseries_list[1:]]
        
        # 中央値を計算
        median_count = median(past_counts)

        # 最新のログ件数が中央値より大きく、かつその差が1000以上の場合、異常ありと判断
        if latest_count > median_count and (latest_count - median_count) >= 1000:
            abnormal_containers.append((container_name, latest_count, median_count))
        # 最新のログ件数が中央値以下、または差が1000未満の場合、異常なしと判断
        else:
            normal_containers.append((container_name, latest_count, median_count))
            
    # 分析結果を出力
    if abnormal_containers:
        print("🔴 異常あり (ログ件数が中央値より多く、その差が1000以上のコンテナ):")
        for container_name, latest_count, median_count in abnormal_containers:
            print(f"  - {container_name} (最新: {latest_count}, 中央値: {median_count})")
    else:
        print("✅ ログ件数が中央値より多く、かつその差が1000以上のコンテナはありませんでした。")

    print("\n------------------------------------------")

    if normal_containers:
        print("🟢 異常なし (上記の条件に該当しないコンテナ):")
        for container_name, latest_count, median_count in normal_containers:
            print(f"  - {container_name} (最新: {latest_count}, 中央値: {median_count})")
    else:
        print("過去のデータと比較可能なコンテナはありませんでした。")

if __name__ == "__main__":
    calculate_and_compare_logs()