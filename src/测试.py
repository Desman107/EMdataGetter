# encoding = utf-8

import requests
import pandas as pd
import json
from pathlib import Path
import joblib
from joblib import Parallel, delayed
from datetime import datetime, timedelta
import schedule
import time
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class FundFlowGetter:
    
    def __init__(self, csv_db_path: Path) -> None:
        def get_secid(code: str):
            return f"{'1' if code.startswith('6') else '0'}.{code}"
        
        hs300_code_csv_path = csv_db_path / 'hs300.csv'
        hs300_code_df = pd.read_csv(hs300_code_csv_path)
        hs300_code_df['code'] = hs300_code_df['code'].astype(str).str.zfill(6)
        self._hs300_code_list = hs300_code_df['code'].apply(get_secid).tolist()

    def get_single(self, code, fetch_time, specific_time):
        map_dict = {
            'f62': '主力流入净额',
            'f184': '今日主力流入净占比',
        }

        url = (f"https://push2.eastmoney.com/api/qt/ulist.np/get"
               f"?cb=jQuery112303110525374636799_1717587517921&fltt=2"
               f"&secids={code}&fields=f62,f184&ut=b2884a393a59ad64002292a3e90d46a5&_=1717587517922")
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }

        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            res_text = response.text
            json_str = res_text[res_text.find("(") + 1:res_text.rfind(")")]
            json_data = json.loads(json_str)
            a = json_data['data']['diff'][0]
            df = pd.DataFrame([a])[list(map_dict.keys())]
            df.columns = df.columns.map(map_dict)
            df['code'] = code.split('.')[1]
            df['fetch_time'] = fetch_time
            df['data_time'] = specific_time
            return df
        except (requests.RequestException, json.JSONDecodeError, KeyError) as e:
            logging.error(f"Error fetching data for {code}: {e}")
            return pd.DataFrame()

    def get_all(self, specific_time):
        fetch_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        specific_time_str = specific_time.strftime('%Y-%m-%d %H:%M:%S')
        results = Parallel(n_jobs=8)(delayed(self.get_single)(code, fetch_time, specific_time_str) for code in self._hs300_code_list)
        results_df = pd.concat(results, ignore_index=True)
        results_df = results_df[['fetch_time', 'data_time', 'code'] + [col for col in results_df.columns if col not in ['fetch_time', 'data_time', 'code']]]
        return results_df

    def save_summary(self, result_df, summary_file_path):
        f62_sum = result_df['主力流入净额'].astype(float).sum()
        fetch_time = result_df['fetch_time'].iloc[0]
        data_time = result_df['data_time'].iloc[0]
        new_data = pd.DataFrame({'抓取时间': [fetch_time], '数据时间': [data_time], '主力流入净额总和': [f62_sum]})

        summary_file_path = Path(summary_file_path)
        if summary_file_path.exists():
            summary_df = pd.read_csv(summary_file_path)
            summary_df = pd.concat([summary_df, new_data], ignore_index=True)
            summary_df.to_csv(summary_file_path, index=False)
            logging.info(f"Summary data updated and saved to {summary_file_path}")
        else:
            new_data.to_csv(summary_file_path, index=False)
            logging.info(f"Summary data saved to {summary_file_path}")

    def merge_and_summarize(self, df1, df2):
        combined_df = pd.concat([df1, df2], ignore_index=True)
        combined_summary = combined_df.groupby(['data_time']).agg({
            '主力流入净额': 'sum',
            'code': 'count'
        }).reset_index()
        combined_summary.rename(columns={'code': 'count'}, inplace=True)
        return combined_summary

    def run(self, csv_db_path, summary_file_path, realtime_file_path, specific_time, filename):
        result_df = self.get_all(specific_time)
        result_df.to_csv(csv_db_path / filename, index=False)
        logging.info(f"Real-time data saved to {csv_db_path / filename}")

        self.save_summary(result_df, summary_file_path)

def get_specific_time():
    now = datetime.now()
    # 获取当前时间的最近的5分钟整点
    minute = (now.minute // 5) * 5
    specific_time = now.replace(minute=minute, second=0, microsecond=0)
    return specific_time

def get_next_run_time():
    now = datetime.now()
    # 获取下一个5分钟整点时间
    minute = (now.minute // 5 + 1) * 5
    next_run = now.replace(minute=minute, second=0, microsecond=0)
    if next_run <= now:
        next_run += timedelta(minutes=5)
    return next_run

def job_5min():
    logging.info("Job 5min started")
    csv_db_path = Path("D:/FundGet/data/csvdb")
    summary_file_path = csv_db_path / "主力资金总和.csv"
    realtime_file_path = csv_db_path / "原始数据.csv"
    specific_time = get_specific_time()
    filename = specific_time.strftime('%H%M') + ".csv"
    
    F = FundFlowGetter(csv_db_path)
    F.run(csv_db_path, summary_file_path, realtime_file_path, specific_time, filename)

def job_1min():
    logging.info("Job 1min started")
    csv_db_path = Path("D:/FundGet/data/csvdb")
    summary_file_path = csv_db_path / "主力资金总和.csv"
    realtime_file_path = csv_db_path / "原始数据.csv"
    specific_time = get_specific_time()
    F = FundFlowGetter(csv_db_path)
    result_df = F.get_all(specific_time)
    result_df.to_csv(realtime_file_path, index=False)
    logging.info(f"Real-time data saved to {realtime_file_path}")

    # 读取5分钟数据进行合并
    try:
        previous_5min_df = pd.read_csv(summary_file_path)
        combined_summary = F.merge_and_summarize(result_df, previous_5min_df)
        logging.info("Combined summary (1min + previous 5min):")
        logging.info(combined_summary)
    except FileNotFoundError:
        logging.warning(f"Summary file not found at {summary_file_path}, skipping merge.")

# 调度器设置
schedule.every(5).minutes.at(":00").do(job_5min)
schedule.every(1).minutes.do(job_1min)

# 启动调度器并手动触发一次5分钟任务
if __name__ == '__main__':
    job_5min()
    
    while True:
        schedule.run_pending()
        time.sleep(1)
