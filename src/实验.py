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

# 设置日志配置
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(asctime)s - %(message)s')

class FundFlowGetter:
    
    def __init__(self, csv_db_path: Path) -> None:
        def get_secid(code: str):
            return f"{'1' if code.startswith('6') else '0'}.{code}"
        
        # 加载HS300股票代码并格式化
        hs300_code_csv_path = csv_db_path / 'hs300.csv'
        hs300_code_df = pd.read_csv(hs300_code_csv_path)
        hs300_code_df['code'] = hs300_code_df['code'].astype(str).str.zfill(6)
        self._hs300_code_list = hs300_code_df['code'].apply(get_secid).tolist()

    def get_single(self, code, fetch_time, specific_time):
        # 字段映射字典
        map_dict = {
            'f62': '主力',
            'f184': '占比',
        }

        # API URL，包含所需字段
        url = (f"https://push2.eastmoney.com/api/qt/ulist.np/get"
               f"?cb=jQuery112303110525374636799_1717587517921&fltt=2"
               f"&secids={code}&fields=f62,f184&ut=b2884a393a59ad64002292a3e90d46a5&_=1717587517922")
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }

        try:
            # 发送HTTP请求获取数据
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
            # 处理异常并记录错误
            logging.error(f"Error fetching data for {code}: {e}")
            return pd.DataFrame()

    def get_all(self, specific_time):
        fetch_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        specific_time_str = specific_time.strftime('%Y-%m-%d %H:%M:%S')
        # 并行获取所有股票的数据
        results = Parallel(n_jobs=8)(delayed(self.get_single)(code, fetch_time, specific_time_str) for code in self._hs300_code_list)
        results_df = pd.concat(results, ignore_index=True)
        results_df = results_df[['code', 'fetch_time', 'data_time'] + [col for col in results_df.columns if col not in ['code', 'fetch_time', 'data_time']]]
        return results_df

    def save_summary(self, result_df, summary_file_path):
        # 计算主力总和
        主力_sum = result_df['主力'].astype(float).sum()
        fetch_time = result_df['fetch_time'].iloc[0]
        data_time = result_df['data_time'].iloc[0]
        new_data = pd.DataFrame({'抓取时间': [fetch_time], '数据时间': [data_time], '主力总和': [主力_sum]})

        summary_file_path = Path(summary_file_path)
        if summary_file_path.exists():
            summary_df = pd.read_csv(summary_file_path)
            summary_df = pd.concat([summary_df, new_data], ignore_index=True)
            summary_df.to_csv(summary_file_path, index=False)
            logging.info(f"Summary data updated and saved to {summary_file_path}")
        else:
            new_data.to_csv(summary_file_path, index=False)
            logging.info(f"Summary data saved to {summary_file_path}")

    def run(self, csv_db_path, summary_file_path, realtime_file_path, specific_time):
        result_df = self.get_all(specific_time)
        
        # 保存summary之前保留原始的result_df
        self.save_summary(result_df, summary_file_path)

        # 删除时间相关的列
        result_df.drop(columns=['fetch_time', 'data_time'], inplace=True)
        
        # 将新的主力和占比列名添加时间后缀
        timestamp = specific_time.strftime('%H%M')
        result_df.set_index('code', inplace=True)
        new_columns = {col: f"{col}_{timestamp}" for col in result_df.columns if col in ['主力', '占比']}
        result_df.rename(columns=new_columns, inplace=True)

        # 检查是否存在原始数据文件，如果存在则追加列，否则创建新文件
        if realtime_file_path.exists():
            existing_df = pd.read_csv(realtime_file_path, index_col='code')
            combined_df = existing_df.join(result_df, how='outer')
            combined_df.reset_index(inplace=True)
            combined_df.to_csv(realtime_file_path, index=False)
        else:
            result_df.reset_index(inplace=True)
            result_df.to_csv(realtime_file_path, index=False)
        
        logging.info(f"Real-time data saved to {realtime_file_path}")

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
    csv_db_path = Path("data/csvdb")
    summary_file_path = csv_db_path / "主力总和.csv"
    realtime_file_path = csv_db_path / "data.csv"
    specific_time = get_specific_time()
    
    F = FundFlowGetter(csv_db_path)
    F.run(csv_db_path, summary_file_path, realtime_file_path, specific_time)

# 调度器设置
schedule.every(5).minutes.at(":00").do(job_5min)

# 启动调度器并手动触发一次5分钟任务
if __name__ == '__main__':
    job_5min()
    
    while True:
        schedule.run_pending()
        time.sleep(1)
