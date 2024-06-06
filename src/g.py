# encoding = utf-8


import requests
import pandas as pd
import json
import config #路径配置
import os 
import joblib
from joblib import Parallel, delayed
import time
from datetime import datetime, timedelta


class FundFlowGetter:
  
    def __init__(self) -> None:
        """
        初始化
        读取hs300的股票代码
        """
        def get_secid(code : str):
            if code.startswith('6'):
                return '1.' + code
            else:
                return '0.' + code
    
        hs300_code_csv_path = os.path.join(config.csv_db_path,'hs300.csv')
        hs300_code_df = pd.read_csv(hs300_code_csv_path)
        hs300_code_df['code'] = hs300_code_df['code'].astype(str)
        hs300_code_df['code'] = hs300_code_df['code'].str.zfill(6)
        self._hs300_code_list = hs300_code_df['code'].apply(get_secid)
  

    def multi_get(self):
        """
        并行获取资金流数据
        无占比信息，数据范围从开盘到当前时间
        """
        results = Parallel(n_jobs=8)(delayed(self.get_one)(code) for code in self._hs300_code_list)
        results_df = pd.concat(results)
        results_df.set_index(['code','时间'],inplace = True)
        return results_df



    def get_one(self,code):
        """
        无占比信息，数据范围从开盘到当前时间
        """
        url = f"https://push2.eastmoney.com/api/qt/stock/fflow/kline/get?cb=jQuery11230035804164294341856_1717574164503&lmt=0&klt=1&fields1=f1%2Cf2%2Cf3%2Cf7&fields2=f51%2Cf52%2Cf53%2Cf54%2Cf55%2Cf56%2Cf57%2Cf58%2Cf59%2Cf60%2Cf61%2Cf62%2Cf63%2Cf64%2Cf65&ut=b2884a393a59ad64002292a3e90d46a5&secid={code}&_=1717574164504"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        response = requests.get(url, headers=headers)

        res_text = response.text
        json_str = res_text[res_text.find("(") + 1:res_text.rfind(")")]
        json_data = json.loads(json_str)
        data = json_data['data']['klines']
        df = pd.DataFrame([item.split(',') for item in data],columns=['时间','主力流入','小单流入','中单流入','大单流入','超大单流入'])
        df['code'] = code.split('.')[1]
        return df
    
    def get_single(self,code):
        """
        获取当前时间的资金流向数据，包含占比信息
        仅包含当前时间
        """
        map_dict = {
           
            'f62': '主力流入净额',
            'f66': '今日超大单流入净额',
            'f72': '今日大单流入净额',
            'f78': '今日中单流入净额',
            'f84': '今日小单流入净额',
            'f184': '今日主力流入净占比',
            'f69': '今日超大单流入净占比',
            'f75': '今日大单流入净占比',
            'f81': '今日中单流入净占比',
            'f87': '今日小单流入净占比'
        }

        url = f"https://push2.eastmoney.com/api/qt/ulist.np/get?cb=jQuery112303110525374636799_1717587517921&fltt=2&secids={code}&fields=f62%2Cf184%2Cf66%2Cf69%2Cf72%2Cf75%2Cf78%2Cf81%2Cf84%2Cf87%2Cf64%2Cf65%2Cf70%2Cf71%2Cf76%2Cf77%2Cf82%2Cf83%2Cf164%2Cf166%2Cf168%2Cf170%2Cf172%2Cf252%2Cf253%2Cf254%2Cf255%2Cf256%2Cf124%2Cf6%2Cf278%2Cf279%2Cf280%2Cf281%2Cf282&ut=b2884a393a59ad64002292a3e90d46a5&_=1717587517922"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        response = requests.get(url, headers=headers)

        res_text = response.text
        json_str = res_text[res_text.find("(") + 1:res_text.rfind(")")]
        
        json_data = json.loads(json_str)
        
        a = json_data['data']['diff'][0]
        data_listed = {key: [value] for key, value in a.items()}
        df = pd.DataFrame.from_dict(data_listed)

        df = df[list(map_dict.keys())]
        df.columns = df.columns.map(map_dict)
        df['code'] = code.split('.')[1]
        df['time'] = self._now
        return df

    def get_all(self):
        """
        获取当前时间的资金流向数据，包含占比信息
        仅包含当前时间
        并行获取
        """
        self._now = self.get_one('0.000001')['时间'].iloc[-1]
        results = Parallel(n_jobs=8)(delayed(self.get_single)(code) for code in self._hs300_code_list)
        results_df = pd.concat(results)
        results_df.set_index(['code','time'],inplace = True)
        return results_df


if __name__ == '__main__':
    F = FundFlowGetter()
    print(F.get_all())
