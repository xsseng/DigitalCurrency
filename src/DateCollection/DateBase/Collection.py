#!/usr/bin/python3
# -*- coding:utf-8 -*-

import sys
import os
import re
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir, os.pardir)))
import pandas as pd
import ccxt
from datetime import datetime, timedelta
from DateCollection.DateBase.DBUtil import Config
from DateCollection.DateBase.DBUtil import DBUtil
from apscheduler.schedulers.blocking import BlockingScheduler

pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 1000)


class HuoBiKey(object):
    def __init__(self, api_key, secret):
        self.api_key = api_key
        self.secret = secret


class Collector(HuoBiKey):
    btc_table_name = 'btc_kline_' + str(datetime.now().year)
    eos_table_name = 'eos_kline_' + str(datetime.now().year)

    def __init__(self, conf_name=None):
        self.conf = Config().get_content(conf_name)
        super(Collector, self).__init__(**self.conf)

    def get_ohlcv(self, symbol):
        exchange = ccxt.huobipro()
        exchange.apiKey = self.api_key
        exchange.secret = self.secret

        time_interval = '1m'
        # 抓取数据
        content = exchange.fetch_ohlcv(symbol, timeframe=time_interval, limit=1)
        # 整理数据
        df = pd.DataFrame(content, dtype=float)
        df.rename(columns={0: 'MTS', 1: 'open', 2: 'high', 3: 'low', 4: 'close', 5: 'volume'}, inplace=True)
        df['candle_begin_time'] = pd.to_datetime(df['MTS'], unit='ms')
        df['candle_begin_time_GMT8'] = df['candle_begin_time'] + timedelta(hours=8)
        df = df[['candle_begin_time_GMT8', 'open', 'high', 'low', 'close', 'volume']]
        return df

    def insert_ohlcv(self):
        mysql = DBUtil("dbMysql")
        symbol = 'BTC/USDT'
        df = self.get_ohlcv(symbol)
        sql = 'insert into ' + self.btc_table_name + '(candle_begin_time,open,high,low,close) value("' + str(df['candle_begin_time_GMT8'][0]) + '","' + str(df['open'][0]) + '","' + str(df['high'][0]) + '","' + str(df['low'][0]) + '","' + str(df['close'][0]) + '")'
        print(sql)
        mysql.insert(sql)

        symbol = 'EOS/USDT'
        df = self.get_ohlcv(symbol)
        sql = 'insert into ' + self.eos_table_name + '(candle_begin_time,open,high,low,close) value("' + str(df['candle_begin_time_GMT8'][0]) + '","' + str(df['open'][0]) + '","' + str(df['high'][0]) + '","' + str(df['low'][0]) + '","' + str(df['close'][0]) + '")'
        print(sql)
        mysql.insert(sql)
        mysql.dispose()

    def create_btc_table(self):
        mysql = DBUtil("dbMysql")
        sql = 'CREATE TABLE ' + self.btc_table_name + '(id INT(11) NOT NULL AUTO_INCREMENT,candle_begin_time VARCHAR(100) NOT NULL,open FLOAT(11) NOT NULL,high FLOAT(11) NOT NULL,low FLOAT(11) NOT NULL,close FLOAT(11) NOT NULL,PRIMARY KEY (`id`),KEY `idx_time` (`candle_begin_time`) ) ENGINE=INNODB DEFAULT CHARSET=utf8;'
        print(sql)
        mysql.create(sql)
        mysql.dispose()

    def create_eos_table(self):
        mysql = DBUtil("dbMysql")
        sql = 'CREATE TABLE ' + self.eos_table_name + '(id INT(11) NOT NULL AUTO_INCREMENT,candle_begin_time VARCHAR(100) NOT NULL,open FLOAT(11) NOT NULL,high FLOAT(11) NOT NULL,low FLOAT(11) NOT NULL,close FLOAT(11) NOT NULL,PRIMARY KEY (`id`),KEY `idx_time` (`candle_begin_time`) ) ENGINE=INNODB DEFAULT CHARSET=utf8;'
        print(sql)
        mysql.create(sql)
        mysql.dispose()

    def table_exists(self):
        # 刷新表名
        mysql = DBUtil("dbMysql")
        self.btc_table_name = 'btc_kline_' + str(datetime.now().year)
        self.eos_table_name = 'eos_kline_' + str(datetime.now().year)
        sql = 'show tables'
        tables = mysql.get_all(sql)
        mysql.dispose()
        tables_list = re.findall('(\'.*?\')', str(tables))
        print(tables_list)
        tables_list = [re.sub("'", '', each) for each in tables_list]
        print(tables_list)
        if self.btc_table_name not in tables_list:
            self.create_btc_table()
        if self.eos_table_name not in tables_list:
            self.create_eos_table()

if __name__ == '__main__':
    collector = Collector("huobi")
    collector.table_exists()
    collector.insert_ohlcv()

    scheduler = BlockingScheduler()
    scheduler.add_job(collector.insert_ohlcv, 'interval', minutes=1)
    scheduler.add_job(collector.table_exists, 'cron', month='1', day='1', hour='0', minute='0', second='0')
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        pass
