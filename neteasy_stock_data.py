# coding:utf-8
'''
@Function:取得上证50成分股(见SH50文件)的N年历史数据(价格数据和交易量数据或者其他备选数据)
定位爬取网址为(以浦发银行2017年4季度数据为例):http://quotes.money.163.com/trade/lsjysj_600000.html?year=2017&season=4
数据来自网易财经，但是数据未经过复权处理

@author:Minux
@date:2017-12-11
'''
import requests
from bs4 import BeautifulSoup
import os
import time
import csv

"""
global variables
browser header
"""
_headers_0={
	"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:56.0) Gecko/20100101 Firefox/56.0"
}

_headers_1={
	"User-Agent":"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.104 Safari/537.36 Core/1.53.1708.400 QQBrowser/9.5.9635.400"
}


'''
Spider class
'''
class StockSpider(object):

    def __init__(self, code, begin_year, end_year):
        self.code = code
        self.begin_year = begin_year
        self.end_year = end_year + 1
        self.url = 'http://quotes.money.163.com/trade/lsjysj_'

    def fetch_stock_data(self, year, season):
        """
        取得year年season季度的股票数据，并将其按日期升序返回
        :param year:指定爬取数据的年份
        :param season:指定爬取数据的季度
        :return:按日期升序返回数据
        """
        stock_url = self.url + str(self.code) + '.html?year='+str(year)+'&season='+str(season)
        # print(stock_url)
        wy_html = requests.get(stock_url, headers = _headers_0).content
        soup = BeautifulSoup(wy_html, 'html.parser', from_encoding='utf-8')
        # print(soup.html())
        # 取得该页表的内容
        page_table = soup('table',{'class':'table_bg001 border_box limit_sale'})[0]
        season_data = page_table('tr')
        # 将数据逆向返回
        return season_data[::-1]

    def stock_data_file(self):
        """
        提取的指标为日期,开盘价,最高价,最低价,收盘价,涨跌额,涨跌幅,成交量(手),成交金额(万元),振幅(%),换手率(%)
        :return:True表示写入成功,False表示写入过程中出现异常
        """
        stock_csv = open('./SH50_DATA/'+str(self.code)+'.csv', "wb")
        writer = csv.writer(stock_csv)
        try:
            writer.writerow(['date','open','high','low','close','delta','delta_rate','vol','vol_sum(w)','vibration(%)','turnover(%)'])
            for year in range(self.begin_year, self.end_year):
                for season in range(1, 5):
                    print('code ' + str(self.code) +' year '+ str(year) +' season '+ str(season) + ' data is fetching....')
                    page_data = self.fetch_stock_data(year, season)
                    if page_data is not None:
                        for row_data in page_data:
                            csv_row = []
                            if row_data('td') is not None:
                                for value in row_data('td'):
                                    csv_row.append(value.get_text().replace(',',''))
                                if csv_row != []:
                                    writer.writerow(csv_row)
                time.sleep(0.1)
        except Exception as e:
            print(e)
            return False
        finally:
            stock_csv.close()
            return True

    @staticmethod
    def get_sh50_code():
        """
        根据SH50.name文件读取上证50成分股的信息,返回股票代码列表
        :return:code_list
        """
        code_list = []
        with open(r'./SH50.name', "rb") as f:
            while True:
                stock = f.readline()
                if not stock:
                    break
                else:
                    code = stock.split('.')[0]
                    code_list.append(code)
        return code_list

def get_all_sh50_hist_data():
    start_year = 2010
    end_year   = 2017
    code_list = StockSpider.get_sh50_code()
    for code in code_list:
        stock_spider = StockSpider(code, start_year, end_year)
        stock_spider.stock_data_file()

def test_main():
    pass
    # stock_spider = StockSpider(600000, 2016, 2017)
    # season_data = stock_spider.fetch_stock_data(2017,3)
    # print(season_data)
    # stock_spider.stock_data_file()
    # StockSpider.get_sh50_code()

if __name__ == "__main__":
    # test_main()
    get_all_sh50_hist_data()