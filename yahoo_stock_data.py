# coding:utf-8
'''
https://finance.yahoo.com/quote/600000.SS/history?period1=1262275200&period2=1512921600&interval=1d&filter=history&frequency=1d
'''
# coding:utf-8
'''
@Function:取得上证50成分股(见SH50文件)的N年历史数据(价格数据和交易量数据或者其他备选数据)
定位爬取网址为(以浦发银行2017年4季度数据为例):http://quotes.money.163.com/trade/lsjysj_600000.html?year=2017&season=4
数据来自yahoo财经，主要提取adjust数据
finance.yahoo为动态加载网页

@author:Minux
@date:2017-12-12
'''
from selenium import webdriver
from selenium.webdriver.common import keys
from selenium.webdriver.support.ui import WebDriverWait
from bs4 import BeautifulSoup
import os
import sys
import time
import csv
import codecs


reload(sys)
sys.setdefaultencoding('utf-8')

'''
Spider class
'''
class StockSpider(object):

    def __init__(self, code, begin_year, end_year):
        """
        finance.yahoo的日期参数为时间戳
        :param code:
        :param begin_year:
        :param end_year:
        """
        self.code = code
        self.begin_time = str(int(time.mktime(time.strptime(str(begin_year)+"-01-01 00:00:00", '%Y-%m-%d %H:%M:%S'))))
        self.end_time = str(int(time.mktime(time.strptime(str(end_year)+"-12-31 23:59:59", '%Y-%m-%d %H:%M:%S'))))
        self.url = 'https://finance.yahoo.com/quote/'

    def fetch_yahoo_page_html(self, times):
        """
        在动态加载的网页下爬取数据
        :param times:模拟scroll bar 下滑的次数
        :return:网页源码是否保存成功 0/-1
        """
        stock_url = self.url + str(self.code) +'.SS/' + 'history?period1=' + self.begin_time + '&period2='+self.end_time+'&interval=1d&filter=history&frequency=1d'
        # stock_url = 'https://www.sina.com.cn/'
        print(stock_url)
        if os.path.exists('./Yahoo_SH50_HTML'):
            print('存储文件夹已经建立')
            str_file = './Yahoo_SH50_HTML/yahoo_'+str(self.code)+'.html'
            if os.path.exists(str_file):
                print("source code already get")
                if (os.path.getsize(str_file)/float(1024)) > 10:
                    print("source code is valid...return True")
                    return True
        else:
            os.mkdir('./Yahoo_SH50_HTML')
        # driver的位置可以手动指定
        # driver = webdriver.Firefox(executable_path=r'D:\MLY\firefox\geckodriver')
        # driver = webdriver.Chrome(executable_path=r'C:\Program Files (x86)\Google\Chrome\Application\chromedriver')
        # 使用无界面浏览器PhantomJS
        driver = webdriver.PhantomJS()
        '''
        根据测试,这里需要一段回滚逻辑,如果geturl时间过短，可能访问的页面是
        空页面，即大小为1KB
        想到了两种解决方案：
        1.监测访问打开页面需要的时间，如果时间过短则进入重新执行函数
        2.监测Yahoo_SH50_HTML文件夹下的文件大小，对于不符合要求的文件(1KB),
        的文件名进行记录,建立list然后进行重新爬取
        '''
        # load_begin = time.time()
        # load_end = time.time()
        # if load_end - load_begin < 20:
        #     file = open('./prob_code','wb')
        #     file.write(str(self.code) +' '+str(load_end - load_begin))
        #     file.close()
        driver.get(stock_url)

        # 如果程序等待时间过长考虑使用条件终止
        # WebDriverWait(driver, 10).until(lambda driver: driver.find_element_by_id("YDC-Col1"))
        time.sleep(1)
        print("load yahoo page finish...")

        for i in range(times):
            # driver.execute_script("window.scrollTo(0, document.scrollHeight);")
            # driver.execute_script("document.body.scrollTop = document.body.scrollHeight;")
            driver.execute_script("window.scrollBy(0, 4000);")
            print("slide down scroll bar!")
            # 暂停0.5S完成页面加载
            time.sleep(0.5)
        yahoo_html = driver.page_source
        with open('./Yahoo_SH50_HTML/yahoo_' + str(self.code) + '.html','wb') as f:
            f.write(yahoo_html)

        print(str(self.code)+".SS page html saved successfully!")
        return 0

    @staticmethod
    def get_data_from_html(stock_code):
        """
        从HTML文件中读取SH50数据,并保存到code.csv文件中
        :param stock_code:传入的股票代码
        :return:0/-1
        """
        yahoo_dir = './Yahoo_SH50_Data'
        yahoo_stock_file = './Yahoo_SH50_Data/'+str(stock_code)+'.csv'
        if not os.path.exists(yahoo_dir):
            os.mkdir(yahoo_dir)

        if os.path.exists(yahoo_stock_file) and os.path.getsize(yahoo_stock_file)/float(1024) > 3:
            print("stock data already get...")
            return 0

        with open(yahoo_stock_file, "wb") as stock_file:
            stock_file.write(codecs.BOM_UTF8)
            writer = csv.writer(stock_file)
            writer.writerow(['Date', 'Open', 'High', 'Low', 'Close', 'Adj_Close'])

            with open('./Yahoo_SH50_HTML/yahoo_'+str(stock_code)+'.html','rb') as f_html:
                soup = BeautifulSoup(f_html, 'lxml')
                page_table = soup.find_all('table',{'class':'W(100%) M(0)'})[0]
                row_info_all = page_table('tr')[::-1]
                # 由于之前的倒序处理，需要将标题行截去
                for row_info in row_info_all[1:-1]:
                    if len(row_info('span')) < 3:
                        continue
                    str_date      = str(row_info('span')[0].get_text().strip())
                    # 转换为时间戳
                    time_stamp = time.mktime(time.strptime(str_date,'%b %d, %Y'))
                    # 时间戳转为%Y-%m-%d的格式
                    s_date = time.strftime('%Y-%m-%d',time.localtime(time_stamp))
                    s_open      = row_info('span')[1].get_text().strip()
                    s_high      = row_info('span')[2].get_text().strip()
                    s_low       = row_info('span')[3].get_text().strip()
                    s_close     = row_info('span')[4].get_text().strip()
                    s_adj_close = row_info('span')[5].get_text().strip()
                    try:
                        writer.writerow([s_date,s_open,s_high,s_low,s_close,s_adj_close])
                    except IOError as e:
                        print("IOError... "+ str(e))
                        return -1
        return 0


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

def get_yahoo_sh50_hist_data():
    """
    读取SH50.name文件中的信息，作为参数传入进行爬取
    :return:0/-1
    """
    start_year = 2010
    end_year   = 2017
    code_list = StockSpider.get_sh50_code()
    for code in code_list:
        stock_spider = StockSpider(code, start_year, end_year)
        stock_spider.fetch_yahoo_page_html(21)
    return 0

def create_data_file_from_html():
    """
    根据code_list,将对应的源码文件转化为data.csv文件
    :return:0/-1
    """
    code_list = StockSpider.get_sh50_code()
    for code in code_list:
        StockSpider.get_data_from_html(code)
        print('code %s try out data finish...' % code)

def test_main():
    pass
    # stock_spider = StockSpider(600000, 2010, 2017)
    # stock_spider.fetch_yahoo_page_html(21)
    # page_data = stock_spider.fetch_stock_data()
    # print(page_data)
    # stock_spider.stock_data_file()
    # StockSpider.get_sh50_code()
    # stock_spider.get_data_from_html()

if __name__ == "__main__":
    # get_yahoo_sh50_hist_data()
    # test_main()
    # test_main()
    create_data_file_from_html()