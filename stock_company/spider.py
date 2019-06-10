import requests
from requests.exceptions import RequestException
from bs4 import BeautifulSoup
import pandas as pd
from lxml import etree
import pymysql
import time
from urllib.parse import urlencode
from sqlalchemy import create_engine
from multiprocessing import Pool

start_time = time.time()

def get_one_page(offset):
    try:
        headers = {
            'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.80 Safari/537.36'
        }
        paras = {
            'reportTime': '2019-03-31',
            'pageNum': offset
        }
        url = 'http://s.askci.com/stock/a/?' + urlencode(paras)
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            return response.text
        return None
    except RequestException:
        print('Fail!!!')

def parse_one_page(html):
    soup = BeautifulSoup(html, 'lxml')
    content = soup.select('#myTable04')[0]
    table1 = pd.read_html(content.prettify(), header=0)[0]
    table1.rename(columns = {'序号':'serial_number','股票代码':'stock_code','股票简称':'stock_abbre','公司名称':'company_name','省份':'province', '城市':'city', '主营业务收入(201712)':'main_bussiness_income', '净利润(201712)':'net_profit', '员工人数':'employees', '上市日期':'listing_date', '招股书':'zhaogushu', '公司财报':'financial_report', '行业分类':'industry_classification', '产品类型':'industry_type', '主营业务':'main_business'}, inplace=True)
    print(table1)
    return table1

def save_mysql():
    conn = pymysql.connect(
        host = 'localhost',
        user = 'root',
        password = '20170723',
        port = 3306,
        charset = 'utf8',
        db = 'stock'
    )
    cursor = conn.cursor()

    sql = 'CREATE TABLE IF NOT EXISTS listed_company (serial_number INT(20) NOT NULL,stock_code INT(20) ,stock_abbre VARCHAR(20) ,company_name VARCHAR(20) ,province VARCHAR(20) ,city VARCHAR(20) ,main_bussiness_income VARCHAR(20) ,net_profit VARCHAR(20) ,employees INT(20) ,listing_date DATETIME(0) ,zhaogushu VARCHAR(20) ,financial_report VARCHAR(20) , industry_classification VARCHAR(20) ,industry_type VARCHAR(100) ,main_business VARCHAR(200) ,PRIMARY KEY (serial_number))'

    cursor.execute(sql)
    conn.close()

def write_to_mysql(table, db='stock'):
    engine = create_engine('mysql+pymysql://root:20170723@localhost:3306/{0}?charset=utf8'.format(db))
    try:
        table.to_sql('listed_company2', con = engine, if_exists = 'append', index = False)
        #append表示在原有的表基础上增加，但该表要有表头
    except Exception as e:
        print(e)

def main(offset):
    save_mysql()
    for i in range(1,offset):
        html = get_one_page(i)
        table = parse_one_page(html)
        write_to_mysql(table)


#单进程
#if __name__ == '__main__':
    #main(179)

    #end_time = time.time() - start_time
    #print('程序运行了{}秒'.format(end_time))

#多进程
if __name__ == '__main__':
    pool = Pool(4)
    pool.map(main, [i for i in range(1,180)])

    end_time = time.time() - start_time
    print('程序运行了{}秒'.format(end_time))




