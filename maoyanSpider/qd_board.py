import json
import requests
from pyquery import PyQuery as pq
import time
from requests.exceptions import RequestException
import pymongo

def get_page(url):
    """
    请求网页
    :param url:
    :return:
    """
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36'
        }
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            return response.text
        return None
    except RequestException:
        return None

def parse_page(html):
    """
    页面解析
    :param html:
    :return:
    """
    doc = pq(html)
    items = doc('#app .board-wrapper dd').items()
    for item in items:
        movie = {}
        movie['index'] = item.find('i').text()
        movie['title'] = item.find('.name a').text()
        movie['actor'] = item.find('.movie-item-info .star').text()
        movie['time'] = item.find('.movie-item-info .releasetime').text()
        yield movie


def add_mongodb(content):
    """
    存储到MongoDB
    :param content:
    :return:
    """
    client = pymongo.MongoClient(host= 'localhost', port = 27017)
    db = client['qidai']
    collection = db['item']
    collection.insert_one(content)

"""def write_file(item):
    #本地存储
    with open('result1.txt', 'a', encoding='utf-8') as f:
        f.write(json.dumps(item, ensure_ascii=False) + '\n')
"""

def main(offset):
    """
    构建URL，存储数据
    :param offset:
    :return:
    """
    url = 'https://maoyan.com/board/6?offset=' + str(offset)
    html = get_page(url)
    for item in parse_page(html):
        print(item)
        add_mongodb(item)
        #write_file(item)


if __name__ == '__main__':
    """
    构建循环
    """
    for i in range(5):
        main(offset=i*10)
        time.sleep(1)