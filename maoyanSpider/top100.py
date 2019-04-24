import json
import time
import pymongo
import requests
from requests.exceptions import RequestException
from lxml import etree


def get_page(url):
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
    content = etree.HTML(html)
    results = content.xpath('//dl[@class="board-wrapper"]/dd')
    for result in results:
        item = {}
        item['title']= result.xpath('.//p[@class="name"]/a//text()')[0]
        item['actor']= result.xpath('.//p[@class="star"]//text()')[0].strip()
        item['time']= result.xpath('.//p[@class="releasetime"]//text()')[0]
        item['index']= result.xpath('./i//text()')[0]
        item['score']= result.xpath('.//p[@class="score"]/i[@class="integer"]//text()')[0] + result.xpath('.//p[@class="score"]/i[@class="fraction"]//text()')[0]
        yield item


def add_mongodb(content):
    client = pymongo.MongoClient(host= 'localhost', port= 27017)
    db = client['top']
    collection = db['item']
    collection.insert_one(content)

"""def write_file(content):
    with open('result2.txt', 'a', encoding='utf-8') as f:
        f.write(json.dumps(content, ensure_ascii=False) + '\n')
"""

def main(offset):
    url = 'https://maoyan.com/board/4?offset=' + str(offset)
    html = get_page(url)
    for item in parse_page(html):
        print(item)
        add_mongodb(item)
        #write_file(item)


if __name__ == '__main__':
    for i in range(10):
        main(i*10)
        time.sleep(1)