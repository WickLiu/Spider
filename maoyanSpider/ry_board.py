import requests
import re
from requests.exceptions import RequestException
import time
import pymongo
import json


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
    pattern = re.compile('<dd>.*?board-index.*?>(\d+)</i>.*?<a.*?title="(.*?)".*?'
                         +'star">(.*?)</p>.*?releasetime">(.*?)</p>.*?integer">'
                          +'(.*?)</i>.*?fraction">(\d+)</i>.*?</dd>', re.S)
    items = re.findall(pattern, html)
    for item in items:
        yield{
            'index': item[0],
            'title': item[1],
            'actor': item[2].strip()[3:],
            'time': item[3][5:],
            'score': item[4] + item[5]
        }

"""def write_file(content):
    with open('result.txt', 'a', encoding='utf-8') as f:
        f.write(json.dumps(content, ensure_ascii=False) + '\n')"""



def add_mongodb(content):
    client = pymongo.MongoClient(host='localhost', port=27017)
    db = client['remen']
    collection = db['item']
    collection.insert_one(content)

def main():
    url = 'https://maoyan.com/board/7'
    html = get_page(url)
    for item in parse_page(html):
        print(item)
        #write_file(item)
        add_mongodb(item)


if __name__ == '__main__':
    main()


