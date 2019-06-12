from bs4 import BeautifulSoup
import re
import os
import requests
import cchardet
from requests.exceptions import RequestException
from multiprocessing import Pool
from urllib.parse import urlencode


headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.80 Safari/537.36'
}

#1.获取索引界面的内容
def get_page_index(i):
    #下载1页
    #url = 'https://www.thepaper.cn/newsDetail_forward_2370041'

    #下载多页，构造url
    paras = {
        'nodeids': 25635,
        'pageidx': i,
        'isList': True
    }
    url = 'https://www.thepaper.cn/load_index.jsp?' + urlencode(paras)

    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        encoding = cchardet.detect(response.content)['encoding']
        html = response.content.decode(encoding)
        #print(html)
        return html

#2.解析索引界面网页的内容
def parse_page_index(html):
    soup = BeautifulSoup(html, 'lxml')
    #获取每页的章数
    num = soup.find_all(name='div', class_='news_li')
    for i in range(len(num)):
        yield{
            #获取title
            'title': soup.select('h2 a')[i].get_text(),
            #获取图片url,需加前缀
            'url': 'https://www.thepaper.cn/' + soup.select('h2 a')[i].attrs['href']
        }


#获取每条文章的详情页 内容
def get_page_detail(item):
    url = item.get('url')
    print(url)
    global title_global
    title_global = item.get('title')
    #增加异常模块
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            encoding = cchardet.detect(response.content)['encoding']
            html = response.content.decode(encoding)
            return html
    except RequestException:
        print('网页请求错误')
        return None

#解析每条文章的详情页内容
def parse_page_detail(html):
    soup = BeautifulSoup(html, 'lxml')
    #获取title
    items = soup.find_all(name='img', width=['100%', '600'])
    #有的图片节点用width='100%'表示，有的用600表示，因此用list合并
    for i in range(len(items)):
        pic = items[i].attrs['src']
        print(pic)
        yield {
            'title': title_global,
            'pic': pic,
            'num': i
        }

def save_pic(pic):
    title = pic.get('title')
    #标题规范命名：去掉符号非法字符|等
    #title = pic.get('title').replace(' ','').replace('|', '').replace('?', '').replace('“', '').replace('”', '')
    title = re.sub('[\/:*?"<>|]', '-', title).strip()
    url = pic.get('pic')
    num = pic.get('num')
    if not os.path.exists(title):
        os.mkdir(title)

    response = requests.get(url, headers=headers)
    try:
        #建立图片存放的地址
        if response.status_code == 200:
            file_path = '{0}/{1}.{2}'.format(title, num, 'jpg')
            #文件名采用编号方便按顺序查看
            if not os.path.exists(file_path):
                #开始下载图片
                with open(file_path, 'wb') as f:
                    f.write(response.content)
                    print('文章"{0}"的第{1}张图片下载完成'.format(title, num))
            else:
                print('该图片%s已下载' % title)
    except RequestException as e:
        print(e, '图片获取失败')
        return None

def main(i):
    html = get_page_index(i)
    data = parse_page_index(html)#测试索引界面url是否获取成功ok
    for item in data:
        html_detail = get_page_detail(item)
        if html_detail:
            data_detail = parse_page_detail(html_detail)
            if data_detail:
                for pic in data_detail:
                    save_pic(pic)

if __name__ == '__main__':
    pool = Pool(4)
    pool.map(main, [i for i in range(1,26)])
    pool.close()
    pool.join()

















