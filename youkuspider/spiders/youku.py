# -*- coding: utf-8 -*-
# -*- coding: utf-8 -*-
import json
import logging
import re
from lxml import etree

import scrapy
import time
import pymysql
from scrapy.utils.project import get_project_settings
from youkuspider.items import YoukuspiderItem

# from youkuspider.videodownload import VdieoDownload


class YoukuSpider(scrapy.Spider):
    name = 'youku'

    def __init__(self,keywords='金正恩',limit=600,taskId=3,startDate=int(time.time())-3600*48,
                 endDate=int(time.time()),num=5,*args,**kwargs):
        super(YoukuSpider, self).__init__(*args, **kwargs)
        self.keywords = keywords
        # keywords = 'hello'
        self.limit_time = limit
        self.start_date = startDate
        self.end_date = endDate
        self.task_id = taskId
        self.num = num
        self.site_name = '优酷'
        self.info ='无简介'
        self.video_category = '未分类'
        self.allowed_domains = ['www.soku.com','v.youku.com']
        self.url1 = 'http://www.soku.com/search_video/q_'+self.keywords+'_orderby_1?site=14&&limitdate=7&page='
        self.page = 1
        self.start_urls = [self.url1+'1']

    #     settings = get_project_settings()
    #     self.host = settings["DB_HOST"]
    #     self.port = settings["DB_PORT"]
    #     self.user = settings["DB_USER"]
    #     self.pwd = settings["DB_PWD"]
    #     self.name = settings["DB_NAME"]
    #     self.charset = settings["DB_CHARSET"]
    #     self.connect()
    #     self.spider_random_id = self.random_string()
    #     self.cursor.execute('insert into running_job(spider_random_id) values("%s") '%(self.spider_random_id))
    #     self.conn.commit()
    # def connect(self):
    #     self.conn = pymysql.connect(host = self.host,
    #                                 port = self.port,
    #                                 user = self.user,
    #                                 password = self.pwd,
    #                                 db = self.name,
    #                                 charset = self.charset)
    #     self.cursor = self.conn.cursor()

    # 生成随机的字符串
    # def random_string(self,length=10):
    #     import random
    #     base_str = 'abcdefghijklmnopqrstuvwxyz1234567890'
    #     return ''.join(random.choice(base_str) for i in range(length))
    def parse(self, response):
        # print(response.text)
        res1 = response.xpath('//script[@type="text/javascript"][2]/text()').extract_first()
        pattern = re.compile(r'bigview.view(.*)')
        res2 = pattern.search(res1).group(1)[1:-1]
        res3 = json.loads(res2)['html']
        selector = etree.HTML(res3)
        div_list = selector.xpath('//div[@class="sk-result-list"]/div[@class="sk-mod"]')
        print(len(div_list))

        for div in div_list:
            #创建对象
            item = YoukuspiderItem()

            title = div.xpath('.//h2[@class="spc-lv-1"]/a/@title')[0]
            print(title)
            video_url = div.xpath('.//h2[@class="spc-lv-1"]/a/@href')[0]
            url = "http:" + video_url
            print(url)
            video_time = div.xpath('.//span[@class="pack-rb pack-time"]/text()')[0]
            print(video_time)
            play_count = div.xpath('.//div[@class="mod-info"]//text()')[0]
            print(play_count)
            pattern = re.compile(r'播放量: (.*?)</span>')
            play_count = pattern.search(play_count).group(0) if pattern.search(play_count) is not None else 0
            # 将上面的数据存储到对象中
            print(play_count)
            item['title'] = self.translation(title).strip()
            item['url'] = url
            video_time1 = self.translate_time(video_time)
            item['video_time'] = video_time1
            item['play_count'] = play_count
            item['keywords'] = self.keywords
            item['site_name'] = self.site_name
            item['tags'] = []
            item['limit_time'] = self.limit_time
            item['task_id'] = self.task_id
            item['video_category'] = self.video_category
            item['start_date'] = self.start_date
            item['end_date'] = self.end_date

            yield scrapy.Request(url=url, callback=self.parse_info,meta={'item':item})
        self.page += 1

        if self.page <= 1:
            # print("开始爬去第%d页" % self.page)
            url = self.url1 + str(self.page)
            time.sleep(5)
            # 再次发送请求
            yield scrapy.Request(url=url, callback=self.parse)

    def parse_info(self,response):
        # 获取传过来的参数
        item = response.meta['item']
        # upload_time=response.xpath('//span[@class=\"video-status\"]//span/text()').extract_first()
        import re
        pattern = re.compile(r'上传于 (.*?)<')
        upload_time =pattern.search(response.text).group(1)

        item['upload_time'] = self.dts2ts(upload_time)
        item['info'] = self.info

        yield item

    def translation(self,instring):
        """去掉数据中的空格换行等字符"""
        move = dict.fromkeys((ord(c) for c in u"\xa0\n\t|:：<>?\\/*’‘“”"))
        outstring = instring.translate(move)
        return outstring

    def translate_time(self,videotime):
        """将文本格式的时间转换为int，以秒为单位"""
        l1 = videotime.split(':')
        # print(l1)
        if len(l1) == 2:
            return int(l1[-2]) * 60 + int(l1[-1])
        if len(l1) == 3:
            return int(l1[-3]) * 3600 + int(l1[-2]) * 60 + int(l1[-1])

    def dts2ts(self,datestr):
        """datestring translate to timestamp"""
        import time
        if len(datestr) == 10:
            timeArray = time.strptime(datestr, "%Y-%m-%d")
            timeStamp = int(time.mktime(timeArray))
            return timeStamp
        else:
            timeArray = time.strptime(datestr, "%Y%m%d")
            timeStamp = int(time.mktime(timeArray))
            return timeStamp

    def close(self, spider):
        # 当爬虫退出的时候
        import datetime
        import os
        # 删除运行id
        # print('..................')
        # self.cursor.execute('delete from running_job where spider_random_id="%s" ' % (self.spider_random_id))
        # self.conn.commit()
        # self.cursor.execute('select * from running_job')
        # time.sleep(10)
        # if self.cursor.fetchall() == ():
        #     print("开始下载.....")
        #     for i in range(self.num):
        #         try:
        #             d = VdieoDownload(db=self.conn,cursor=self.cursor)
        #             d.Automatic_download()
        #         except EOFError as e:
        #             logging.error('下载失败 %s'%e)

        dt = datetime.datetime.now().strftime("%Y-%m-%d")

        path = os.getcwd()  # 获取当前路径
        count = 0
        sizes = 0
        for root, dirs, files in os.walk(path+"/"+self.keywords+"/"+dt):  # 遍历统计
            for each in files:
                size = os.path.getsize(os.path.join(root, each))  # 获取文件大小
                sizes += size
                count += 1  # 统计文件夹下文件个数
        count = int(count/2)
        sizes = sizes / 1024.0 / 1024.0
        sizes = round(sizes, 2)
        videojson = {}
        videojson['title']=self.keywords
        videojson['time'] = dt
        videojson['keywords'] = self.keywords
        videojson['file_number'] = count
        videojson['file_size'] = str(sizes)+'M'
        dt = datetime.datetime.now().strftime("%Y-%m-%d")
        videojson = json.dumps(videojson, ensure_ascii=False)
        with open(self.keywords+"/"+dt + "/"+"task_info.json",'w',encoding='utf-8' ) as fq:
            fq.write(videojson)
        print("spider closed")
        # self.conn.close()
        # self.cursor.close()






