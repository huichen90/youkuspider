# -*- coding: utf-8 -*-
# -*- coding: utf-8 -*-
import json
import logging
import re
import stat

from lxml import etree

import scrapy
import time
import pymysql
from scrapy.utils.project import get_project_settings
from youkuspider.items import YoukuspiderItem


# from youkuspider.videodownload import VdieoDownload


class YoukuSpider(scrapy.Spider):
    name = '关键词采集'

    def __init__(self, keywords='中国好声音', video_time_long="1000", video_time_short="0", taskId=3,
                 startDate=int(time.time()) - 3600 * 24 * 7,
                 endDate=int(time.time())+3600, num=5, *args, **kwargs):
        super(YoukuSpider, self).__init__(*args, **kwargs)
        self.keywords = keywords
        self.video_time_long = video_time_long
        self.video_time_short = video_time_short
        self.start_date = startDate
        self.end_date = endDate
        self.task_id = taskId
        self.num = num
        self.site_name = '优酷'
        self.info = '无简介'
        self.video_category = '未分类'
        self.allowed_domains = ['www.soku.com', 'v.youku.com', 'so.youku.com']
        self.url1 = 'http://so.youku.com/search_video/q_' + self.keywords + '?f=1&limit_date=1&spm=a2h0k.11417342' \
                                                                            '.filter.dpubtime&pg= '
        self.page = 1
        self.start_urls = [self.url1 + '1']

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

    def parse(self, response):
        # print(response.text)
        res1 = response.xpath('//script[@type="text/javascript"][2]/text()').extract_first()
        pattern = re.compile(r'bigview.view(.*)')
        res2 = pattern.search(res1).group(1)[1:-1]
        res3 = json.loads(res2)['html']
        selector = etree.HTML(res3)
        div_list = selector.xpath('//div[@class="sk-result-list"]/div[@class="sk-mod"]')
        # 创建对象
        item = YoukuspiderItem()
        for div in div_list:
            try:
                item['video_time_long'] = self.video_time_long
                item['video_time_short'] = self.video_time_short
                title = div.xpath('.//h2[@class="spc-lv-1"]/a/@title')[0]
                video_url = div.xpath('.//h2[@class="spc-lv-1"]/a/@href')[0]
                url = "http:" + video_url
                video_time = div.xpath('.//span[@class="pack-rb pack-time"]/text()')[0]
                play_count = div.xpath('.//div[@class="mod-info"]//text()')[0]
                upload_time = div.xpath('.//div[@class="mod-info"]//span[@class="spc-lv-4"][1]/text()')[0]
                pattern = re.compile(r'播放量: (.*?)</span>')
                play_count = pattern.search(play_count).group(0) if pattern.search(play_count) is not None else 0
                # 将上面的数据存储到对象中
                item['title'] = self.translation(title).strip()
                item['url'] = url
                video_time1 = self.translate_time(video_time)
                item['video_time'] = video_time1
                item['play_count'] = play_count
                item['keywords'] = self.keywords
                item['site_name'] = self.site_name
                item['tags'] = []
                item['task_id'] = self.task_id
                item['video_category'] = self.video_category
                item['start_date'] = self.start_date
                item['end_date'] = self.end_date
                item['upload_time'] = self.dts2ts(upload_time)
                item['info'] = self.info
            except Exception as e:
                print(e)

            yield item
            # yield scrapy.Request(url=url, callback=self.parse_info, meta={'item': item})
        self.page += 1

        if self.page <= 5:
            # print("开始爬去第%d页" % self.page)
            url = self.url1 + str(self.page)
            time.sleep(5)
            # 再次发送请求
            yield scrapy.Request(url=url, callback=self.parse)

    def parse_info(self, response):
        # 获取传过来的参数
        item = response.meta['item']
        # upload_time=response.xpath('//span[@class=\"video-status\"]//span/text()').extract_first()
        import re
        pattern = re.compile(r'上传于 (.*?)<')
        upload_time = pattern.search(response.text).group(1)

        item['upload_time'] = self.dts2ts(upload_time)
        item['info'] = self.info

        yield item

    def translation(self, instring):
        """去掉数据中的空格换行等字符"""
        move = dict.fromkeys((ord(c) for c in u"\xa0\n\t|:：<>?\\/*’‘“”"))
        outstring = instring.translate(move)
        return outstring

    def translate_time(self, videotime):
        """将文本格式的时间转换为int，以秒为单位"""
        l1 = videotime.split(':')
        # print(l1)
        if len(l1) == 2:
            return int(l1[-2]) * 60 + int(l1[-1])
        if len(l1) == 3:
            return int(l1[-3]) * 3600 + int(l1[-2]) * 60 + int(l1[-1])

    def dts2ts(self, datestr):
        """datestring translate to timestamp"""
        import time
        try:
            if len(datestr) == 10:
                timeArray = time.strptime(datestr, "%Y-%m-%d")
                timeStamp = int(time.mktime(timeArray))
                return timeStamp
            else:
                timeArray = time.strptime(datestr, "%Y%m%d")
                timeStamp = int(time.mktime(timeArray))
                return timeStamp
        except Exception as e:
            return int(time.time())

    def close(self, spider):
        # 当爬虫退出的时候 关闭chrome
        import datetime
        import os
        from scrapy.utils.project import get_project_settings

        dt = datetime.datetime.now().strftime("%Y-%m-%d")
        settings = get_project_settings()
        videos_save_dir = settings['VIDEOS_SAVE_DIR']
        path = os.getcwd()  # 获取当前路径
        count = 0
        sizes = 0
        for root, dirs, files in os.walk(
                path + "/" + videos_save_dir + "/" + self.keywords.replace(' ', '_') + "/" + dt):  # 遍历统计
            for each in files:
                size = os.path.getsize(os.path.join(root, each))  # 获取文件大小
                os.chmod(os.path.join(root, each), stat.S_IRWXO + stat.S_IRWXG + stat.S_IRWXU)
                sizes += size
                count += 1  # 统计文件夹下文件个数
        count = count // 2
        sizes = sizes / 1024.0 / 1024.0
        sizes = round(sizes, 2)
        videojson = {}
        videojson['title'] = self.keywords
        videojson['time'] = dt
        videojson['keywords'] = self.keywords
        videojson['file_number'] = count
        videojson['file_size'] = str(sizes) + 'M'
        dt = datetime.datetime.now().strftime("%Y-%m-%d")
        videojson = json.dumps(videojson, ensure_ascii=False)
        with open(videos_save_dir + '/' + self.keywords.replace(' ', '_') + "/" + dt + "/" + "task_info.json",
                  'w', encoding='utf-8') as fq:
            fq.write(videojson)
        os.chmod(videos_save_dir + '/' + self.keywords.replace(' ', '_') + "/" + dt + "/" + "task_info.json",
                 stat.S_IRWXO + stat.S_IRWXG + stat.S_IRWXU)
        os.chmod(videos_save_dir + '/' + self.keywords.replace(' ', '_') + "/" + dt,
                 stat.S_IRWXO + stat.S_IRWXG + stat.S_IRWXU)
        os.chmod(videos_save_dir + '/' + self.keywords.replace(' ', '_'),
                 stat.S_IRWXO + stat.S_IRWXG + stat.S_IRWXU)
        os.chmod(videos_save_dir,
                 stat.S_IRWXO + stat.S_IRWXG + stat.S_IRWXU)
        print("spider closed")
