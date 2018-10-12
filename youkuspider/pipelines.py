# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import datetime
import logging

import pymysql
from scrapy.utils.project import get_project_settings
from langdetect import detect

from youkuspider.translate import Translate
from youkuspider.videodownload import VdieoDownload


class Mysql(object):
    """存储到数据库中"""

    def __init__(self):
        settings = get_project_settings()
        self.host = settings["DB_HOST"]
        self.port = settings["DB_PORT"]
        self.user = settings["DB_USER"]
        self.pwd = settings["DB_PWD"]
        self.name = settings["DB_NAME"]
        self.charset = settings["DB_CHARSET"]

        self.connect()

    def connect(self):
        self.conn = pymysql.connect(host=self.host,
                                    port=self.port,
                                    user=self.user,
                                    password=self.pwd,
                                    db=self.name,
                                    charset=self.charset)
        self.cursor = self.conn.cursor()

    def colose_spider(self, spider):
        self.conn.close()
        self.cursor.close()


class YoukuspiderPipeline(Mysql):

    def process_item(self, item, spider):
        try:
            d = VdieoDownload(db=self.conn, cursor=self.cursor)
            d.Automatic_download()
        except Exception as e:
            print(e)
            logging.error('下载失败 %s' % e)
        return item


class MysqlPipeline(Mysql):
    """存储到数据库中"""

    def colose_spider(self, spider):
        self.conn.close()
        self.cursor.close()

    def process_item(self, item, spider):

        # 查重处理
        self.cursor.execute(
            """select * from videoitems where url = %s""",
            item['url'])
        # 是否有重复数据
        repetition = self.cursor.fetchone()

        # 重复
        if repetition:
            print("此条重复抓取，没有存入数据库")
        elif int(item['video_time_long']) < int(item['video_time']) or \
                int(item['video_time']) < int(item['video_time_short']):
            print('视频时间不满足要求')
        elif int(item['upload_time']) >= int(item['start_date']) and int(item['upload_time']) <= int(item['end_date']):
            item['upload_time'] = self.ts2dts(item['upload_time'])
            # dt = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            dt = datetime.datetime.now().strftime("%Y-%m-%d")
            if detect(item['title']) != 'zh-cn':
                t = Translate(q=item['title'])  # 翻译
                item['title_cn'], item['language'] = t.translate()
            else:
                item['title_cn'] = item['title']
                item['language'] = '中文'
            sql = 'insert into videoitems(title,keywords,spider_time,url,site_name,video_time,' \
                  'play_count,upload_time,info,video_category,tags,task_id,isdownload,lg,title_cn)' \
                  ' values( "%s","%s","%s","%s", "%s" ,"%s","%s", "%s", "%s","%s","%s","%s",0,"%s","%s")' \
                  % (item['title'], item['keywords'], dt, item['url'], item['site_name'], item['video_time'],
                     item["play_count"], item['upload_time'], item['info'],
                     item['video_category'], item['tags'], item['task_id'], item['language'], item['title_cn'])
            # 执行SQL语句
            self.cursor.execute(sql)
            self.conn.commit()
        else:
            print('发布日期不符合要求，没有存入数据库')
        return item

    def ts2dts(self, timeStamp):
        '''timestamp translate to datestring'''
        import time
        timeArray = time.localtime(timeStamp)
        datestr = time.strftime("%Y-%m-%d", timeArray)
        return datestr
