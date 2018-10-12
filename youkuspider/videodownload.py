#!/usr/bin/python3
# coding:utf8
import logging

import pymysql
import datetime
import youtube_dl
import json
import time


# 打开数据库连dic = json.dumps(duck)接
from scrapy.utils.project import get_project_settings


class VdieoDownload(object):
    """download videos """
    settings = get_project_settings()
    videos_save_dir = settings['VIDEOS_SAVE_DIR']

    def __init__(self, db, cursor):
        self.db = db
        self.url = ''
        self.title = ''
        self.title_cn = ''
        self.language = '中文'
        self.cursor = cursor
        self.videojson = {}
        self.play_count = ''
        self.keywords = ''
        self.info = ''
        self.upload_time = ''
        self.video_time = ''

    def _Query(self):
        # 使用cursor()方法获取操作游标

        # SQL 查询语句 每次取出一条
        sql = "select title,url,play_count,keywords,info,upload_time,spider_time,video_time,site_name,video_category,tags,task_id,lg,title_cn" \
              " from videoitems " \
              "where isdownload =0 limit 0,1 "
        try:
            self.cursor.execute(sql)
            # 获取所有记录列表
            results = self.cursor.fetchall()
            for row in results:
                self.title = row[0]
                self.url = row[1]
                self.play_count = row[2]
                self.keywords = self.keywords + row[3]
                self.info = row[4]
                self.upload_time = row[5]
                self.spider_time = row[6]
                self.video_time = row[7]
                self.site_name = row[8]
                self.video_category = row[9]
                self.tags = row[10]
                self.task_id = row[11]
                self.language = row[12]
                self.title_cn = row[13]
            if results == ():
                return False
            else:
                return True
        except:
            print("Error: unable to fetch data")
            return False

    def UpdateStatus(self, num):
        # SQL 更新语句 更改isdownload的值
        sql = "UPDATE videoitems SET isdownload =%d WHERE url = '%s'" % (num, self.url)
        try:
            # 执行SQL语句
            self.cursor.execute(sql)
            # 提交到数据库执行
            self.db.commit()
        except:
            # 发生错误时回滚
            self.db.rollback()

    def Download(self):
        # 下载视频
        self.dt = datetime.datetime.now().strftime("%Y-%m-%d")
        options = {}
        options['retries'] = 2
        # options['proxy'] = 'socks5://127.0.0.1:1080'
        options['outtmpl'] = self.videos_save_dir + '/' + self.keywords.replace(' ', '_') + "/" + self.dt + "/" + \
                             self.title + '.%(ext)s'
        ydl = youtube_dl.YoutubeDL(options)
        if self.url != '':
            with ydl:
                result = ydl.extract_info(
                    url=self.url,
                    download=True  # We can extract the info and download the video
                )
            if 'entries' in result:
                # Can be a playlist or a list of videos
                video = result['entries'][0]
            else:
                # Just a video
                video = result

            self.videojson["title"] = self.title
            self.videojson["title_cn"] = self.title_cn
            self.videojson["task_id"] = self.task_id
            self.videojson["upload_time"] = self.upload_time
            self.videojson["spider_time"] = self.spider_time
            self.videojson["url"] = self.url
            self.videojson["info"] = '' + video.get('description', '')
            self.videojson["site_name"] = self.site_name
            self.videojson["site_name_cn"] = self.site_name
            self.videojson["play_count"] = self.play_count
            self.videojson["section"] = self.video_category
            self.videojson["video_lang"] = self.language
            # self.videojson["keywords"] = video.get('tags',[])
            if video.get('tags', []) is None:
                self.videojson["keywords"] = []
            else:
                self.videojson["keywords"] = video.get('tags', [])
            self.videojson["video_time"] = self.video_time
            # 生成关于视频的json文件

    def WriteJson(self):
        videojson = json.dumps(self.videojson, ensure_ascii=False)
        try:
            with open(self.videos_save_dir + '/' + self.keywords.replace(' ', '_') +
                      "/" + self.dt + "/" + self.videojson['title'] + ".json", 'w',
                      encoding='utf-8') as fq:
                fq.write(videojson)
        except:
            pass

    def AddVideoJson(self):
        # SQL 更新语句 更新视频的信息
        tags = json.dumps(self.videojson["keywords"], ensure_ascii=False)
        info = json.dumps(self.videojson["info"], ensure_ascii=False)
        sql = "UPDATE videoitems SET upload_time = '%s',info='%s' ,play_count='%s',tags='%s'\
              WHERE url = '%s'" % \
              (self.videojson["upload_time"], info, self.videojson["play_count"], tags, self.url)
        try:
            # 执行SQL语句
            self.cursor.execute(sql)
            # 提交到数据库执行
            self.db.commit()
        except EOFError as e:
            # 发生错误时回滚
            print(e)
            self.db.rollback()

    def Automatic_download(self):
        import threading
        l = threading.Lock()
        l.acquire()
        self._Query()
        self.UpdateStatus(num=1)
        try:
            self.Download()
            self.UpdateStatus(num=2)
            self.WriteJson()
            self.AddVideoJson()

        except EOFError as e:
            print(e)
            print('下载失败')
            self.UpdateStatus(num=3)

        l.release()


if __name__ == '__main__':
    db = pymysql.connect("localhost", "root", "root", "test", charset='utf8')
    d = VdieoDownload(db=db)
    d.Automatic_download()
    # 关闭数据库连接
    db.close()
