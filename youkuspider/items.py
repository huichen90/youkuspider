# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class YoukuspiderItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    title = scrapy.Field()
    keywords = scrapy.Field()
    tags = scrapy.Field()
    upload_time = scrapy.Field()
    url = scrapy.Field()
    info = scrapy.Field()
    site_name = scrapy.Field()
    video_time = scrapy.Field()
    play_count = scrapy.Field()
    video_category = scrapy.Field()
    limit_time = scrapy.Field()
    task_id = scrapy.Field()
    start_date = scrapy.Field()
    end_date = scrapy.Field()
