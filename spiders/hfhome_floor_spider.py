# -*- coding: utf-8 -*-

import scrapy
import logging
import threading
import MySQLdb
from bs4 import BeautifulSoup
from fangzidian.floor_items import FloorItem

class HfhomeFloorSpider(scrapy.Spider):
    name = "hfhome_floor"
    allowed_domains = ["hfhome.cn"]
    start_urls = [
        "http://news.hfhome.cn/list"
    ]

    handle_httpstatus_list = [521]

    FLAG_INTERRUPT = False
    SELECT_FLOOR_BY_TITLE = "SELECT * FROM news_list WHERE title='%s'"

    lock = threading.RLock()
    conn = MySQLdb.connect(host='127.0.0.1', user='root', passwd='root', db='news')
    conn.set_character_set('utf8')
    cursor = conn.cursor()
    cursor.execute('SET NAMES utf8;')
    cursor.execute('SET CHARACTER SET utf8;')
    cursor.execute('SET character_set_connection=utf8;')

    def is_not_saved(self, title):
        if self.FLAG_INTERRUPT:
            self.lock.acquire()
            rows = self.cursor.execute(self.SELECT_FLOOR_BY_TITLE % title)
            if rows > 0:
                logging.log(logging.INFO,"Floor saved all finished.")
                return False
            else:
                return True
            self.lock.release()
        else:
            return True

    def parse(self, response):
        logging.log(logging.INFO, "Start to parse page " + response.url)
        url = response.url
        items = []
        soup = BeautifulSoup(response)
        try:
            response = response.body
            # print (soup)
            lists = response.xpath('//*[@class="dt_b3"]/ul/li')
        except:
            items.append(self.make_requests_from_url(url))
            logging.log(logging.INFO, "Page " + url + " parse ERROR, try again !")
            return items
        need_parse_next_page = True
        if len(lists) > 0:
            for i in range(0, len(lists)):
                link = lists[i].xpath('div[@class="news"]/a/@href').extract()
                title = lists[i].xpath('div[@class="news"]/a/text()').extract()
                need_parse_next_page = self.is_not_saved(title)
                if not need_parse_next_page:
                    break
                #link = 'http://news.hfhome.cn/'+link
                if len(link) :  
                    link = link[0]
                    items.append(self.make_requests_from_url(link).replace(callback=self.parse_floor, meta={'title': title}))
            if (soup.find('a', text=u'下一页')['href'].startswith('http://')):
                page_next = soup.find('a', text=u'下一页')['href']
                if need_parse_next_page:
                    items.append(self.make_requests_from_url(page_next))
            return items


    def parse_floor(self, response):
        logging.log(logging.INFO,"Start to parse news " + response.url)
        item = FloorItem()
        day = title = _type = keywords = url = article = ''
        url = response.url
        item['title'] = response.meta['title']
        return item
