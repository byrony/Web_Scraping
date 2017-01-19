#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Dec 15 15:38:11 2016

@author: Vincent
"""
import requests
from bs4 import BeautifulSoup
import re
import pymysql
import csv
import os

class YelpSeattleRestaurant:
    """
    The code scrapes the restaurant information of Name, Price, Category, Star and
    Review count in Seattle areas. You can use it to scrape other areas but the url
    need be modified correctly.

    Params: url: the web page link of restaurant list.
            num_restaurant: the number of restaurant. Since Yelp has 10 restaurants
            per page, the code scrapes the largest multiple of 10 less than num_restaurant

    Note: MySQL database need be created before executing.
    """
    def __init__(self, url, num_restaurant, style):
        self.UA = 'Chrome/54 (Macintosh; Intel Mac OS X 10.11; rv:32.0) Gecko/20100101 Firefox/32.0'
        self.url = url
        self.page_num = [str(i) for i in range(0, num_restaurant, 10)]
        self.style = style

    def scrape_name(self, soup):
        # seach a outer tag and a inner tag
        name = ''
        name_div = soup.find_all('h3', {'class': 'search-result-title'})
        name_a = soup.find_all('a', {'class': 'biz-name js-analytics-click'})
        for d in name_a:
            name = name + d.string
        return name

    def scrape_price(self, soup):
        price = ''
        price_ = soup.find_all('span', {'class': 'business-attribute price-range'})
        for p in price_:
            price = price + p.string
        return price

    def scrape_category(self, soup):
        category = ''
        category_ = soup.find_all('span', {'class': 'category-str-list'})
        if category_:
            for c in category_:
                ## .get_text() returns the same as .text; ## replace both '\n' and space with ''
                category = category + c.get_text().strip().replace('\n', '').replace(' ', '')
        else:
            category = None
        return category

    def scrape_star(self, soup):
        star = ''
        star_ = soup.find_all('div', {'class': re.compile('i-stars i-stars*')})
        if star_:
            for s in star_:
                star_str = s['title'].split()[0]
                star = star + star_str
        else:
            star = None
        return star

    def scrape_review(self, soup):
        review_count = ''
        reviews = soup.find_all('span', {'class': 'review-count rating-qualifier'})
        if reviews:
            for r in reviews:
                review_count = review_count + r.text.strip().split()[0]
        else:
            review_count = None
        return review_count

    def scrape_address(self, soup):
        address = ''
        address_ = soup.find_all('address')
        if address_:
            for a in address_:
                address = address + a.text.strip()
        else:
            address = None
        return address

    def store(self, name, price, review_count, star, category, address):
        conn = pymysql.connect(host='localhost',
                               user='root',
                               passwd='root',
                               db='Yelp_scrape',
                               use_unicode=True,
                               charset="utf8")

        # create a cursor object in order to execute queries
        cur = conn.cursor()
        # create a new record
        sql = 'Insert into Restaurant (Name, Price, Review_count, ' \
              'Star, Category, Address) Values (%s, %s, %s, %s, %s, %s)'
        cur.execute(sql, (name, price, review_count, star, category, address))
        # commit changes
        cur.connection.commit()

    def write_mysql(self):
        for i, page in enumerate(self.page_num):
            print('Begin to scrape: Page %s' % (i))
            url_curr = self.url + page + '&cflt=' + self.style
            html = requests.get(url_curr, headers={'User-Agent': self.UA})
            soup = BeautifulSoup(html.content, 'lxml')

            restaurants_div = soup.find_all('div', {'class': 'biz-listing-large'})
            for restaurant in restaurants_div:
                name = self.scrape_name(restaurant)
                price = self.scrape_price(restaurant)
                category = self.scrape_category(restaurant)
                star = self.scrape_star(restaurant)
                review_count = self.scrape_review(restaurant)
                address = self.scrape_address(restaurant)
                # for each restaurant, store its information in MySQL database
                try:
                    self.store(name, price, review_count, star, category, address)
                except:
                    # print('Failed or error')
                    pass
            print('Finished scraping: Page %s' % (i))

    def write_csv(self):
        # if the file doesn't exist, create a new file
        file = 'yelp_' + self.style + '.csv'
        if not os.path.exists(file):
            with open(file, 'w') as f:
                writer = csv.writer(f)
                writer.writerow(['Name', 'Price', 'Review_count', 'Star', 'Category', 'Address'])
        else:
            pass
        # check each restaurant, append its info into csv file.
        for i, page in enumerate(self.page_num):
            print('Begin to scrape: Page %s' % (i))
            url_curr = self.url + page + '&cflt=' + self.style
            html = requests.get(url_curr, headers={'User-Agent': self.UA})
            soup = BeautifulSoup(html.content, 'lxml')

            restaurants_div = soup.find_all('div', {'class': 'biz-listing-large'})
            for restaurant in restaurants_div:
                name = self.scrape_name(restaurant)
                price = self.scrape_price(restaurant)
                category = self.scrape_category(restaurant)
                star = self.scrape_star(restaurant)
                review_count = self.scrape_review(restaurant)
                address = self.scrape_address(restaurant)
                with open(file, 'a') as f: # note the mode is 'a', append
                    writer = csv.writer(f)
                    writer.writerow([name, price, review_count, star, category, address])
            print('Finished scraping: Page %s' % (i))

url = 'https://www.yelp.com/search?find_desc=restaurants&find_loc=Seattle,+WA&start='
num_restaurant = 300
styles = ['chinese', 'japanese', 'thai', 'vietnamese', 'india',
          'American (New)', 'American (Traditional)']
Scraper = YelpSeattleRestaurant(url, num_restaurant, 'vietnamese')
Scraper.write_csv()