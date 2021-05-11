import scrapy
import pandas as pd
from datetime import datetime
from scrapy.http import FormRequest
from ..items import YahooScraperItem
from ..clean_text import clean



class YahooScraper(scrapy.Spider):
    name = 'yahoo'
    headers = {
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.90 Safari/537.36',
        'cookie': 'AO=u=1; B=37ivp71boddqc&b=4&d=nVvahyJtYFi7ZtHyNuDJ&s=jg&i=o27ljC5vf.GapligXPer; A1=d=AQABBLirFF4CECJSD7_m11XFtM2ZUN_YJMwFEgABAgHyFGD_YOA9b2UB9iMAAAcITLeGVzj5yzMID6Nu5Ywub3_hmqZYoFz3qwkBBwoBnA&S=AQAAAuZ2CH-cMRaxSlWxZjLARFg; A3=d=AQABBLirFF4CECJSD7_m11XFtM2ZUN_YJMwFEgABAgHyFGD_YOA9b2UB9iMAAAcITLeGVzj5yzMID6Nu5Ywub3_hmqZYoFz3qwkBBwoBnA&S=AQAAAuZ2CH-cMRaxSlWxZjLARFg; GUC=AQABAgFgFPJg_0IdzgSP; cmp=v=18&t=1611907728&j=1&o=106; APID=UPb12a4ff6-6209-11eb-9e1a-0225c6d4cd42; thamba=2; EuConsent=CPAxG36PAxG36AOACBDEBACoAP_AAH_AACiQHCNd_X_fb39j-_59__t0eY1f9_7_v20zjgeds-8Nyd_X_L8X_2M7vB36pr4KuR4ku3bBAQFtHOncTQmx6IlVqTPsak2Mr7NKJ7PEinsbe2dYGHtfn9VT-ZKZr97s___7________79______3_vt_9__wOCAJMNS-AizEscCSaNKoUQIQriQ6AEAFFCMLRNYQErgp2VwEfoIGACA1ARgRAgxBRiyCAAAAAJKIgBADwQCIAiAQAAgBUgIQAEaAILACQMAgAFANCwAigCECQgyOCo5TAgIkWignkrAEou9jDCEMooAaAAA; A1S=d=AQABBLirFF4CECJSD7_m11XFtM2ZUN_YJMwFEgABAgHyFGD_YOA9b2UB9iMAAAcITLeGVzj5yzMID6Nu5Ywub3_hmqZYoFz3qwkBBwoBnA&S=AQAAAuZ2CH-cMRaxSlWxZjLARFg&j=GDPR; PRF=t%3DBA%252BTSLA%252BGME; cmp=v=18&t=1616489507&j=1',
        'upgrade-insecure-requests': 1,
        'accept-language': 'en-US;q=0.8,en;q=0.7',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9'
    }
    df = pd.read_csv('D:\stock_sentiment\yahoo_scraper\stock_requests3.csv')

    def start_requests(self):
        for i in range(0, len(self.df) + 1):
            url = self.df.iloc[i]['link']
            ticker = self.df.iloc[i]['stock']
            publisher = self.df.iloc[i]['publisher']
            network = self.df.iloc[i]['network']
            if network == 'on_network':
                yield scrapy.Request(
                    url=url,
                    headers=self.headers,
                    meta={'ticker': ticker, 'publisher': publisher},
                    callback=self.parse_yahoo
                )
            else:
                yield scrapy.Request(
                    url=url,
                    headers=self.headers,
                    meta={'ticker': ticker, 'publisher': publisher},
                    callback=self.check_response
                )

    def check_response(self, response):
        items = YahooScraperItem()
        ticker = response.meta['ticker']
        publisher = response.meta['publisher']

        if response.meta['publisher'] == 'Motley Fool':
            self.parse_motley(response, ticker, items)

        if response.meta['publisher'] == "Investor's Business Daily":
            self.parse_investors(response, ticker, items)

        if response.meta['publisher'] == 'TheStreet.com':
            self.parse_street(response, ticker, items)

        if response.meta['publisher'] == 'Barrons.com':
            self.parse_barrons(response, ticker, items)
        yield items

    def parse_yahoo(self, response):
        if 'collectConsent' in response.url:
            yield FormRequest.from_response(
                response,
                formname='Form',
                meta={'ticker': response.meta['ticker'], 'publisher': response.meta['publisher']},
                clickdata={'name': 'agree', 'value': 'agree'},
                callback=self.parse_yahoo_article
            )
        else:
            items = YahooScraperItem()
            items['ticker'] = response.meta['ticker']
            items['publisher'] = response.meta['publisher']
            sections = response.xpath('.//div[@class="caas-body"]/p/text()').getall()
            items['article'] = clean(' '.join(sections))
            dt = response.xpath('.//div[@class="caas-attr-time-style"]/time/text()').get()
            items['date'] = datetime.strptime(''.join(dt.split(',', 2)[:2]), '%B %d %Y')
            items['url'] = response.url
            items['consensus'] = 'no_consensus'
            yield items

    def parse_yahoo_article(self, response):
        items = YahooScraperItem()
        items['ticker'] = response.meta['ticker']
        items['publisher'] = response.meta['publisher']
        sections = response.xpath('.//div[@class="caas-body"]/p/text()').getall()
        items['article'] = clean(' '.join(sections))
        dt = response.xpath('.//div[@class="caas-attr-time-style"]/time/text()').get()
        items['date'] = datetime.strptime(''.join(dt.split(',', 2)[:2]), '%B %d %Y')
        items['url'] = response.url
        items['consensus'] = 'yes_consensus'
        yield items

    def parse_motley(self, response, ticker, items):
        sections = response.xpath('.//span[@class="article-content"]/p/text()').getall()
        items['article'] = clean(''.join(sections))
        dt = response.xpath('.//div[@class="publication-date"]/text()').getall()[-1].split('at')[0].strip()
        if dt.startswith('Published'):
            items['date'] = datetime.strptime(dt.split('Published: ')[-1], '%b %d, %Y')
        else:
            items['date'] = datetime.strptime(dt, '%b %d, %Y')
        items['publisher'] = 'Motley Fool'
        items['ticker'] = ticker
        items['url'] = response.url
        items['consensus'] = 'no_consensus'

    def parse_investors(self, response, ticker, items):
        sections = response.xpath('.//div[@class="single-post-content post-content drop-cap"]/p/text()').getall()
        items['article'] = clean(''.join(sections))
        dt = response.xpath('.//li[@class="post-time"]/text()').get().split('ET ')[-1]
        items['date'] = datetime.strptime(dt, '%m/%d/%Y')
        items['publisher'] = 'Investors Daily'
        items['ticker'] = ticker
        items['url'] = response.url
        items['consensus'] = 'no_consensus'

    def parse_street(self, response, ticker, items):
        if 'realmoney.thestreet' in response.url:
            sections = response.xpath('.//div[@itemprop="articleBody"]/p/text()').getall()
            items['article'] = clean(''.join(sections))
            dt = response.xpath('.//time/text()').get().split('|')[0].strip()
            items['date'] = datetime.strptime(dt, '%b %d, %Y')
        else:
            sections = response.xpath('.//div[@class="m-detail--body"]/p/text()').getall()
            items['article'] = clean(''.join(sections))
            dt = response.xpath('.//time/text()').get()
            date_string = (dt.split(',')[0] + ' ' + dt.split(',')[-1][1:5])
            items['date'] = datetime.strptime(date_string, '%b %d %Y')
        items['publisher'] = 'TheStreet.com'
        items['ticker'] = ticker
        items['url'] = response.url
        items['consensus'] = 'no_consensus'

    def parse_barrons(self, response, ticker, items):
        sections = response.xpath('.//div[@class="snippet__body"]/text()').get()
        items['article'] = clean(sections)
        dt = response.xpath('.//time[contains(@class, "timestamp")]/text()').get()
        if dt.startswith('Updated'):
            date_string = dt.split('Updated ')[-1].split('/')[0].strip()
            items['date'] = datetime.strptime(date_string, '%B %d, %Y')
        else:
            date_string = (dt.split(',')[0] + ' ' + dt.split(',')[-1][1:5]).strip()
            items['date'] = datetime.strptime(date_string, '%b. %d %Y')
        items['publisher'] = 'Barrons.com'
        items['ticker'] = ticker
        items['url'] = response.url
        items['consensus'] = 'no_consensus'



