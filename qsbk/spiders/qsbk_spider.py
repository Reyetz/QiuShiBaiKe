# -*- coding: utf-8 -*-
import scrapy
from qsbk.items import QsbkItem

class QsbkSpiderSpider(scrapy.Spider):
    name = 'qsbk_spider'
    allowed_domains = ['qiushibaike.com']
    start_urls = ['https://www.qiushibaike.com/text/page/1/']
    base_domain = 'https://www.qiushibaike.com'

    # 解析详情页
    def parse_content(self, response):
        author = response.xpath('.//div[@class="detail-col0"]/a/img/@alt').get()
        stats_vote = response.xpath('.//span[@class="stats-vote"]/i/text()').get()
        content_page = response.xpath('.//div[@class="col1 new-style-col1"]')
        contents = content_page.xpath('.//div[@class="content"]/text()').getall()
        content = "".join(contents).strip().replace('\"', ' ')
        item = QsbkItem(author=author, stats_vote=stats_vote, content=content)
        yield item

    def parse(self, response):
        content_letf = response.xpath('//div[@id="content-left"]/div')
        for duanzidiv in content_letf:
            # 判断是否存在“阅读全文”按钮
            content_for_all = duanzidiv.xpath('.//span[@class="contentForAll"]')
            if content_for_all:
                content_url = duanzidiv.xpath('.//a[@class="contentHerf"]/@href').get()
                # 请求详情页，获得完整的段子
                yield scrapy.Request(self.base_domain+content_url, callback=self.parse_content)
                continue
            # 作者
            author = duanzidiv.xpath('.//h2/text()').get().strip()
            # 好笑值
            stats_vote = duanzidiv.xpath('.//span[@class="stats-vote"]/i/text()').get()
            # 内容
            contents = duanzidiv.xpath('.//div[@class="content"]//span/text()').getall()
            content = "".join(contents).strip().replace('\"', ' ')
            item = QsbkItem(author=author, stats_vote=stats_vote, content=content)
            yield item
        next_url = response.xpath('//ul[@class="pagination"]/li[last()]/a/@href').get()
        if not next_url:
            return
        else:
            # 请求下一页
            yield scrapy.Request(self.base_domain+next_url, callback=self.parse)