# -*- coding: utf-8 -*-
import scrapy
import json


class CategorySpider(scrapy.Spider):
    name = 'category'

    start_urls = [
        # 'https://www.amazon.com/s/ref=nb_sb_noss?url=search-alias%3Dbeauty&field-keywords='

        'https://www.amazon.com/b/ref=dp_csx_lgbl_n_luxury-beauty/ref=s9_acss_bw_cg_lxq4_3a1_w?_encoding=UTF8&node=13214802011&pf_rd_m=ATVPDKIKX0DER&pf_rd_s=merchandised-search-6&pf_rd_r=2H3GSJX74Z645JVWFRE9&pf_rd_t=101&pf_rd_p=0b316630-629c-4c77-a798-a8450ecdb8df&pf_rd_i=7175545011'
    ]

    custom_settings = {
        # 'LOG_FILE': 'logs/crawl_category.log',
        'ITEM_PIPELINES': {
            # 'test_amazon.pipelines.MongoItemsPipeline': 302
        }
    }

    def start_requests(self):
        for start_url in self.start_urls:
            yield scrapy.Request(url=start_url, callback=self.parse_prime_url)

    def parse(self, response):
        pass

    def parse_category(self, response):
        main_category = response.css('#searchDropdownBox option[selected]::text').extract_first()

        categories = response.css('a.bxc-grid-overlay__link')

        if len(categories) == 0:
            print('0 items {}'.format(response.url))

            # categories = response.css('.bxc-grid__row:nth-child(4) a')
            #
            # print('0 to {}'.format(len(categories)))

        for category in categories:
            name = ''.join(category.css('a ::text').extract())
            name = name.strip()

            url = category.css('a::attr(href)').extract_first()
            url = response.urljoin(url)

            print('Category {} {}'.format(name, url))

            if 'main_category' not in response.meta:
                yield scrapy.Request(url=url,
                                     meta={'main_category': main_category, 'category': name},
                                     callback=self.parse_category)
            else:
                p_category = response.meta['category']

                # yield {
                #     'main_category': main_category,
                #     'name': '{} : {}'.format(p_category, name),
                #     'url': url,
                #     'mongo_collection': 'categories'
                # }

    def parse_prime_url(self, response):
        prime_input = response.xpath(
            "//div[@id='leftNav']//i[contains(@class, 'a-icon-prime')]//..//..//..//../@data-s-ref-selected").extract_first()

        prime_url = json.loads(prime_input)['url']

        print(prime_url)

    # def parse_results_page(self, response):

