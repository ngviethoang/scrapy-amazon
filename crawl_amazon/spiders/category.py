# -*- coding: utf-8 -*-
import scrapy
import json
import utils
from bs4 import BeautifulSoup
from scrapy.selector import Selector
import re
import copy
from datetime import datetime


class CategorySpider(scrapy.Spider):
    name = 'category'

    custom_settings = {
        'LOG_FILE': 'logs/crawl_category.log',
        'ITEM_PIPELINES': {
            'crawl_amazon.pipelines.FilterPipeline': 301,
            'crawl_amazon.pipelines.MongoItemsPipeline': 302
        },

        # Settings for test
        'DEV_MODE': False,
        'MAX_CATEGORIES_PER_PAGE': 1,  # All: None
        'IS_CRAWL_NEXT_PAGE': False,  # Crawl next page in results page
        'ITEMS_PER_RESULT_PAGE': 10,  # Max items in results page
    }

    custom_stats = {
        'items_total': 0,
        'items_has_variants': 0,
    }

    start_urls = [
        'https://www.amazon.com/s/ref=nb_sb_noss?url=search-alias%3Dbeauty&field-keywords='

        # DEV
        # results page
        # 'https://www.amazon.com/b/ref=dp_csx_lgbl_n_luxury-beauty/ref=s9_acss_bw_cg_lxq4_3a1_w?_encoding=UTF8&node=13214802011&pf_rd_m=ATVPDKIKX0DER&pf_rd_s=merchandised-search-6&pf_rd_r=2H3GSJX74Z645JVWFRE9&pf_rd_t=101&pf_rd_p=0b316630-629c-4c77-a798-a8450ecdb8df&pf_rd_i=7175545011'

        # prime url
        # 'https://www.amazon.com/s/ref=lp_13214802011_nr_p_85_0/135-7490104-0070251?fst=as%3Aoff&rh=n%3A3760911%2Cn%3A%2112880941%2Cn%3A%217627928011%2Cn%3A13214802011%2Cp_85%3A2470955011&bbn=13214802011&ie=UTF8&qid=1531967892&rnid=2470954011'

        # items
        # has twis
        # 'https://www.amazon.com/Moisturizer-D%C3%A9collet%C3%A9-Anti-Aging-Wrinkles-Circles/dp/B0166W9GMI/ref=lp_11060711_1_9_s_it?s=beauty&ie=UTF8&qid=1531987533&sr=8-9&th=1',
        # 'https://www.amazon.com/Brickell-Mens-Purifying-Charcoal-Face/dp/B00NQ7G62A/ref=lp_6706811011_1_11_s_it?s=beauty&ie=UTF8&qid=1531984707&sr=1-11&th=1',
        # no twis
        # 'https://www.amazon.com/ELEMIS-Pro-Collagen-Eye-Renewal-Anti-Wrinkle/dp/B000J10II4/ref=lp_13214802011_1_5_s_it?s=beauty&ie=UTF8&qid=1531966939&sr=1-5',
        # has feature & videos
        # 'https://www.amazon.com/Gillette-Fusion-Manual-Refills-Razors/dp/B004B8AZH0/ref=lp_3778591_1_1_s_it?s=beauty&ie=UTF8&qid=1531975201&sr=1-1'
    ]

    def __init__(self, *args, **kwargs):
        super(CategorySpider, self).__init__(**kwargs)
        self.start = datetime.now()

    def start_requests(self):
        for start_url in self.start_urls:
            yield scrapy.Request(url=start_url, callback=self.parse_category)

    def parse(self, response):
        pass

    # Parse category page
    # Get sub category pages or results page
    def parse_category(self, response):
        main_category = response.css('#searchDropdownBox option[selected]::text').extract_first()
        # print(main_category)

        categories = response.css('a.bxc-grid-overlay__link')

        if len(categories) == 0:
            print('Category page 0 categories {}'.format(response.url))

        for i, category in enumerate(categories):
            if self.custom_settings['DEV_MODE'] \
                    and self.custom_settings['MAX_CATEGORIES_PER_PAGE'] is not None \
                    and i == self.custom_settings['MAX_CATEGORIES_PER_PAGE']:
                break

            category_name = ''.join(category.css('a ::text').extract())
            category_name = category_name.strip()

            category_url = category.css('a::attr(href)').extract_first()
            category_url = response.urljoin(category_url)
            # print('Category {} {}'.format(category_name, category_url))

            meta = copy.deepcopy(response.meta)

            # First request
            if 'data' not in meta or 'categories' not in meta['data']:
                meta['data'] = {
                    'categories': [main_category, category_name]
                }

                yield scrapy.Request(url=category_url,
                                     meta=meta,
                                     callback=self.parse_category)
            # Second request
            else:
                meta['data']['categories'].append(category_name)

                yield scrapy.Request(url=category_url,
                                     meta=meta,
                                     callback=self.parse_prime_url)

    # Get url for prime items only from sub category's results page
    def parse_prime_url(self, response):
        prime_input = response.xpath(
            "//div[@id='leftNav']//i[contains(@class, 'a-icon-prime')]//..//..//..//../@data-s-ref-selected") \
            .extract_first()

        prime_url = json.loads(prime_input)['url']
        prime_url = prime_url.replace('amp;', '')
        prime_url = response.urljoin(prime_url)
        # print(prime_url)

        yield scrapy.Request(url=prime_url,
                             meta=response.meta,
                             callback=self.parse_results_page)

    # Parse results page to get items' urls
    def parse_results_page(self, response):
        url = response.url

        item_urls = response.css('#resultsCol .s-access-detail-page::attr(href)').extract()
        print('{} items {}'.format(len(item_urls), url))

        self.custom_stats['items_total'] += len(item_urls)

        # Crawl next page
        if self.custom_settings['DEV_MODE'] \
                and self.custom_settings['IS_CRAWL_NEXT_PAGE']:
            next_page = response.css('#pagnNextLink::attr(href)').extract_first()
            if next_page is not None:
                next_page = response.urljoin(next_page)

                yield scrapy.Request(url=next_page,
                                     meta=response.meta,
                                     callback=self.parse_results_page)

        # Crawl all items in a page
        for i, item_url in enumerate(item_urls):
            if self.custom_settings['DEV_MODE'] \
                    and self.custom_settings['ITEMS_PER_RESULT_PAGE'] is not None \
                    and i == self.custom_settings['ITEMS_PER_RESULT_PAGE']:
                break

            item_url = response.urljoin(item_url)

            yield scrapy.Request(url=item_url,
                                 meta=response.meta,
                                 callback=self.parse_item_page)

    # Parse item page
    def parse_item_page(self, response):
        # Get the first item
        item = self.get_item(response)

        parent_id = item['ASIN']

        item['variants'] = []

        # Add item's category info
        item['categories'] = response.meta['data']['categories']
        # item['categories'] = None

        # parse twister to get its variants
        variant_urls = response.css('#twister_feature_div .twisterShelf_swatch::attr(data-dp-url)').extract()

        item['variants_len'] = len(variant_urls) if len(variant_urls) > 0 else 1

        # Create new item (only insert few keys)
        new_item = {k: item[k] if k in item else None
                    for k in ['url', 'variants', 'variants_len', 'ASIN', 'brand', 'categories']}

        yield new_item

        # Update this item as its variant
        variant_item = copy.deepcopy(item)

        if len(variant_urls) > 0:
            variant_item['options'] = self.get_variant_options(response)

        del variant_item['variants']
        del variant_item['variants_len']
        del variant_item['brand']
        del variant_item['categories']

        variant_item['update_variant'] = True
        variant_item['parent_id'] = parent_id

        yield variant_item

        # if the item has variants
        if len(variant_urls) > 0:
            self.custom_stats['items_has_variants'] += 1

            meta = copy.deepcopy(response.meta)

            if 'data' not in meta:
                meta['data'] = dict()
            meta['data']['parent_id'] = parent_id

            for variant_url in variant_urls:
                # print('Variant {}'.format(twister_item_url))

                # Update other variant for item
                if variant_url != '':
                    variant_url = 'https://www.amazon.com' + variant_url

                    yield scrapy.Request(url=variant_url,
                                         meta=meta,
                                         callback=self.parse_variant)

    def parse_variant(self, response):
        item = self.get_item(response)
        item['options'] = self.get_variant_options(response)

        del item['brand']

        item['update_variant'] = True
        item['parent_id'] = response.meta['data']['parent_id']

        yield item

    def get_variant_options(self, response):
        twister_data = response.xpath(
            """//script[contains(., "P.register('twister-js-init-dpx-data'")]/text()""")\
            .extract_first()

        if twister_data is not None:
            # Get JS object
            data = twister_data.split('dataToReturn')[1]
            data = re.sub('\n|=|;|return', '', data)
            data = re.sub(',(\s+)?]', ']', data)  # js array error
            data = json.loads(data)
            # print(data)

            # Get variant's values
            variation_display_labels = data['variationDisplayLabels']
            selected_variation_values = data['selectedVariationValues']
            variation_values = data['variationValues']

            # options: [{label: value}, ...]
            options = [{variation_display_labels[k]: variation_values[k][selected_variation_values[k]]}
                       for k in selected_variation_values]

            return options

        return None

    # Get item details from response
    def get_item(self, response):
        title = response.css('#productTitle::text').extract_first()
        title = title.strip() if title is not None else None
        # print(title)

        details_output = dict()
        ASIN = None

        details = response.css('#detail-bullets .content > ul > li')

        for detail in details:
            detail_name = detail.css('b::text').extract_first()
            detail_name = detail_name.replace(':', '').strip()

            detail = BeautifulSoup(detail.extract(), 'lxml')

            # Remove detail name's tag in each detail
            for span in detail.find_all('b'):
                span.extract()
            detail = Selector(text=str(detail))

            detail_values = detail.css('li ::text').extract()
            detail_values = utils.normalize_str_array(detail_values)

            detail_value = detail_values[0] if len(detail_values) > 0 else None

            # Parse ranks number
            if 'Amazon Best Sellers Rank' in detail_name:
                detail_value = detail_value.strip().split(' ')[0]
                detail_value = utils.parse_int(detail_value)

            if 'ASIN' in detail_name:
                ASIN = detail_value

            details_output[detail_name] = detail_value

        alt_images = response.css('#altImages img::attr(src)').extract()
        # print(alt_images)

        brand = response.css('#bylineInfo::text').extract_first()
        # print(brand)

        brand_url = response.css('#bylineInfo::attr(href)').extract_first()
        brand_url = response.urljoin(brand_url) if brand_url is not None else None
        # print(brand_url)

        price = response.css('.snsPriceBlock .a-color-price::text').extract_first()
        if price is None:
            price = response.css('#priceblock_ourprice::text').extract_first()

        price = price.strip() if price is not None else None
        # print(price)

        description = response.css('#productDescription p::text, #productDescription h3::text').extract()
        description = utils.normalize_str_array(description)
        # description = '\n'.join(description)
        # print(description)

        plus_desc = response.css('#aplus')
        plus_desc_html = plus_desc.css('.aplus-v2').extract_first()

        plus_desc_texts = plus_desc.css('*:not(script):not(style)::text').extract()
        plus_desc_texts = utils.normalize_str_array(plus_desc_texts)
        plus_desc_text = '\n'.join(plus_desc_texts)

        features = response.css('#feature-bullets ul li ::text').extract()
        features = [feature.strip() for feature in features]
        # print(features)

        videos = response.css('#vse-rel-videos-carousel .vse-video-item::attr(data-video-url)').extract()
        # print(videos)

        return {
            'ASIN': ASIN,
            'url': response.url,
            'title': title,
            'brand': {
                'name': brand,
                'url': brand_url
            },
            'alt_images': alt_images,
            'details': details_output,
            'price': price,
            'description': description,
            'plus_description': {
                'text': plus_desc_text,
                'html': plus_desc_html
            },
            'features': features,
            'videos': videos,
        }

    def close(self, spider, reason):
        runtime = datetime.now() - self.start
        print('Total runtime: {}'.format(runtime))

        print(self.custom_stats)
