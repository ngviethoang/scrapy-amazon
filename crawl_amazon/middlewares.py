# -*- coding: utf-8 -*-

# Define here the models for your spider middleware
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/spider-middleware.html

from scrapy.http.request import Request
from datetime import datetime
import json
import random
from scrapy.downloadermiddlewares.retry import RetryMiddleware
from urllib import urlencode
from w3lib.http import basic_auth_header
import requests
from settings import PROXIES_LIST_URL, PROXIES_UPDATE_RUNTIME_URL


class CustomDownloaderMiddleware(RetryMiddleware):

    def process_response(self, request, response, spider):
        # if passing captcha fails because of timeout
        # retry the first request
        if 'type' in request.meta \
                and 'first_req' in request.meta \
                and ('retry_times' in request.meta and request.meta['retry_times'] >= 1):
            print('Timeout: {}'.format(request.meta))

            return self.get_first_request(request)

        # if it's a solve captcha response
        if 'type' in request.meta and request.meta['type'] == 'solve_captcha':
            """ Get captcha text to pass """
            try:
                captcha_response = json.loads(response.text)
                # print(captcha_response)

                captcha_text = captcha_response['captcha_text']

                form_data = request.meta['form_data']
                form_data['field-keywords'] = captcha_text

                meta = request.meta
                meta['type'] = 'pass_captcha'

                first_req = request.meta['first_req']

                # Send form to pass captcha
                # only try this once!
                return Request(url='https://www.amazon.com/errors/validateCaptcha?{}'.format(urlencode(form_data)),
                               meta=meta,
                               dont_filter=True,
                               callback=first_req.callback)
            except ValueError:
                return request
        else:
            """ Check captcha: Send request to pass captcha """

            title = response.css('title::text').extract_first()
            # print(title)
            if title is not None and 'Robot Check' in title:
                """ Retry """
                # return self._retry(request=request, reason='captcha', spider=spider)

                # save request meta to pass captcha
                meta = request.meta

                # save first request to middleware
                if 'type' not in request.meta:
                    meta['first_req'] = request
                else:
                    # retry first request after pass captcha fails
                    print('Retry (wrong captcha): {}'.format(request.meta))

                    return self.get_first_request(request)

                meta['type'] = 'solve_captcha'

                """ Pass Captcha """
                # Get form inputs
                form = response.css('form')

                # Get captcha text from captcha image
                captcha_img_url = form.css('img::attr(src)').extract_first()
                # print(captcha_img_url)

                captcha_data = urlencode({'image_url': captcha_img_url})

                # Get input values from form
                amzn_value = form.css('input[name=amzn]::attr(value)').extract_first()
                amzn_r_value = form.css('input[name=amzn-r]::attr(value)').extract_first()

                form_data = {
                    'amzn': amzn_value,
                    'amzn-r': amzn_r_value,
                }
                # print(form_data)

                meta['form_data'] = form_data

                # Call request to solve captcha
                return Request(url='http://decaptcha.spyamz.com/solve?{}'.format(captcha_data),
                               meta=meta,
                               headers={'Authorization': basic_auth_header('rnd', 'QemtdsvpFfp3t8sd')},
                               dont_filter=True)
            else:
                return response

    def process_exception(self, request, exception, spider):
        pass

    # retry the first request if the captcha solution fails
    # set first request's cookiejar
    def get_first_request(self, request):
        first_req = request.meta['first_req']

        meta = dict()

        if 'data' in first_req.meta:
            meta['data'] = first_req.meta['data']

        meta['cookiejar'] = request.meta['cookiejar']

        if 'url' in request.meta:
            meta['url'] = request.meta['url']

        return Request(url=first_req.url,
                       meta=meta,
                       dont_filter=True,
                       callback=first_req.callback)


# Get proxies from api
def get_proxies(proxies_number=100):
    res = requests.get(url=PROXIES_LIST_URL.format(proxies_number))
    proxies = json.loads(res.content)

    return proxies


class ProxyDownloaderMiddleware(object):

    proxy_requests = 0

    def __init__(self, proxies):
        self.proxies = proxies

    @classmethod
    def from_crawler(cls, crawler):
        proxies = get_proxies()

        return cls(
            proxies=proxies
        )

    def process_request(self, request, spider):
        # Reload proxies list
        # if self.proxy_requests == 100:
        #     spider.logger.info('Reload proxies')
        #
        #     self.proxy_requests = 0
        #     self.proxies = get_proxies()

        # always change proxy
        # select randomly a proxy's index in proxies array
        index = random.randrange(len(self.proxies))

        request.meta['proxy'] = self.proxies[index]  # set proxy
        request.meta['cookiejar'] = index  # save cookies by the proxy's index

        # don't change if it is a decaptcha request
        if 'proxy' in request.meta \
                and 'type' in request.meta and request.meta['type'] in ['solve_captcha']:
            del request.meta['proxy']
        # request with proxy
        else:
            self.proxy_requests += 1

        if 'DEV_MODE' in spider.custom_settings and spider.custom_settings['DEV_MODE']:
            spider.logger.info('Request: {} | meta: {}'.format(request.url, request.meta))

        return None


# Calculate proxy's run time and feedback to database
class RuntimeDownloaderMiddleware(object):
    start_time = None

    def process_request(self, request, spider):
        self.start_time = datetime.now()

        return None

    def process_response(self, request, response, spider):
        if 'proxy' in request.meta:
            proxy = request.meta['proxy']

            run_time = datetime.now() - self.start_time
            # print('Proxy {} runtime: {}'.format(proxy, run_time))

            run_time = run_time.seconds

            # update proxy's run time to database
            requests.get(PROXIES_UPDATE_RUNTIME_URL.format(proxy, run_time))

        return response
