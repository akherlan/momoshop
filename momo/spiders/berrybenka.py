import scrapy
from urllib.parse import urljoin, urlparse
from datetime import datetime
import pytz
import re


class BerrybenkaSpider(scrapy.Spider):
    name = "berrybenka"
    brand = "Berrybenka"
    start_urls = ["https://berrybenka.com/"]
    navs = []

    local_tz = "Asia/Jakarta"

    def parse(self, response):
        ahref = response.css("a::attr(href)").extract()
        ahref = list(map(lambda a: a.replace("http:", "https:"), ahref))
        links = []
        for link in ahref:
            if re.findall(r"\/(wo)?men\/?$", link) and link not in links:
                links.append(link)
        self.navs.extend(links)
        for link in links:
            yield scrapy.Request(link, callback=self.parse_link)

    def parse_link(self, response):
        page_nums = response.css("div.pagination li a::attr(data-page)").extract()
        page_nums = list(filter(lambda p: re.findall(r"\d{1,}", p), page_nums))
        page_nums = list(filter(lambda p: int(p) > 0, page_nums))
        if str(response.url)[-3:] == "men":
            url = "{}/".format(response.url)
        else:
            url = str(response.url)
        next_pages = list(map(lambda p: urljoin(url, str(int(p) * 48)), page_nums))
        for nav in next_pages:
            if nav not in self.navs:
                self.navs.append(nav)
                yield scrapy.Request(nav, callback=self.parse_link)

        links = list(map(lambda a: a.split("?")[0], response.css("#li-catalog a::attr(href)").extract()))
        for link in links:
            yield scrapy.Request(link, callback=self.parse_item)

    def parse_item(self, response):
        def extract_price(string):
            return string.replace("IDR", "").replace(".", "").strip()
        def extract_link(string):
            return string.split("?")[0]
        def extract_slug(string):
            return urlparse(extract_link(string)).path
        def extract_source(string):
            return urlparse(extract_link(string)).netloc
        def extract_product_id(string):
            return list(filter(lambda x: x.isdigit(), extract_link(string).split("/")))[0]
        local_date_now = pytz.timezone(self.local_tz).localize(datetime.now())
        yield {
            "name": response.css(".prod-title h1::text").get(),
            "price": extract_price(response.css(".price::text").get()),
            "variant_name": response.css("#filter-color label::attr(data-original-title)").extract(), # color
            "description": response.css("#product_description::text").get(),
            "category": response.css(".tag li::text").get(),
            "tag": response.css(".tag li a::text").get(),
            "image": response.css("ul#images-selected li img::attr(src)").extract(),
            "brand": self.brand,
            "slug": extract_slug(str(response.url)),
            "source": extract_source(str(response.url)),
            "product_id": extract_product_id(str(response.url)),
            "referer": response.request.headers.get('referer', None).decode("utf-8"),
            "date_acquisition": local_date_now.strftime("%Y-%m-%dT%H:%M%z"),
        }
