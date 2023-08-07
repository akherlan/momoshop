import scrapy
from datetime import datetime, timedelta, timezone
import re


class GaudiSpider(scrapy.Spider):
    name = "gaudi"
    allowed_domains = ["www.gaudi-clothing.com"]
    start_urls = ["https://www.gaudi-clothing.com"]
    brand = "Gaudi"

    def parse(self, response):
        for link in response.css("li > a::attr(href)").extract():
            if re.findall(r"/catalog/\w*", link):
                yield scrapy.Request(
                    url=f"{link}/1",
                    callback=self.parse_item,
                    # meta={"ref": response.url},
                )

    def parse_item(self, response):
        links = response.css(".product .front a::attr(href)").extract()
        if len(links):
            for link in links:
                yield scrapy.Request(
                    url=link,
                    callback=self.parse_product,
                    # meta={"ref": response.url}
                )
            meta_link = response.url.split("/")
            base_link = "/".join(meta_link[:-1])
            next_page = int(meta_link[-1]) + 1
            next_link = f"{base_link}/{str(next_page)}"
            yield scrapy.Request(
                url=next_link,
                callback=self.parse_item,
                # meta={"ref": response.url}
            )

    def parse_product(self, response):
        currency = "IDR"
        link_parse = re.search(r"product/(.+)/(\d+)-.+$", response.url)
        tzinfo = timezone(timedelta(hours=7))
        date_acquisition = datetime.now(tzinfo).replace(microsecond=0).isoformat()
        yield {
            "product_id": link_parse.group(2),
            "sku": response.css(".kode_prod .main_code ::text").get().strip(),
            "name": response.css(".tit_prod ::text").get().strip(),
            "brand": self.brand,
            "category": " ".join(link_parse.group(1).split("-")).title(),
            "variant_id": None,
            "variant_name": list(
                filter(
                    lambda x: x != "",
                    [
                        li.strip().title()
                        for li in response.css(".cont_color_prod li ::text").extract()
                    ],
                )
            ),
            "date_release": None,
            "description": "; ".join(
                list(
                    filter(
                        lambda x: x != "",
                        [
                            i.strip()
                            for i in response.css(".modal-body ::text").extract()
                        ],
                    )
                )
            ),
            "slug": response.url.replace(self.start_urls[0], ""),
            "price": int(
                "".join(re.findall(r"\d", response.css(".price ::text").get()))
            ),
            "is_instock": response.css("[class*=stock] ::text").get().lower() == "in stock",
            "date_acquisition": date_acquisition,
            "source": self.start_urls[0],
            # "ref": response.meta.get("ref"),
        }
