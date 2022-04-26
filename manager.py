from typing import List

from crawl_group import CrawlGroup
from resolution import Resolution

class Manager:
    def __init__(self, crawl_group_size:int):
        self.crawl_group_size  = crawl_group_size

    async def crawl_domain_list(self, domain_list:List[str]) -> None:
        resolution_list = []
        for domain in domain_list:
            resolution_list.append(Resolution(domain))
        print(resolution_list[0].next_query().to_query_list())