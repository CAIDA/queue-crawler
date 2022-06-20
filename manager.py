from typing import List

import asyncio

from crawl_group import CrawlGroup
from resolution_queue import ResolutionQueue, AuthNSResolution, IPResolution, ShallowIPResolution

class Manager:
    ''' Handles queuing the initial set of resolutions

    Attributes:
    crawl_group_size - Max resolutions to start at a time
    resolution_queue - The instance which will manage individual resolutions
    '''
    def __init__(self, crawl_group_size:int, resolution_queue:ResolutionQueue):
        self.crawl_group_size  = crawl_group_size
        self.resolution_queue = resolution_queue

    async def crawl_domain_list(self, domain_list:List[str]) -> None:
        crawl_group_list = self._create_crawl_groups(domain_list)
        async with self.resolution_queue:
            for crawl_group in crawl_group_list:
                await self._query_crawl_group(crawl_group)

    def _create_crawl_groups(self, domain_list:List[str]) -> List[CrawlGroup]:
        crawl_group_list:List[CrawlGroup] = []
        for domain in domain_list:
            if len(crawl_group_list) == 0 or crawl_group_list[-1].full():
                crawl_group_list.append(CrawlGroup(self.crawl_group_size))
            crawl_group_list[-1].add(domain)
        return crawl_group_list

    async def _query_crawl_group(self, crawl_group:CrawlGroup):
        resolution_list = []
        res_callback = res_callback_generator(crawl_group.domains)
        for domain in crawl_group.domains:
            resolution = IPResolution(domain, callback=res_callback)
            resolution_list.append(self.resolution_queue.add(resolution))
        res_response = await asyncio.gather(*resolution_list)

def res_callback_generator(domain_list):
    counter = {"domain_list":domain_list, "remaining_domains":domain_list, "completed_resolutions":0 }
    def mng_cbk(res):
        counter['completed_resolutions'] +=1 
        print(f"Finished {res.hostname}")
        print(f"{counter['completed_resolutions']}/{len(counter['domain_list'])} resolutions completed")
    return mng_cbk