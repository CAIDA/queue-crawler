import argparse
from typing import List

import asyncio

from dns_utils import DNSUtils
from manager import Manager

class QueueCrawler:
    def __init__(self, crawl_manager:Manager):
        self.crawl_manager = crawl_manager

    async def run_domains_from_file(self, filename:str) -> None:
        domain_list = self._get_domains_from_file(filename)
        await self.crawl_manager.crawl_domain_list(domain_list)

    def _get_domains_from_file(self, filename:str) -> List[str]:
        domain_list: List[str] = []
        with open(filename, "r") as infile:
            for row in infile:
                domain = DNSUtils.normalize_domain(row)
                domain_list.append(domain)
        return domain_list


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--domain-list', required=True)
    parser.add_argument('--workers', type=int, default=1)
    args = parser.parse_args()
    crawler = QueueCrawler(crawl_manager=Manager(crawl_group_size=args.workers))
    asyncio.run(crawler.run_domains_from_file(filename=args.domain_list))