from typing import List

import asyncio


from async_queue import AsyncQueue, AsyncQueueCall
from query import QueryBlock, Query
from resolver import Resolver, DNSResponse

class QueryQueueResponse:
    def __init__(self, query_block:QueryBlock, dns_response_list:List[DNSResponse]):
        self.query_block = query_block
        self.data = {}
        for dns_response in dns_response_list:
            self.data[dns_response.query.rtype] = dns_response



class QueryQueue:
    def __init__(self, resolver:Resolver):
        self._query_block_queue = AsyncQueue()
        self._query_queue = AsyncQueue()
        self._resolver = resolver

    async def _query(self, query_list:List[Query]) -> List[DNSResponse]:
        query_coro_list = []
        for query in query_list:
            aqc = AsyncQueueCall(query.id(), self._resolver.query(query))
            query_coro_list.append(self._query_queue.queue_call(aqc))
        dns_response_list = await asyncio.gather(*query_coro_list)
        return dns_response_list


    async def dispatch_query(self, query_block:QueryBlock) -> QueryQueueResponse:
        queue_call_id = query_block.id()
        query_list = query_block.to_query_list()
        aqc = AsyncQueueCall(queue_call_id, self._query(query_list))
        dns_response_list = await self._query_block_queue.queue_call(aqc)
        query_response = QueryQueueResponse(query_block, dns_response_list)
        return query_response