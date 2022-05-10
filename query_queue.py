from typing import List

import asyncio


from async_queue import AsyncQueue, AsyncQueueCall
from query import QueryBlock, Query
from resolver import Resolver

class QueryQueue:
    def __init__(self, resolver:Resolver):
        self._query_block_queue = AsyncQueue()
        self._query_queue = AsyncQueue()
        self._resolver = resolver

    async def _query(self, query_list:List[Query]):
        query_coro_list = []
        for query in query_list:
            aqc = AsyncQueueCall(query.id(), self._resolver.query(query))
            query_coro_list.append(self._query_queue.queue_call(aqc))
        query_responses = await asyncio.gather(*query_coro_list)
        return query_responses


    async def dispatch_query(self, query_block:QueryBlock):
        queue_call_id = query_block.id()
        query_list = query_block.to_query_list()
        aqc = AsyncQueueCall(queue_call_id, self._query(query_list))
        result = await self._query_block_queue.queue_call(aqc)
        return result