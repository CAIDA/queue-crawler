from collections import defaultdict

import asyncio

from async_queue import AsyncQueue, AsyncQueueCall
from dns_parser import DNSParser
from dns_utils import DNSUtils
from nsr import NSRBlock
from query import QueryBlock
from query_queue import QueryQueue
from resolution_status import ResolutionStatus

class ResolutionQueue:
    def __init__(self, max_active_resolutions:int, query_queue:QueryQueue):
        self.max_active_resolutions = max_active_resolutions
        self._task = None
        self._running = asyncio.Event()
        self._queue = AsyncQueue()
        self.resolutions = defaultdict(dict)
        self._query_queue = query_queue
        self._queue_shift_tasks = []

    def __repr__(self) -> str:
        status = [f"{s.name}: {str(tuple(self.resolutions[s].keys()))}" for s in ResolutionStatus]
        return "\n".join(status)

    async def __aenter__(self):
        self._task = asyncio.create_task(self._run())
        return self

    async def __aexit__(self, exc_t, exc_v, exc_tb):
        self._running.set()
        await asyncio.gather(*self._queue_shift_tasks)
        await self._task

    def is_running(self):
        return not self._running.is_set()

    def set_resolution_status(self, resolution:"Resolution", status:ResolutionStatus) -> None:
        self.resolutions[resolution.status].pop(resolution.id())
        resolution.status = status
        self.resolutions[resolution.status][resolution.id()] = resolution

    def get_resolution_list(self, status:ResolutionStatus) -> None:
        return [*self.resolutions[status].values()]

    async def _run(self):
        self._running.clear()
        while self.is_running():
            for resolution in self.get_resolution_list(ResolutionStatus.PENDING):
                load_parent_domain_auth_block_task = asyncio.create_task(resolution.load_parent_domain_auth_block(self))
                self._queue_shift_tasks.append(load_parent_domain_auth_block_task)
                self.set_resolution_status(resolution, ResolutionStatus.BLOCKED)
            for resolution in self.get_resolution_list(ResolutionStatus.BLOCKED):
                if resolution.ready_for_querying:
                    self.set_resolution_status(resolution, ResolutionStatus.ACTIVE)
            for resolution in self.get_resolution_list(ResolutionStatus.ACTIVE):
                if resolution.result:
                    self.set_resolution_status(resolution, ResolutionStatus.DONE)
                    resolution.finish()
                else:
                    query_task = asyncio.create_task(resolution.resolve(self))
                    self._queue_shift_tasks.append(query_task)
                    self.set_resolution_status(resolution, ResolutionStatus.QUERYING)
            for resolution in self.get_resolution_list(ResolutionStatus.QUERYING):
                if resolution.done_querying:
                    self.set_resolution_status(resolution, ResolutionStatus.ACTIVE)
            await asyncio.sleep(0)

    async def _wait_for(self, resolution:"Resolution"):
        self.resolutions[resolution.status][resolution.id()] = resolution
        await resolution._done.wait();
        return resolution.result

    async def add(self, resolution:"Resolution"):
        queue_call_id = resolution.id()
        aqc = AsyncQueueCall(queue_call_id, self._wait_for(resolution))
        result = await self._queue.queue_call(aqc)
        return result

    async def is_done(self, resolution:"Resolution") -> bool:
        queue_call_id = resolution.id()
        return self._queue.is_done(queue_call_id)

class Resolution:
    def __init__(self, hostname:str, res_type:str):
        self.hostname = hostname
        self.res_type = res_type
        self.status = ResolutionStatus.PENDING
        self._done = asyncio.Event()
        self.ready_for_querying = False
        self.done_querying = False
        self.parent_domain_auth_block = None
        self.result = None

    def __repr__(self) -> str:
        return f"Resolution{(self.hostname, self.res_type, self.status)}"

    def id(self) -> str:
        return f'{self.hostname}/{self.res_type}'

    def finish(self) -> None:
        self._done.set()

    async def start(self, queue:ResolutionQueue) -> None:
        raise NotImplementedError


class AuthNSResolution(Resolution):
    def __init__(self, hostname:str):
        super().__init__(hostname, "AuthNS")
        self.auth_parent = None
        self.auth_child = None
        self.query_target_auth_block = None

    async def load_parent_domain_auth_block(self, queue:ResolutionQueue):
        self.ready_for_querying = False
        if self.hostname == ".":
            self.parent_domain_auth_block = DNSUtils.get_root_nsr_block()
        else:
            parent_domain = DNSUtils.get_parent_domain(self.hostname)
            parent_domain_auth_block_resolution = AuthNSResolution(parent_domain)
            self.parent_domain_auth_block = await queue.add(parent_domain_auth_block_resolution)
        self.query_target_auth_block = self.parent_domain_auth_block
        self.ready_for_querying = True


    async def resolve(self, queue:ResolutionQueue) -> None:
        self.done_querying = False
        if self.hostname == ".":
            self.result = DNSUtils.get_root_nsr_block()
        else:
            print(self.hostname, self.query_target_auth_block)
            query_block = QueryBlock(self.hostname,['NS','A'], self.query_target_auth_block)
            query_response = await queue._query_queue.dispatch_query(query_block)
            query_response = query_response.data['NS']
            if query_response.status == "SUCCESS":
                drm = DNSParser.parse_dns_response_NS(query_response)
                closest_superdomain = DNSUtils.closest_superdomain(self.hostname, drm.hosts_with_nameservers(), True)
                if not self.auth_parent:
                    self.auth_parent = drm.getNSRBlock(closest_superdomain)
                    self.query_target_auth_block = self.auth_parent
                elif not self.auth_child:
                    self.auth_child = drm.getNSRBlock(closest_superdomain)
                    # Replace with merge of parent and child
                    self.result = self.auth_child
        self.done_querying = True
# class Resolution:
#     def __init__(self, hostname:str, res_type:str, res_queue:ResolutionQueue):
#         self.hostname = hostname
#         self.res_type = res_type

#     def id(self) -> str:
#         return f'{self.hostname}/{self.res_type}'

#     async def resolve() -> ResolutionResponse:
#         pass

# class ResolutionResponse:
#     def __init__(self, data=None):
#         self.data = data

# class IPResolution(Resolution):
#     def __init__(self, hostname:str):
#         super().__init__(hostname, "IP")
#         self.hostname = hostname
#         self.queries = []
#         self.current_question = hostname
#         self.current_ns_block = DNSUtils.get_root_nsr_block()

#     def next_query(self) -> QueryBlock:
#         return QueryBlock(self.current_question, ['NS','A'], self.current_ns_block)

# class AuthNSResolution(Resolution):
#     def __init__(self, hostname:str):
#         super().__init__(hostname, "AuthNS")
#         self.hostname = hostname
#         self.parent_domain = DNSUtils.get_parent_domain(hostname)

#     async def resolve() -> AuthNSResolutionResponse:


# class AuthNSResolutionResponse(ResolutionResponse):
#     def __init__(self, nsr_block:NSRBlock):
#         super().__init__(nsr_block)
