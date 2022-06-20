from typing import Any, Optional
from collections import defaultdict

import asyncio

from async_queue import AsyncQueue, AsyncQueueCall
from dns_parser import DNSParser
from dns_utils import DNSUtils
from nsr import NSRBlock, NSR
from query import QueryBlock
from query_queue import QueryQueue
from resolution_status import ResolutionStatus
from resolution_response_code import ResolutionResponseCode

class ResolutionResponse:
    '''Returned by the resolution queue when a resolution is run

    Attributes:
    status - Whether the resolution was successful or not
    data - The result of the resolution
    '''
    def __init__(self, status:ResolutionResponseCode, data:Optional[Any] = None):
        self.status = status
        self.data = data

class ResolutionQueue:
    '''Manages all active resolutions

    Attributes:
    max_active_resolutions - Max simultaneous querying resolutions (not implemented)
    _task - Background task managing the queue
    _running - Event which sets when the all queued resolutions are finished
    _queue - AsyncQueue to cache all resolutions
    resolutions - Dictionary sorting all active resolutions by status
    _query_queue - DNS query queue for caching dns queries
    _queue_shift_tasks - List which collects all tasks shifting one resolution 
                         from one status to another
    '''
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

    def set_resolution_status(self, resolution:"Resolution", new_status:ResolutionStatus, old_status:ResolutionStatus) -> None:
        self.resolutions[old_status].pop(resolution.id())
        resolution.status = new_status
        self.resolutions[resolution.status][resolution.id()] = resolution

    def get_resolution_list(self, status:ResolutionStatus) -> None:
        return [*self.resolutions[status].values()]

    async def _run(self):
        self._running.clear()
        while self.is_running():
            for resolution in self.get_resolution_list(ResolutionStatus.PENDING):
                load_query_target_auth_block_task = asyncio.create_task(resolution.load_query_target_auth_block(self))
                self._queue_shift_tasks.append(load_query_target_auth_block_task)
                self.set_resolution_status(resolution, ResolutionStatus.BLOCKED, ResolutionStatus.PENDING)
            for resolution in self.get_resolution_list(ResolutionStatus.BLOCKED):
                if resolution.ready_for_querying:
                    self.set_resolution_status(resolution, ResolutionStatus.ACTIVE, ResolutionStatus.BLOCKED)
                elif resolution.status == ResolutionStatus.DONE:
                    self.set_resolution_status(resolution, ResolutionStatus.DONE, ResolutionStatus.BLOCKED)
                    resolution.finish()
            for resolution in self.get_resolution_list(ResolutionStatus.ACTIVE):
                if resolution.result:
                    self.set_resolution_status(resolution, ResolutionStatus.DONE, ResolutionStatus.ACTIVE)
                    resolution.finish()
                else:
                    query_task = asyncio.create_task(resolution.resolve(self))
                    self._queue_shift_tasks.append(query_task)
                    self.set_resolution_status(resolution, ResolutionStatus.QUERYING, ResolutionStatus.ACTIVE)
            for resolution in self.get_resolution_list(ResolutionStatus.QUERYING):
                if resolution.done_querying:
                    self.set_resolution_status(resolution, ResolutionStatus.ACTIVE, ResolutionStatus.QUERYING)
            await asyncio.sleep(0)

    async def _wait_for(self, resolution:"Resolution") -> ResolutionResponse:
        self.resolutions[resolution.status][resolution.id()] = resolution
        await resolution._done.wait();
        return resolution.result

    async def add(self, resolution:"Resolution") -> ResolutionResponse:
        queue_call_id = resolution.id()
        aqc = AsyncQueueCall(queue_call_id, self._wait_for(resolution))
        result = await self._queue.queue_call(aqc)
        return result

    async def is_done(self, resolution:"Resolution") -> bool:
        queue_call_id = resolution.id()
        return self._queue.is_done(queue_call_id)

class ResolutionTree:
    def __init__(self, tail:'Resolution'):
        self._tail = tail
        self._resolutions = []
        spawned_by = tail.spawned_by
        while spawned_by is not None:
            self._resolutions.append(spawned_by)
            spawned_by = spawned_by.spawned_by
        if len(self._resolutions) > 0:
            self._head = self._resolutions[-1]
        else:
            self._head = self._tail
    
    def __repr__(self) -> str:
        res_list = [f"{res.res_type}({res.hostname})" for res in (list(reversed(self._resolutions)) + [self._tail])]
        return " -> ".join(res_list)

    def contains(self, res:'Resolution') -> bool:
        in_tree = False
        for other in self._resolutions:
            if other == res:
                in_tree = True
                break
        return in_tree

class Resolution:
    '''Base class for resolutions

    Attributes:
    hostname - The hostname being resolved
    res_type - The kind of resolution
    status - The current resolution status
    _done - Event that is set when the resolution is completed
    ready_for_querying - A flag set when the resolution is able to send more quries out
    done_querying - A flag set when the resolution is done sending queries
    query_target_auth_block - The set of nameserver to which the resolution will begin sending
                              queries to
    result -  The final result of the resolution
    callback - A callback function which is triggered when the resolution completes
    spawned_by - The parent resolution which spawned this resolution
    _spawned_resolutions - A dictionary containing all resolutions this resolution has spawned,
                           indexed by resolution id.
    '''
    def __init__(self, hostname:str, res_type:str, spawned_by=None, callback=None):
        self.hostname = hostname
        self.res_type = res_type
        self.status = ResolutionStatus.PENDING
        self._done = asyncio.Event()
        self.ready_for_querying = False
        self.done_querying = False
        self.query_target_auth_block = None
        self.result = None
        self.callback = callback
        self.spawned_by = spawned_by
        self._spawned_resolutions = {}

    def _get_resolution_tree(self) -> ResolutionTree:
        return ResolutionTree(self)

    def _in_resolution_tree(self, other:'Resolution', head_found:Optional[bool]=False):
        if not head_found:
            res_tree = self._get_resolution_tree()
            if res_tree.contains(other):
                return True
            head = res_tree._head
            return head._in_resolution_tree(other, True)
        not_in_resolution_tree = False
        for status, res in self._spawned_resolutions.values():
            if status == "QUEUED":
                if other == res or res._in_resolution_tree(other, True):
                    not_in_resolution_tree = True
                    break
        return not_in_resolution_tree

    def _resolution_branches(self, tab_size=4) -> str:
        curr_res_str = self._key()+"\n"
        child_delimiter = "+" + (tab_size - 2) * "-" + " "
        descendant_delimiter = "|" + (tab_size - 1) * " "
        for status, res in self._spawned_resolutions.values():
            spawned_res_str = res._resolution_branches()
            spawned_res_str_parts = [part for part in spawned_res_str.split("\n") if len(part) > 0]
            curr_res_str += child_delimiter + spawned_res_str_parts[0] + "\n"
            for part in spawned_res_str_parts[1:]:
                curr_res_str += descendant_delimiter + part + "\n"
        return curr_res_str


    async def _queue_resolution(self, other:'Resolution', queue:ResolutionQueue) -> ResolutionResponse:
        if self._in_resolution_tree(other):
            return ResolutionResponse(ResolutionResponseCode.LOOP_DETECTED)
        key = other._key()
        self._spawned_resolutions[key] = ("QUEUED", other)
        response = await queue.add(other)
        self._spawned_resolutions[key] = ("DEQUEUED", other)
        return response

    def _key(self) -> str:
        return f"HOSTNAME:{self.hostname}/TYPE:{self.res_type}"

    def __repr__(self) -> str:
        return f"Resolution{(self.hostname, self.res_type, self.status)}"

    def __eq__(self, other:Any) -> bool:
        if not isinstance(other, Resolution):
            return False
        return self._key() == other._key()

    def id(self) -> str:
        return f'{self.hostname}/{self.res_type}'

    def finish(self) -> None:
        self._done.set()
        if self.callback:
            self.callback(self)

    async def start(self, queue:ResolutionQueue) -> None:
        raise NotImplementedError


class AuthNSResolution(Resolution):
    ''' A comprehensive resolution returning the combined parent-child
    NS set for a given hostname

    Attribute:
    auth_parent -  The parent auth ns block
    auth_child -  The child auth ns block
    '''
    def __init__(self, hostname:str, res_type="AuthNS", *args, **kwargs):
        super().__init__(hostname, res_type,  *args, **kwargs)
        self.auth_parent = None
        self.auth_child = None

    # Load the NS block authoritative for the hostname's parent domain
    async def load_query_target_auth_block(self, queue:ResolutionQueue):
        self.ready_for_querying = False
        if self.hostname == ".":
            parent_domain_auth_block = DNSUtils.get_root_nsr_block()
        else:
            parent_domain = DNSUtils.get_parent_domain(self.hostname)
            parent_domain_auth_block_resolution = AuthNSResolution(parent_domain, spawned_by=self)
            res = await self._queue_resolution(parent_domain_auth_block_resolution, queue)
            if res.status == ResolutionResponseCode.LOOP_DETECTED:
                parent_domain_auth_block_resolution = ShallowAuthNSResolution(parent_domain, spawned_by=self)
                res = await self._queue_resolution(parent_domain_auth_block_resolution, queue)
            if res.status in (ResolutionResponseCode.ERROR, ResolutionResponseCode.LOOP_DETECTED):
                self.result = res
                self.status = ResolutionStatus.DONE
                return 
            parent_domain_auth_block = res.data
        self.query_target_auth_block = parent_domain_auth_block
        self.ready_for_querying = True

    # Reresolve any nameservers missing A records (Most likely cause of the loops and timeouts)
    async def _resolve_cross_zone_nsrs(self, nsr_block:NSRBlock, queue:ResolutionQueue):
        queryable_nsr_list = [nsr for nsr in nsr_block.nsr_list if len(nsr.ips) > 0]
        queryable_nsr_name_set = {nsr.hostname for nsr in queryable_nsr_list}
        missing_ip_nsr_set = {nsr.hostname for nsr in nsr_block.nsr_list if len(nsr.ips) == 0}
        # The same nsr might appear twice in the same block with differing ip counts
        true_missing_ip_nsr_list = list(missing_ip_nsr_set.difference(queryable_nsr_name_set))
        resolved_nsr_list = []
        if len(true_missing_ip_nsr_list) > 0:
            missing_ip_nsr_resolution_list = []
            for nsr_name in true_missing_ip_nsr_list:
                missing_ip_nsr_resolution = ShallowIPResolution(nsr_name, spawned_by=self)
                missing_ip_nsr_resolution_list.append(self._queue_resolution(missing_ip_nsr_resolution, queue))
            missing_ip_nsr_response_list = await asyncio.gather(*missing_ip_nsr_resolution_list)
            for i, res in enumerate(missing_ip_nsr_response_list):
                nsr_name = true_missing_ip_nsr_list[i]
                if res.status == ResolutionResponseCode.SUCCESS and len(res.data) > 0:
                    resolved_nsr = NSR(nsr_name, res.data)
                    resolved_nsr_list.append(resolved_nsr)
        final_nsr_list = queryable_nsr_list + resolved_nsr_list
        return NSRBlock(nsr_block.name, final_nsr_list)
        
    # Resolve the hostname's parent and child auth ns blocks
    async def resolve(self, queue:ResolutionQueue) -> None:
        if self.hostname == ".":
            self.result = ResolutionResponse(ResolutionResponseCode.SUCCESS, DNSUtils.get_root_nsr_block())
        elif len(self.query_target_auth_block.nsr_list) == 0:
            self.result = ResolutionResponse(ResolutionResponseCode.WARNING, NSRBlock(self.hostname))
        else:
            query_block = QueryBlock(self.hostname,['NS','A'], self.query_target_auth_block)
            query_response = await queue._query_queue.dispatch_query(query_block)
            query_response = query_response.data['NS']
            if query_response.status == "SUCCESS":
                if query_response.rcode == "NOERROR":
                    drm = DNSParser.parse_dns_response_NS(query_response)
                    closest_superdomain = DNSUtils.closest_superdomain(self.hostname, drm.hosts_with_nameservers(), True)
                    nsr_block = drm.getNSRBlock(closest_superdomain)
                    if nsr_block is None:
                        if len(drm.get_rtype_records('soa')) > 0:
                            # Empty non-terminal
                            nsr_block = self.query_target_auth_block
                        else:
                            nsr_block = NSRBlock(self.hostname)
                    if not self.auth_parent:
                        merged_nsr_block = nsr_block.merge(self.query_target_auth_block, join_on='left')
                        branch_resolved_nsr_block = await self._resolve_cross_zone_nsrs(merged_nsr_block, queue)
                        self.auth_parent = branch_resolved_nsr_block
                        self.query_target_auth_block = self.auth_parent
                    elif not self.auth_child:
                        branch_resolved_nsr_block = await self._resolve_cross_zone_nsrs(nsr_block, queue)
                        self.auth_child = branch_resolved_nsr_block
                        merged_auth_block = self.auth_parent.merge(self.auth_child)
                        self.result = ResolutionResponse(ResolutionResponseCode.SUCCESS, merged_auth_block)
                else:
                    # NXDOMAIN
                    self.result = ResolutionResponse(ResolutionResponseCode.ERROR)
            else:
                # Query timeout
                self.result = ResolutionResponse(ResolutionResponseCode.WARNING, NSRBlock(self.hostname))
        self.done_querying = True
        

class IPResolution(Resolution):
    '''A comprehensive resolution returning the A records fom querying the 
    parent-child NS set for a given hostname
     '''
    def __init__(self, hostname:str, res_type="IP", *args, **kwargs):
        super().__init__(hostname, res_type, *args, **kwargs)

    async def load_query_target_auth_block(self, queue:ResolutionQueue):
        self.ready_for_querying = False
        auth_block_resolution = AuthNSResolution(self.hostname, spawned_by=self)
        res = await self._queue_resolution(auth_block_resolution, queue)
        if res.status == ResolutionResponseCode.LOOP_DETECTED:
            auth_block_resolution = ShallowAuthNSResolution(self.hostname, spawned_by=self)
            res = await self._queue_resolution(auth_block_resolution, queue)
        if res.status in (ResolutionResponseCode.ERROR, ResolutionResponseCode.LOOP_DETECTED):
            self.result = res
            self.status = ResolutionStatus.DONE
            return 
        self.query_target_auth_block = res.data
        self.ready_for_querying = True

    async def resolve(self, queue:ResolutionQueue) -> None:
        self.done_querying = False
        query_block = QueryBlock(self.hostname,['A'], self.query_target_auth_block)
        query_response = await queue._query_queue.dispatch_query(query_block)
        if 'A' not in query_response.data:
            return ResolutionResponse(ResolutionResponseCode.ERROR)
        query_response = query_response.data['A']
        if query_response.status == "SUCCESS":
            if query_response.rcode == "NOERROR":
                combined_record_list = query_response.answer.get_rtype_records('a') + \
                                       query_response.authority.get_rtype_records('a') + \
                                       query_response.additional.get_rtype_records('a')
                response_set = [rr.rdata for rr in combined_record_list if rr.name == self.hostname]
            self.result = ResolutionResponse(ResolutionResponseCode.SUCCESS, response_set)
        else:
            # Query timeout
            self.result = ResolutionResponse(ResolutionResponseCode.WARNING, [])
        self.done_querying = True

class ShallowAuthNSResolution(AuthNSResolution):
    ''' A non-comprehensive resolution returning the an incomplete NS set 
    for a given hostname
    '''
    def __init__(self, hostname:str, *args, **kwargs):
        super().__init__(hostname, "ShallowAuthNS",  *args, **kwargs)

    async def load_query_target_auth_block(self, queue:ResolutionQueue):
        print(self.res_type, self.hostname)
        self.ready_for_querying = False
        if self.hostname == ".":
            parent_domain_auth_block = DNSUtils.get_root_nsr_block()
        else:
            parent_domain = DNSUtils.get_parent_domain(self.hostname)
            parent_domain_auth_block_resolution = ShallowAuthNSResolution(parent_domain, spawned_by=self)
            res = await self._queue_resolution(parent_domain_auth_block_resolution, queue)
            if res.status in (ResolutionResponseCode.ERROR, ResolutionResponseCode.LOOP_DETECTED):
                self.result = res
                self.status = ResolutionStatus.DONE
                return 
            parent_domain_auth_block = res.data
        self.query_target_auth_block = parent_domain_auth_block
        self.ready_for_querying = True

    async def _resolve_cross_zone_nsrs(self, nsr_block:NSRBlock, queue:ResolutionQueue):
        queryable_nsr_list = [nsr for nsr in nsr_block.nsr_list if len(nsr.ips) > 0]
        if len(queryable_nsr_list) > 0:
            return nsr_block
        return await super()._resolve_cross_zone_nsrs(nsr_block, queue)

    async def resolve(self, queue:ResolutionQueue) -> None:
        return await super().resolve(queue)

class ShallowIPResolution(IPResolution):
    ''' A non-comprehensive resolution returning the an incomplete A record set 
    for a given hostname
    '''
    def __init__(self, hostname:str, res_type="ShallowIP", *args, **kwargs):
        super().__init__(hostname, res_type, *args, **kwargs)

    async def load_query_target_auth_block(self, queue:ResolutionQueue):
        self.ready_for_querying = False
        auth_block_resolution = ShallowAuthNSResolution(self.hostname, spawned_by=self)
        res = await self._queue_resolution(auth_block_resolution, queue)
        if res.status in (ResolutionResponseCode.ERROR, ResolutionResponseCode.LOOP_DETECTED):
            self.result = res
            self.status = ResolutionStatus.DONE
            return 
        self.query_target_auth_block = res.data
        self.ready_for_querying = True