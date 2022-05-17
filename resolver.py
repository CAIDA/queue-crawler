import logging
import random
import socket
from collections import namedtuple, defaultdict
from itertools import chain
from time import sleep
from typing import Callable, List

import dns
import socks
from dns import message as dnsmessage
from dns import asyncquery as dnsquery
from dns import rdatatype

from query import Query
from dns_utils import DNSUtils


RR = namedtuple('RR', ['name', 'ttl', 'rclass', 'rtype', 'rdata'])

class RRCollection:
    def __init__(self):
        self._records = defaultdict(set)

    def add(self, rr:RR) -> None:
        self._records[rr.rtype].add(rr)

    def get_rtype_records(self, rtype:str) -> List[RR]:
        if rtype not in self._records:
            return []
        return list(self._records[rtype])

    def records(self) -> List[RR]:
        combined_rr_set = set()
        for s in self._records.values():
            combined_rr_set.update(s)
        return list(combined_rr_set)

    def __repr__(self) -> str:
        return repr(self._records)

class DNSResponse:
    def __init__(self, query:Query,
                       status:str,
                       rcode:str,
                       flags:List[str],
                       authority:RRCollection,
                       answer:RRCollection,
                       additional:RRCollection):
        self.query = query
        self.status = status
        self.rcode = rcode
        self.flags = flags
        self.authority = authority
        self.additional = additional
        self.answer = answer

class Resolver:
    """Resolver makes the actual UDP queries to IPs.
    If UDP socks proxy can be used the Basic Resolver will pick
    a proxy at random to do the DNS query.
    """
    def __init__(self, timeout:int = 0):
        self.timeout = timeout

    @staticmethod
    def clean_response(response_record: List[dns.rrset.RRset]) -> RRCollection:
        response = RRCollection()
        for rrset in response_record:
            for record in rrset.to_text().lower().splitlines():
                record_parts = record.split(maxsplit=4)
                [name, ttl, rclass, rtype, rdata] = record_parts
                if rtype in ['a','aaaa','ns']:
                    name = DNSUtils.normalize_domain(name)
                if rtype in ['ns']:
                    rdata = DNSUtils.normalize_domain(rdata)
                response.add(RR(name, ttl, rclass, rtype, rdata))
        return response

    async def query(self, query:Query) -> DNSResponse:
        print(query)
        request = dnsmessage.make_query(query.q, rdatatype.from_text(query.rtype))
        # Disable RD
        request.flags = request.flags & ~dns.flags.RD
        query_succeeded = False
        retry_count = 0
        query_status='TIMEOUT'
        response = dnsmessage.Message()
        while not query_succeeded and retry_count < 3:
            try:
                response = await dnsquery.udp(
                    q=request, where=query.nsr_ip, timeout=self.timeout)
                query_succeeded = True
                query_status = 'SUCCESS'
            except dns.exception.Timeout:
                retry_count += 1
                sleep(2*retry_count)
            except Exception as e:
                raise e

        answer = DNSResponse(
            query=query,
            status=query_status,
            rcode=dns.rcode.to_text(response.rcode()),
            flags=dns.flags.to_text(response.flags).split(" "),
            answer=self.clean_response(response.answer),
            authority=self.clean_response(response.authority),
            additional=self.clean_response(response.additional),
        )
        return answer

