from typing import Optional, List
from collections import defaultdict

from host import NamedHost
from nsr import NSR, NSRBlock
from resolver import DNSResponse, RR, RRCollection

class DNSRelationMap:
    def __init__(self):
        self._hosts = {}
        self._host_nsr = defaultdict(set)
        self._records = RRCollection()

    def storeNS(self, rr:RR) -> None:
        self.store(rr)
        self._host_nsr[rr.name].add(rr.rdata)

    def storeA(self, rr:RR) -> None:
        self.store(rr)
        if rr.name not in self._hosts:
            self._hosts[rr.name] = NamedHost(rr.name)
        self._hosts[rr.name].add(rr.rdata)

    def store(self, rr:RR) -> None:
        self._records.add(rr)

    def get_rtype_records(self, rtype:str) -> List[RR]:
        return self._records.get_rtype_records(rtype)

    def hosts_with_nameservers(self) -> List[str]:
        return list(self._host_nsr.keys())

    def getNSRBlock(self, hostname:str) -> Optional[NSRBlock]:
        if hostname not in self._host_nsr:
            return None
        nb  = NSRBlock(hostname)
        for nsr_name in self._host_nsr[hostname]:
            if nsr_name in self._hosts:
                nsr = NSR.from_named_host(self._hosts[nsr_name])
            else:
                nsr = NSR(nsr_name)
            nb.add(nsr)
        return nb

class DNSParser:
    @staticmethod
    def parse_dns_response_A(response: DNSResponse) -> DNSRelationMap:
        auth_nsset = None
        if response.status == 'SUCCESS' and response.rcode == 'NOERROR':
            for rec in response['answer']:
                rec_list = rec.split()
                resp_ns = self.formatDescriptor(rec_list[0])
                if rec_list[3] == 'cname':
                    self.logger.debug(f"CNAME: {rec}")
                    if resp_ns == nameserver:
                        match_ns.append(self.formatDescriptor(rec_list[4]))
                elif rec_list[3] == 'a':
                    self.logger.debug(f"A record: {rec}")
                    if resp_ns in match_ns:
                        auth_nsset['ip'].add(rec_list[4])
                else:
                    self.logger.warning(
                        f"A Query for {nameserver} instead contains answer for {resp_ns}: {rec}")

        return auth_nsset

    def parse_dns_response_NS(response: DNSResponse) -> DNSRelationMap:
        drm = DNSRelationMap()
        for rr in response.answer.get_rtype_records('ns'):
            drm.storeNS(rr)
        for rr in response.authority.get_rtype_records('ns'):
            drm.storeNS(rr)
        for rr in response.additional.get_rtype_records('ns'):
            drm.storeNS(rr)
        
        for rr in response.answer.get_rtype_records('a'):
            drm.storeA(rr)
        for rr in response.authority.get_rtype_records('a'):
            drm.storeA(rr)
        for rr in response.additional.get_rtype_records('a'):
            drm.storeA(rr)
        
        for rr in response.answer.get_rtype_records('soa'):
            drm.store(rr)
        for rr in response.authority.get_rtype_records('soa'):
            drm.store(rr)
        for rr in response.additional.get_rtype_records('soa'):
            drm.store(rr)
        return drm