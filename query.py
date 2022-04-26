from typing import List

from nsr import NSRBlock

class Query:
    def __init__(self,  q: str, rtype:str, nsr_ip:str):
        self.q = q
        self.rtype = rtype
        self.nsr_ip = nsr_ip

    def __repr__(self) -> str:
        return f"Query({self.q}, {self.rtype}, {self.nsr_ip})"

    
class QueryBlock:
    def __init__(self,  q: str, rtypes:List[str], nsr_block:NSRBlock):
        self.q = q
        self.rtypes = rtypes
        self.nsr_block = nsr_block

    def to_query_list(self) -> List[Query]:
        query_list:List[Query] = []
        for nsr in self.nsr_block.nsr_list:
            for ip in nsr.ips:
                for rtype in self.rtypes:
                    query_list.append(Query(self.q, rtype, ip))
        return query_list