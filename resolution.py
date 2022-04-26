from dns_utils import DNSUtils
from query import QueryBlock

class Resolution:
    def __init__(self, hostname:str):
        self.hostname = hostname
        self.queries = []
        self.current_question = hostname
        self.current_ns_block = DNSUtils.get_root_nsr_block()

    def next_query(self) -> QueryBlock:
        return QueryBlock(self.current_question, ['NS','A'], self.current_ns_block)
