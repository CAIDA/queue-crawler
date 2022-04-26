from typing import List

class NSR:
    def __init__(self, hostname:str, ips:List[str]):
        self.hostname = hostname
        self.ips = ips

class NSRBlock:
    def __init__(self, name:str, nsr_list:List[NSR]):
        self.name = name
        self.nsr_list = nsr_list