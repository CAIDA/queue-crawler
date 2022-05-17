from typing import List, Optional

class Host:
    def __init__(self, ips:Optional[List[str]] = None):
        if ips is None:
            ips = []
        self.ips = ips
        
    def add(self, ip:str):
        if ip not in self.ips:
            self.ips.append(ip)


class NamedHost(Host):
    def __init__(self, hostname:str, ips:Optional[List[str]] = None):
        super().__init__(ips)
        self.hostname = hostname

class NamedHostSet:
    def __init__(self, name:str, host_list:List[NamedHost]):
        self.name = name
        self.host_list = host_list

    def add(self, named_host:NamedHost) -> None:
        self.host_list.append(named_host)