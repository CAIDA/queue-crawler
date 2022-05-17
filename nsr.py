from typing import List, Optional

from host import NamedHost, NamedHostSet

class NSR(NamedHost):
    @staticmethod
    def from_named_host(host:NamedHost):
        return NSR(host.hostname, host.ips)

    def __repr__(self) -> str:
        return f"{self.hostname}({', '.join(self.ips)})"

class NSRBlock(NamedHostSet):
    def __init__(self, name:str, nsr_list:Optional[List[NSR]] = None):
        if nsr_list is None:
            nsr_list = []
        super().__init__(name, nsr_list)
        self.nsr_list = nsr_list

    def __repr__(self) -> str:
        nsr_list_rep = [repr(nsr) for nsr in self.nsr_list]
        return f"`{self.name}`-Block[{', '.join(nsr_list_rep)}]"

    def add(self, nsr:NSR) -> None:
        super().add(nsr)
        self.nsr_list.append(nsr)
