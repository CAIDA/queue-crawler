import traceback
from typing import List, Optional, Any

from host import NamedHost, NamedHostSet

class NSR(NamedHost):
    @staticmethod
    def from_named_host(host:NamedHost):
        return NSR(host.hostname, host.ips)

    def __repr__(self) -> str:
        return f"{self.hostname}({', '.join(self.ips)})"

    def _key(self) -> str:
        ip_str = "_".join(sorted(self.ips))
        host_str = self.hostname.lower()
        return f"{host_str}/{ip_str}"

    def __hash__(self) -> int:
        return hash(self._key)

    def __eq__(self, other:Any) -> bool:
        if not isinstance(other,NSR):
            return False
        return self._key() == other._key()

class NSRBlock(NamedHostSet):
    def __init__(self, name:str, nsr_list:Optional[List[NSR]] = None):
        if nsr_list is None:
            nsr_list = []
        super().__init__(name, nsr_list)
        self._nsr_data = {}
        for nsr in nsr_list:
            self.add(nsr)


    def __repr__(self) -> str:
        nsr_list_rep = sorted([repr(nsr) for nsr in self.nsr_list])
        return f"`{self.name}`-Block[{', '.join(nsr_list_rep)}]"

    def add(self, nsr:NSR) -> None:
        if nsr._key() not in self._nsr_data:
            super().add(nsr)
            self._nsr_data[nsr._key()] = nsr

    @property
    def nsr_list(self) -> List[NSR]:
        return list(self._nsr_data.values())

    def merge(self, other:'NSRBlock', join_on='outer') -> 'NSRBlock':
        new_block = NSRBlock(self.name)
        current_nsr_name_set = set([nsr.hostname for nsr in self.nsr_list])
        other_nsr_name_set = set([nsr.hostname for nsr in other.nsr_list])
        for nsr in self.nsr_list:
            if join_on == 'outer' or join_on =='left' or nsr.hostname in other_nsr_name_set:
                new_block.add(nsr)
        for nsr in other.nsr_list:
            if join_on == 'outer' or join_on =='right' or nsr.hostname in current_nsr_name_set:
                new_block.add(nsr)
        return new_block