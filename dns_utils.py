from typing import List, Optional
from nsr import NSR, NSRBlock

class DNSUtils:
    @staticmethod
    def normalize_domain(domain:str) -> str:
        domain = domain.lower().strip()
        if domain[-1] != ".":
            domain += "."
        return domain

    @staticmethod
    def get_root_nsr_block() -> NSRBlock:
        return NSRBlock('.', [NSR('a.root-servers.net.', ['198.41.0.4'])])

    @staticmethod
    def get_parent_domain(domain:str) -> str:
        label_list = [l for l in domain.split(".") if len(l) > 0]
        return ".".join(label_list[1:]) + "."

    @staticmethod
    def is_superdomain(domain:str, superdomain:str, include_identical=False) -> bool:
        domain = DNSUtils.normalize_domain(domain)
        superdomain = DNSUtils.normalize_domain(superdomain)
        if superdomain == "." or include_identical and domain == superdomain:
            return True
        else:
            return domain.endswith("."+superdomain)

    @staticmethod
    def closest_superdomain(domain:str, superdomain_list:List[str], include_identical=False) -> Optional[str]:
        superdomain_list = [(len(s), s) for s in superdomain_list if DNSUtils.is_superdomain(domain, s, include_identical)]
        if len(superdomain_list) == 0:
            return None
        return sorted(superdomain_list, reverse=True)[0][1]