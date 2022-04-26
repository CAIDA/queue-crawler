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
        return NSRBlock('Root NSR', [NSR('a.root-servers.net.', ['198.41.0.4'])])