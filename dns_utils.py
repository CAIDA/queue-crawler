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
