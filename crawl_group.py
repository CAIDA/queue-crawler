class CrawlGroup:
    '''Limits the number of domains resolved simultaneously'''
    def __init__(self, size:int):
        self.size = size
        self.domains = []

    def full(self) -> bool:
        return len(self.domains) == self.size 

    def add(self, domain:str) -> bool:
        if self.full():
            return False
        self.domains.append(domain)
        return True

    def __repr__(self) -> str:
        return f"CrawlGroup({', '.join(self.domains)})"