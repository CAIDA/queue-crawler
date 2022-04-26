import logging
import random
import socket
from itertools import chain
from time import sleep
from typing import Callable, List

import dns
import socks
from dns import message as dnsmessage
from dns import query as dnsquery
from dns import rdatatype

from query import Query


class Resolver:
    """Resolver makes the actual UDP queries to IPs.
    If UDP socks proxy can be used the Basic Resolver will pick
    a proxy at random to do the DNS query.
    """

    @staticmethod
    def clean_response(response_record: List[dns.rrset.RRset]):
        return list(chain.from_iterable(map(lambda x: x.to_text().lower().splitlines(), response_record)))

    def query(self, query:Query) -> dict:

        """[summary]

        Args:
            domain (str): domain to be queried
            nameserver_ip (str): IP address of nameserver
            record (str): type of record to be queried

        Returns:
            dict: dict containing parsed DNS response
        """

        domain = query.q
        nameserver_ip = query.nsr_ip
        record = query.rtype
        request = dnsmessage.make_query(domain, rdatatype.from_text(record))
        # Disable RD
        request.flags = request.flags & ~dns.flags.RD
        # Ratelimiter code should go in dt_resolver.py
        answer = {}
        answer['domain'] = domain
        answer['nameserver_ip'] = nameserver_ip
        answer['status'] = 'TIMEOUT'
        response = dnsmessage.Message()
        retry = True
        retry_cnt = 0
        """
        Retry query for three times
        """
        while retry and retry_cnt < 3:
            try:
                response = dnsquery.udp(
                    q=request, where=nameserver_ip, timeout=self.timeout)
                retry = False
                answer['status'] = 'SUCCESS'
            except dns.exception.Timeout:
                retry_cnt += 1
                self.logger.info(
                    f"Query sent to {nameserver_ip} timed out {retry_cnt} times")
                sleep(2*retry_cnt)
            except Exception as e:
                self.logger.error(f"Error querying {nameserver_ip} for {domain}")
                raise e
        answer['rcode'] = dns.rcode.to_text(response.rcode())
        answer['answer'] = self.clean_response(response.answer)
        answer['additional'] = self.clean_response(response.additional)
        answer['authority'] = self.clean_response(response.authority)
        answer['flags'] = dns.flags.to_text(response.flags).split(" ")
        return answer
