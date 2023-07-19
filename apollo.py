import requests
from dataclasses import dataclass
from time import sleep


@dataclass
class RequestRemaining:
    minute: int
    hour: int
    day: int

    def canrequest(self) -> bool:
        return self.minute > 0 and self.hour > 0 and self.day > 0

    # Number of seconds before you can safely request again
    def nextrequest(self) -> int:
        if self.day == 0:
            return 24 * 60 * 60
        if self.hour == 0:
            return 60 * 60
        if self.minute == 0:
            return 60


class ApolloAPI:
    ApolloAPIUrl: str = 'https://api.apollo.io/v1/'

    def __init__(self, APIKEY):
        # Instance variables
        self.APIKEY: str = APIKEY
        self.requestsLeft = RequestRemaining(1, 1, 1)

    # returns true if we've hit the rate limit
    def __isratelimit(self) -> bool:
        return not self.requestsLeft.canrequest()

    # keeps track of rate limit using the response headers
    def __setratelimit(self, resp):
        self.requestsLeft.day = int(resp.headers['x-24-hour-requests-left'])
        self.requestsLeft.hour = int(resp.headers['x-hourly-requests-left'])
        self.requestsLeft.minute = int(resp.headers['x-minute-requests-left'])

    # Takes a list of people and company ids and filters the people by if they are currently working at that org
    def filterbyorgid(self, people, orgid):
        return [person for person in people if person['organization_id'] in orgid]

    # Takes a singular or list of domains and titles, returns a list of people
    def getPeople(self, domains: [str], titles: [str] = None):

        if self.__isratelimit():
            print('WARNING: Out of requests, waiting ' + str(self.requestsLeft.nextrequest()) + ' sec')
            sleep(self.requestsLeft.nextrequest())

        if titles is None:
            titles = ["Chief Executive Officer", ]
        if isinstance(domains, str):
            domains = [domains, ]
        if isinstance(titles, str):
            titles = [titles, ]

        headers = {
            "Content-Type": "application/json",
            "Cache-Control": "no-cache"
        }
        data = {
            "api_key": self.APIKEY,
            "q_organization_domains": '\n'.join(domains),
            "page": 1,
            "person_titles": titles
        }

        resp = requests.post(self.ApolloAPIUrl + 'mixed_people/search',
                             headers=headers,
                             json=data
                             )
        self.__setratelimit(resp)
        if len(resp.json()['people']) == 0:
            raise RuntimeError
        return resp.json()['people']

    # Takes a singular or list of domains and titles, returns a list of people, additionally filtered for currency
    def getPeopleFiltered(self, domains: [str], titles: [str] = None):
        dorgs = self.getOrgIDs(domains)
        orgids = [orgid for dom, orgid in dorgs.items()]
        ppl = self.getPeople(domains, titles)

        return self.filterbyorgid(ppl, orgids)

    # Takes a domain and returns {domain: id,}
    def __getOrgID(self, domain: str) -> [(str, str)]:
        if self.__isratelimit():
            print('WARNING: Out of requests, waiting ' + str(self.requestsLeft.nextrequest()) + ' sec')
            sleep(self.requestsLeft.nextrequest())

        headers = {
            "Content-Type": "application/json",
            "Cache-Control": "no-cache"
        }
        data = {
            "api_key": self.APIKEY,
            "domain": domain,
        }

        resp = requests.post(self.ApolloAPIUrl + 'organizations/enrich',
                             headers=headers,
                             json=data
                             )
        self.__setratelimit(resp)
        if len(resp.json()) == 0:
            raise RuntimeError
        test = resp.json()
        return {resp.json()['organization']['name']: resp.json()['organization']['id'], }

    # Takes a list of domains and returns a dictionary of {domain: id,}
    def getOrgIDs(self, domains: [str]) -> [(str, str)]:
        if isinstance(domains, str):
            return self.__getOrgID(domains)

        if self.__isratelimit():
            print('WARNING: Out of requests, waiting ' + str(self.requestsLeft.nextrequest()) + ' sec')
            sleep(self.requestsLeft.nextrequest())

        headers = {
            "Content-Type": "application/json",
            "Cache-Control": "no-cache"
        }
        data = {
            "api_key": self.APIKEY,
            "domains": domains,
        }

        resp = requests.post(self.ApolloAPIUrl + 'organizations/bulk_enrich',
                             headers=headers,
                             json=data
                             )
        self.__setratelimit(resp)

        return {org['name']: org['id'] for org in resp.json()['organizations']}
