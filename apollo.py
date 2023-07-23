"""
Apollo API Handler
===========

Provides controlled access to the Apollo API

Handles rate limiting and more.
"""
from dataclasses import dataclass
from time import sleep
import warnings
import requests
from requests import Response


@dataclass
class RequestRemaining:
    """Keeps track of Api rate limits"""

    minute: int
    hour: int
    day: int

    def can_request(self) -> bool:
        """Checks if we've hit a limit before requesting"""
        return self.minute > 0 and self.hour > 0 and self.day > 0

    # Number of seconds before you can safely request again
    def next_request(self) -> int:
        """Returns how long to wait after hitting a limit"""
        if self.day == 0:
            return 24 * 60 * 60
        if self.hour == 0:
            return 60 * 60
        if self.minute == 0:
            return 60
        return 0


class ApolloAPI:
    """
    Provides controlled access to Apollo's api.

    :param apikey: Apollo API key to use with requests.
    :type apikey: str

    :ivar apikey: Apollo API key.
    :type apikey: str
    :ivar requests_left: Instance of RequestsRemaining to keep track of rate limits.
    :type requests_left: RequestsRemaining
    """
    apollo_api_url: str = 'https://api.apollo.io/v1/'

    headers = {
        "Content-Type": "application/json",
        "Cache-Control": "no-cache"
    }

    def __init__(self, apikey):
        """
        Constructs instance with API key to manage accessing Apollo's api.

        :param apikey: Apollo API key to use with requests.
        :type apikey: str
        """
        self.apikey: str = apikey
        self.requests_left = RequestRemaining(1, 1, 1)

    # returns true if we've hit the rate limit
    def __is_rate_limit(self) -> bool:
        return not self.requests_left.can_request()

    # keeps track of rate limit using the response headers
    def __set_rate_limit(self, resp):
        if 'x-24-hour-requests-left' in resp.headers:
            self.requests_left.day = int(resp.headers['x-24-hour-requests-left'])
        if 'x-hourly-requests-left' in resp.headers:
            self.requests_left.hour = int(resp.headers['x-hourly-requests-left'])
        if 'x-minute-requests-left' in resp.headers:
            self.requests_left.minute = int(resp.headers['x-minute-requests-left'])

    @staticmethod
    def filter_by_org_ids(people, org_ids):
        """
        Returns only people who currently work at an organization in the list org_id
        :param people:
        :type people: list
        :param org_ids:
        :type org_ids: list
        :return: list of people
        :rtype: list

        Example::

            >>> ApolloAPI.filter_by_org_ids([
            ...     {'name':'bob', 'organization_id': 'asd'},
            ...     {'name':'john', 'organization_id': 'qwe'}, ]
            ... , ['asd','asdf']) == [{'name': 'bob', 'organization_id': 'asd'}]
            True
            >>> ApolloAPI.filter_by_org_ids([
            ...     {'name':'bob', 'organization_id': 'asd'},
            ...     {'name':'john', 'organization_id': 'qwe'}, ]
            ... , [])
            []
            >>> ApolloAPI.filter_by_org_ids([],['asd','asdf'])
            []


        """
        return [person for person in people if person['organization_id'] in org_ids]

    def get_people(self, domains: [str], titles: [str] = None):
        """
        Get a list of people at an org as one of titles.
        In nearly all cases, using the filtered version `getPeopleFiltered` is better. Apollo
        hallucinates a lot.  Pass in multiple domains if org is known by more than one.

        :param domains: domains of org in format list of 'domain.tld'
        :type domains: list
        :param titles: titles to search for such as 'CEO' or 'COO'
        :type titles: list
        :return: list of people
        :rtype: list
        """

        if titles is None:
            titles = ["Chief Executive Officer", ]
        if isinstance(domains, str):
            domains = [domains, ]
        if isinstance(titles, str):
            titles = [titles, ]

        data = {
            "api_key": self.apikey,
            "q_organization_domains": '\n'.join(domains),
            "page": 1,
            "person_titles": titles
        }

        resp = self.__api_post_call(data, 'mixed_people/search')

        return resp.json()['people']

    def get_people_filtered(self, domains: [str], titles: [str] = None):
        """
        Get a list of people currently working at an org as one of titles.

        Pass in multiple domains if org is known by more than one

        :param domains: domains of org in format list of 'domain.tld'
        :type domains: list
        :param titles: titles to search for such as 'CEO' or 'COO'
        :type titles: list
        :return: list of people
        :rtype: list

        Example::

            >>> import os; from dotenv import load_dotenv; load_dotenv()
            True
            >>> api = ApolloAPI(os.getenv('APOLLO_API_KEY'))
            >>> api.get_people_filtered('seclytics.com','CEO') # doctest: +ELLIPSIS
            [{'id': '61151850418eb80001ce98ce'...
            >>> results = api.get_people_filtered(['seclytics.com'], ['CEO', 'Head of Engineering'])
            >>> isinstance(results, list) and len(results) == 2 and all(isinstance(item, dict) for
            ... item in results)  # check is in form [{...},{...}]
            True
            >>> api.get_people_filtered('websitethatdoesnotexistasdf','CEO')
            []
            >>> api.get_people_filtered('seclytics.com','positionthatdoesnotexist')
            []
        """

        # get all org_ids associated with organization
        org_id = [org_id for domain, org_id in self.get_org_ids(domains).items()]

        # get all people related to titles and organization
        ppl = self.get_people(domains, titles)

        # return only people currently working at organization by org id
        return self.filter_by_org_ids(ppl, org_id)

    def get_org_ids(self, domains: [str]) -> {str: str}:
        """
        Get a dictionary of {domain: organization id} from apollo given a list of domains.

        :param domains: List of domains in format 'domain.tld'
        :type domains: list

        :returns: dictionary of {domain: organization id,}, containing only valid organizations.
        :rtype: dict

        Example::

            >>> import os; from dotenv import load_dotenv; load_dotenv()
            True
            >>> api = ApolloAPI(os.getenv('APOLLO_API_KEY'))
            >>> api.get_org_ids('seclytics.com')
            {'seclytics.com': '54a1227269702da10f1e1d03'}
            >>> api.get_org_ids('websitethatdoesnotexistasdf')
            {}
            >>> api.get_org_ids(['seclytics.com', 'google.com']) ==
            ... {'seclytics.com': '54a1227269702da10f1e1d03', 'google.com': '5fc93db64c38d300d6aa24e6'}
            True
        """
        # Check if we can get away with not using bulk_enrich, as bulk_enrich has 10 times the cost
        if isinstance(domains, str):
            endpoint = 'enrich'
            data = {
                "api_key": self.apikey,
                "domain": domains,
            }
        else:
            endpoint = 'bulk_enrich'
            data = {
                "api_key": self.apikey,
                "domains": domains,
            }

        resp = self.__api_post_call(data, 'organizations/' + endpoint)

        # Handle cases of none, one, or multiple organizations being returned
        if resp.json() == {}:
            return {}
        if 'organization' in resp.json():
            return {
                resp.json()['organization']['primary_domain']: resp.json()['organization']['id'],
            }
        return {
            org['primary_domain']: org['id'] for org in resp.json()['organizations']
            if org is not None
        }

    def __api_post_call(self, payload, endpoint: str) -> Response:
        """
        Sends a POST request to Apollo.io's api.

        :param payload: json post data
        :param endpoint: subdirectory to direct post to
        :type endpoint: str
        :returns: Response data from post.
        :rtype: Response
        """
        # Preempt check if we are going to hit a rate limit
        if self.__is_rate_limit():
            warnings.warn(
                'Out of requests, waiting ' + str(self.requests_left.next_request()) + ' sec',
                UserWarning)
            sleep(self.requests_left.next_request())

        # Might have to retry, best to loop until we get a good return or raise an exception
        while True:
            resp = requests.post(self.apollo_api_url + endpoint,
                                 headers=self.headers,
                                 json=payload,
                                 timeout=10
                                 )
            self.__set_rate_limit(resp)

            if resp.status_code == 429:
                warnings.warn(f'Too many requests sent, unexpected 429. Attempting to retry after'
                              f' waiting {self.requests_left.next_request()} sec')
                sleep(self.requests_left.next_request())
                continue
            if resp.status_code == 200:
                return resp
            raise RuntimeError('"organizations/'
                               + endpoint + '" did not return with a status of 200'
                                            '\n' + str(resp.text))
