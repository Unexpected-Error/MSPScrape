import sqlite3
import requests
from lxml import html
from contextlib import closing
from dotenv import load_dotenv
import os
from time import sleep


RefreshMSPdb: bool = False

# Contains list of top 500 MSPs
MSPsTopUrl: str = 'https://www.crn.com/rankings-and-lists/msp2023.htm'
def MSPsDetailUrl(c: int) -> str:
    return 'https://data.crn.com/2023/detail-handler.php?c={}&r=45'.format(c)


ApolloAPIUrl: str = 'https://api.apollo.io/v1/'
# ApolloUrl: str = 'https://api.apollo.io/v1/mixed_people/search'


def getMSPs(db):

    # Wipe table and increment counter
    db.execute('''Delete from MSPs''')
    db.execute('''Delete from SQLITE_SEQUENCE where name='MSPs' ''')

    # Parse page containing top 500 MSPs
    tree = html.fromstring(requests.get(MSPsTopUrl).text)

    # Capture key used by page to identify top 500 MSPs in order
    MSPKeys = [MSPKey.split('=')[1] for MSPKey in tree.xpath("//div[contains(concat(' ',normalize-space(@class),' '),' data1 ')]/a/@href")]
    print(MSPKeys)

    MSPs = []
    # Iterate through keys to collect information about MSPs
    for key in MSPKeys:

        # Avoid sending too many requests while scraping
        sleep(1)

        # Get info about a MSP from their server, and store the name and url (sans the scheme & subdomain)
        MSP = requests.get(MSPsDetailUrl(key)).json()
        MSPs.append( (MSP['Company'], MSP['URL'].removeprefix('https://www.')) )

        print(MSP)

    # Insert info about MSPs into our DB
    db.executemany('''insert into MSPs (name, url) values (?, ?)''', MSPs)


def main():
    # Get secrets
    load_dotenv()

    # Spin up db connection, and auto close on scope exit
    with closing(sqlite3.connect('MSP.db')) as connection:
        with closing(connection.cursor()) as cursor:

            # Save top 500 MSPs to MSPs table in db
            if RefreshMSPdb: getMSPs(cursor)
            else: print('Skipping db update...')

            cursor.execute('SELECT url FROM MSPs LIMIT 1')

            # Request CEO info from apollo about first company
            name = requests.post(ApolloAPIUrl + 'mixed_people/search',
                                     headers={
                                         "Content-Type": "application/json",
                                         "Cache-Control": "no-cache"
                                     },
                                     data=
                                     '{' +
                                     '"api_key": "{}", '.format(os.getenv('APOLLO_API_KEY')) +
                                     '"q_organization_domains": "{}", '.format(cursor.fetchone()[0]) +
                                     '"page" : 1, '
                                     '"person_titles" : ["Chief Executive Officer"]'
                                     '}',

                                     ).json()['people'][0]['name']


            print(name)

        # Persist changes to db
        connection.commit()


if __name__ == '__main__':
    main()


'''


domain -> potential people in that role

if one person: 
    good data
if no one:
    null
if >1 person:
    domain -> company id
    filer by company id
    

'''