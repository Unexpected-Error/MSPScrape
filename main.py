import sqlite3
import requests
from lxml import html
from contextlib import closing
from dotenv import load_dotenv
import os
from time import sleep


# Contains list of top 500 MSPs
# MSPsTopUrl: str = 'https://www.crn.com/rankings-and-lists/msp2023.htm'
def MSPsDetailUrl(c: int) -> str:
    return 'https://data.crn.com/2023/detail-handler.php?c={}&r=45'.format(c)


ApolloAPIUrl: str = 'https://api.apollo.io/v1/'
# ApolloUrl: str = 'https://api.apollo.io/v1/mixed_people/search'


def getMSPs(db):
    # Wipe table and increment counter
    db.execute('''Delete from MSPs''')
    db.execute('''Delete from SQLITE_SEQUENCE where name='MSPs' ''')

    MSPs = []

    for i in range(1, 501):

        # Avoid spamming server
        sleep(1)

        # Get info about a MSP from their server, and store the name and url (sans the scheme & subdomain)
        MSP = requests.get(MSPsDetailUrl(i)).json()
        MSPs.append( (MSP['Company'], MSP['URL'].removeprefix('https://www.')) )

        # Debug info
        print(MSP)

    # Insert info about MSP into our DB
    db.executemany('''insert into MSPs (name, url) values (?, ?)''', MSPs)


def main():
    # Get secrets
    load_dotenv()

    # Spin up db connection, and auto close on scope exit
    with closing(sqlite3.connect('MSP.db')) as connection:
        with closing(connection.cursor()) as cursor:

            # Save top 500 MSPs to MSPs table in db
            getMSPs(cursor)

            # Print MSPs
            # cursor.execute("SELECT * FROM MSPs")
            # rows = cursor.fetchall()
            # print(rows)

            cursor.execute('SELECT url FROM MSPs LIMIT 1')

            # Request CEO info from apollo about first company
            response = requests.post(ApolloAPIUrl + 'mixed_people/search',
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

                                     ).json()


            print(response)

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