import sqlite3
import requests
from lxml import html
from contextlib import closing
from dotenv import load_dotenv
import os
from time import sleep
from apollo import ApolloAPI
import argparse


# Contains list of top 500 MSPs
MSPsTopUrl: str = 'https://www.crn.com/rankings-and-lists/msp2023.htm'


def MSPsDetailUrl(c: int) -> str:
    return 'https://data.crn.com/2023/detail-handler.php?c={}&r=45'.format(c)


def getMSPs(db):
    # Wipe table and increment counter
    db.execute('''Delete from MSPs''')
    db.execute('''Delete from SQLITE_SEQUENCE where name='MSPs' ''')

    # Parse page containing top 500 MSPs
    tree = html.fromstring(requests.get(MSPsTopUrl).text)

    # Capture key used by page to identify top 500 MSPs in order
    MSPKeys = [MSPKey.split('=')[1] for MSPKey in
               tree.xpath("//div[contains(concat(' ',normalize-space(@class),' '),' data1 ')]/a/@href")]
    print(MSPKeys)

    MSPs = []
    # Iterate through keys to collect information about MSPs
    for key in MSPKeys:
        # Avoid sending too many requests while scraping
        sleep(1)

        # Get info about a MSP from their server, and store the name and url (sans the scheme & subdomain)
        MSP = requests.get(MSPsDetailUrl(key)).json()
        MSPs.append((MSP['Company'], MSP['URL'].removeprefix('https://www.')))

        print(MSP)

    # Insert info about MSPs into our DB
    db.executemany('''insert into MSPs (name, url) values (?, ?)''', MSPs)


def getCEOs(db):
    api = ApolloAPI(os.getenv('APOLLO_API_KEY'))

    # Wipe table and increment counter
    db.execute('''Delete from VIPs''')
    db.execute('''Delete from SQLITE_SEQUENCE where name='VIPs' ''')

    # Get all MSPs, so we can get people data about all of them
    db.execute("SELECT * FROM MSPs")
    rows = db.fetchall()

    for i, (orgid, url) in enumerate([(row[0], row[2]) for row in rows]):

        # only try a few, otherwise the free tier api limits don't last long
        if i == 2:
            break

        # Collect the CEO's info
        try:
            people = api.getPeopleFiltered(url, "Chief Executive Officer")
        except RuntimeError:  # Apollo might not have info to return, so just move on to the next
            continue

        # Parse info and store it in our db
        data = [(orgid, person['first_name'], person['last_name'], person['email'],
                 person['phone_numbers'][0]['sanitized_number']) for person in people]

        db.executemany('''insert into VIPs (msp_id, firstName, lastName, email, phoneNumber) values ( ?, ?, ?, ?, ?)''',
                       data)
        print('Stored Ceo number ' + str(i))


def main():
    # Get secrets
    load_dotenv()

    # Spin up db connection, and auto close on scope exit
    with closing(sqlite3.connect('MSP.db')) as connection:
        with closing(connection.cursor()) as cursor:

            # Save top 500 MSPs to MSPs table in db
            if args.refresh_MSPs or args.refresh:
                getMSPs(cursor)
            else:
                print('Skipping MSP table update...')
            if args.refresh_vip or args.refresh:
                getCEOs(cursor)
            else:
                print('Skipping VIPs table update...')

        # Persist changes to db
        connection.commit()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group()
    group.add_argument('-r', '--refresh', help='wipes entire db and loads in fresh data', action='store_true')
    group.add_argument('-rv', '--refresh_vip', help='wipes only VIPs and loads in fresh data', action='store_true')
    group.add_argument('-rm', '--refresh_MSPs', help='wipes only MSPs and loads in fresh data', action='store_true')
    args = parser.parse_args()
    main()
