import sqlite3
import requests
from lxml import html
from contextlib import closing
from dotenv import load_dotenv
import os
from time import sleep
from apollo import ApolloAPI

RefreshMSPdb: bool = False

# today's todos: filter by currently working there on org id, add rate limit handling

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
    # Get all MSP
    db.execute("SELECT * FROM MSPs")
    rows = db.fetchall()

    for i, (orgid, url) in enumerate([(row[0], row[2]) for row in rows]):
        if i == 5: break
        # try:
        #     print(api.getOrgIDs(url))
        # except:
        #     print('failed')
        # print('index: ' + str(i) + '\nUrl: ' + url)

        try:
            people = api.getPeopleFiltered(url)
        except RuntimeError:
            continue

        data = [(orgid, person['first_name'], person['last_name'], person['email'],
                 person['phone_numbers'][0]['sanitized_number']) for person in people]

        db.executemany('''insert into VIPs (msp_id, firstName, lastName, email, phoneNumber) values ( ?, ?, ?, ?, ?)''',
                       data)

        length = len(people)
        print(length)

    # db.execute("SELECT COUNT(*) FROM MSPs")
    # for i in range(0,db.fetchone()[0]):


def main():
    # Get secrets
    load_dotenv()

    # Spin up db connection, and auto close on scope exit
    with closing(sqlite3.connect('MSP.db')) as connection:
        with closing(connection.cursor()) as cursor:

            # Save top 500 MSPs to MSPs table in db
            if RefreshMSPdb:
                getMSPs(cursor)
            else:
                print('Skipping db update...')

            getCEOs(cursor)

            # cursor.execute('SELECT url FROM MSPs LIMIT 1')

            # Request CEO info from apollo about first company

            # print(name)

        # Persist changes to db
        connection.commit()


if __name__ == '__main__':
    main()
