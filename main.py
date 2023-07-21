from contextlib import closing
import os
from time import sleep
import warnings
import argparse
import sqlite3
import requests
from lxml import html
from dotenv import load_dotenv
from apollo import ApolloAPI

# List of titles to search
titles = ['Chief Executive Officer', 'CEO',
          'Chief Product Officer', 'CPO',
          'Chief Innovation Officer', 'CIO',
          'VP operations', 'VP of operations', 'VPO',
          'Chief Operating Officer', ' COO',
          'Chief Technology Officer', 'CTO'
          ]


def get_msps(cursor):
    # Wipe table and increment counter
    # noinspection SqlWithoutWhere
    cursor.execute('''Delete from MSPs''')
    cursor.execute('''Delete from SQLITE_SEQUENCE where name='MSPs' ''')

    # Parse page containing top 500 MSPs
    tree = html.fromstring(requests.get('https://www.crn.com/rankings-and-lists/msp2023.htm',
                                        timeout=10).text)

    # Capture key used by page to identify top 500 MSPs in order
    msp_keys = [msp_key.split('=')[1] for msp_key in
                tree.xpath(
                    "//div[contains(concat(' ',normalize-space(@class),' '),' data1 ')]/a/@href")]

    msps = []
    # Iterate through keys to collect information about MSPs
    for key in msp_keys:
        # Avoid sending too many requests while scraping
        sleep(0.6)

        # Get info about a MSP from their server, and store the name and domain.tld
        msp = requests.get(f'https://data.crn.com/2023/detail-handler.php?c={key}&r=45',
                           timeout=10).json()
        msps.append((msp['Company'], msp['URL'].removeprefix('https://www.')))

        print(msp)

    # Insert info about MSPs into our DB
    cursor.executemany('''insert into MSPs (name, url) values (?, ?)''', msps)


def get_vips(cursor):
    api = ApolloAPI(os.getenv('APOLLO_API_KEY'))

    # Wipe table and increment counter
    # noinspection SqlWithoutWhere
    cursor.execute('''Delete from VIPs''')
    cursor.execute('''Delete from SQLITE_SEQUENCE where name='VIPs' ''')

    # Get all MSPs, so we can get people data about all of them
    cursor.execute("SELECT * FROM MSPs")
    rows = cursor.fetchall()

    for i, (msp_id, url) in enumerate([(row[0], row[2]) for row in rows]):

        # only try a few, otherwise the free tier api limits don't last long
        if i == 150:
            break

        # Collect the VIPs's info
        people = api.get_people_filtered(url, titles)

        # Apollo might not have info to return, so just move on to the next
        if len(people) == 0:
            warnings.warn('No VIPs returned for (url, titles) pair: ' + str(url) + str(titles),
                          UserWarning)
            continue

        # Parse info and store it in our db
        data = [(msp_id, person['first_name'], person['last_name'], person['title'], person['email'],
                 person['phone_numbers'][0]['sanitized_number']) for person in people]

        cursor.executemany(
            '''insert into VIPs (msp_id, firstName, lastName, title, email, phoneNumber) '''
            '''values ( ?, ?, ?, ?, ?, ?)''', data)
        print('Stored VIPs, org number ' + str(i))


def main():
    # Get secrets
    load_dotenv()

    # Spin up db connection, and auto close on scope exit
    with closing(sqlite3.connect('MSP.db')) as connection:
        with closing(connection.cursor()) as cursor:

            # Save top 500 MSPs to MSPs table in db
            if args.refresh_msps or args.refresh:
                get_msps(cursor)
            else:
                print('Skipping MSP table update...')
            if args.refresh_vips or args.refresh:
                get_vips(cursor)
            else:
                print('Skipping VIPs table update...')

        # Persist changes to db
        connection.commit()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group()
    group.add_argument('-r', '--refresh', help='wipes entire db and loads in fresh data',
                       action='store_true')
    group.add_argument('-rv', '--refresh_vips', help='wipes only VIPs and loads in fresh data',
                       action='store_true')
    group.add_argument('-rm', '--refresh_msps', help='wipes only MSPs and loads in fresh data',
                       action='store_true')
    args = parser.parse_args()
    main()
