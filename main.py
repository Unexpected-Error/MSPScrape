from contextlib import closing
import os
from time import sleep
import warnings
import argparse
import sqlite3
import csv
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


def get_msps(connection):
    with closing(connection.cursor()) as cursor:
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
        connection.commit()

def get_vips(connection, over_write: bool = False):
    """
    Collects VIPs in database

    :param connection: sqlite3 database connection to MSP.db
    :param over_write: will wipe table and rewrite if true
    """
    with closing(connection.cursor()) as cursor:
        api = ApolloAPI(os.getenv('APOLLO_API_KEY'))

        # Wipe table and increment counter
        if over_write:
            print('Deleting VIPs db in 3...')
            sleep(1)
            print('2...')
            sleep(1)
            print('1...')
            sleep(1)
            # noinspection SqlWithoutWhere
            cursor.execute('''Delete from VIPs''')
            cursor.execute('''Delete from SQLITE_SEQUENCE where name='VIPs' ''')
            print('Deleted')

        # Get all MSPs, so we can get people data about all of them
        cursor.execute("SELECT * FROM MSPs")
        rows = cursor.fetchall()

        cursor.execute('''SELECT MAX(msp_id) FROM VIPs;''')
        start_index = cursor.fetchone()[0]
        if start_index is not None and over_write is False:
            print(f'Start index is {start_index}, skipping written entries')
        else:
            start_index = -1

        for (msp_id, url) in [(row[0], row[2]) for row in rows]:

            if msp_id <= start_index:
                print(f'Skipped msp_id #{msp_id}')
                continue
            if msp_id > 15:
                break

            # Collect the VIPs's info
            people = api.get_people_filtered(url, titles)

            # Apollo might not have info to return, so just move on to the next
            if len(people) == 0:
                warnings.warn(f'No VIPs returned for (url, titles) pair: {url}, {titles}', UserWarning)
                continue

            # Parse info and store it in our db
            data = [
                (msp_id, person['first_name'], person['last_name'], person['title'], person['email'],
                 person['phone_numbers'][0]['sanitized_number']) for person in people]

            cursor.executemany(
                '''insert into VIPs (msp_id, firstName, lastName, title, email, phoneNumber) '''
                '''values ( ?, ?, ?, ?, ?, ?)''', data)
            connection.commit()
            print('Transactions committed to db')
            print(f'Stored VIPs, org number {msp_id}')


def main():
    # Get secrets
    load_dotenv()

    # Spin up db connection, and auto close on scope exit
    with closing(sqlite3.connect('MSP.db')) as connection:

        # Save top 500 MSPs to MSPs table in db
        if args.refresh_msps or args.refresh:
            get_msps(connection)
        else:
            print('Skipping MSP table update...')

        if args.refresh_vips or args.refresh:
            get_vips(connection, False)
        else:
            print('Skipping VIPs table update...')

        with closing(connection.cursor()) as cursor:
            if args.output_dir is not None:
                cursor.execute(
                    '''
                    SELECT MSPs.ID, NAME, URL, VP.id, FIRSTNAME, LASTNAME, TITLE, EMAIL, PHONENUMBER
                    FROM MSPs
                    LEFT JOIN VIPs VP on MSPs.id = VP.msp_id
                    ''')

                data = cursor.fetchall()

                with open(args.output_dir, 'w', newline='') as csv_file:
                    csv_writer = csv.writer(csv_file)
                    csv_writer.writerow(
                        [i[0] for i in cursor.description])  # Write the column headers
                    csv_writer.writerows(data)
            else:
                print('Not outputting data')
        # Persist changes to db
        connection.commit()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    updates = parser.add_mutually_exclusive_group()
    updates.add_argument('-r', '--refresh', help='wipes entire db and loads in fresh data',
                         action='store_true')
    updates.add_argument('-rv', '--refresh_vips', help='wipes only VIPs and loads in fresh data',
                         action='store_true')
    updates.add_argument('-rm', '--refresh_msps', help='wipes only MSPs and loads in fresh data',
                         action='store_true')
    output = parser.add_mutually_exclusive_group()
    output.add_argument('-o', '--output_dir', help='full path to file location, with filename')
    args = parser.parse_args()
    main()
