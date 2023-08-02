from contextlib import closing
import os
import sys
from time import sleep
import warnings
import argparse
import sqlite3
import csv
import requests
from lxml import html
from dotenv import load_dotenv
from apollo import ApolloAPI
from seamless import Seamless

# List of titles to search
titles = [
    # 'Chief Executive Officer', 'CEO',
    # 'Chief Product Officer', 'CPO',
    'Chief Innovation Officer', 'CIO',
    'VP operations', 'VP of operations', 'VPO',
    'Chief Operating Officer', ' COO',
    'Chief Technology Officer', 'CTO'
]


def get_msps(connection, over_write):
    with closing(connection.cursor()) as cursor:

        # Wipe table and increment counter
        if over_write:
            print('Deleting MSPs table in 3...')
            sleep(1)
            print('2...')
            sleep(1)
            print('1...')
            sleep(1)
            # noinspection SqlWithoutWhere
            cursor.execute('''Delete from MSPs''')
            cursor.execute('''Delete from SQLITE_SEQUENCE where name='MSPs' ''')
            print('Deleted')
        else:
            print('MSPs refresh does not implement resuming at this time, please wipe DB')
            return

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
        cursor.executemany('''insert into MSPs (Name, URL) values (?, ?)''', msps)
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
            print('Deleting VIPs table in 3...')
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

            # Collect the VIPs's info
            people = api.get_people_filtered(url, titles)

            # Apollo might not have info to return, so just move on to the next
            if len(people) == 0:
                warnings.warn(f'No VIPs returned for (url, titles) pair: {url}, {titles}',
                              UserWarning)
                continue

            # Parse info and store it in our db
            data = [
                (
                    msp_id, person['first_name'], person['last_name'], person['title'],
                    person['email'],
                    person['phone_numbers'][0]['sanitized_number']) for person in people]

            cursor.executemany(
                '''insert into VIPs (MSPID, FirstName, LastName, Title, Email) '''
                '''values ( ?, ?, ?, ?, ?)''', [point[:-1] for point in data])
            cursor.execute(
                '''UPDATE MSPs SET CompanyNumber=(?) where MSPs.id=(?)''', (data[0][-1], msp_id))
            connection.commit()
            print('Transactions committed to db')
            print(f'Stored VIPs, org number {msp_id}')


def main():
    # Get secrets
    load_dotenv()

    # Spin up db connection, and auto close on scope exit
    with closing(sqlite3.connect('MSP.db')) as connection:

        # Create tables if they don't exist
        with closing(connection.cursor()) as cursor:
            with open('up.sql', 'r') as sql_file:
                sql_script = sql_file.read()
                cursor.executescript(sql_script)
                connection.commit()

        # Save top 500 MSPs to MSPs table in db
        if args.refresh_msps or args.wipe:
            get_msps(connection)
        else:
            print('Skipping MSP table update...')

        # Get vips info and save to db
        if args.refresh_vips or args.wipe:
            get_vips(connection, args.wipe)
        else:
            print('Skipping VIPs table update...')

        with closing(connection.cursor()) as cursor:
            if args.seamless_update is not None:
                credits_remaining = args.seamless_update
                with Seamless(os.getenv('SEAMLESS_USER'), os.getenv('SEAMLESS_PASS')) as scraper:
                    # get missing MSPs
                    cursor.execute("WITH Numbers AS ("
                                   "SELECT 1 AS num "
                                   "UNION ALL "
                                   "SELECT num + 1 "
                                   "FROM Numbers "
                                   "WHERE num < 500"
                                   ") "
                                   "SELECT num "
                                   "FROM Numbers "
                                   "LEFT JOIN VIPs ON Numbers.num = VIPs.MSPID "
                                   "WHERE VIPs.MSPID IS NULL;")
                    keys = [key[0] for key in cursor.fetchall()]
                    cursor.execute(
                        f"SELECT * FROM main.MSPs WHERE ID IN ({','.join(map(str, keys))});")
                    missed_msps = cursor.fetchall()
                    for (msp_id, name, url, _) in missed_msps:
                        data = scraper.seamless_scrape_vips([url], titles, credits_remaining)
                        credits_remaining -= data[1]
                        results = data[0]
                        print(f'Inserting {len(results)} contacts for {name}, {credits_remaining} '
                              f'credits remaining')
                        cursor.executemany(
                            '''insert into VIPs (MSPID, FirstName, LastName, Title, Email) '''
                            '''values ( ?, ?, ?, ?, ?)''',
                            [(msp_id,) + result for result in results])
            else:
                print("Not scraping seamless")

            if args.clean_csv is not None:
                data = Seamless.extract_cleaned(args.clean_csv, 70)
                for key, value in data.items():
                    if value.get('email', False):
                        if value.get('phone', False):
                            # both
                            cursor.execute(
                                'UPDATE VIPs SET (Phone, Email) = (?,?) '
                                'WHERE FirstName = ? AND LastName = ?'
                                , (value.get('phone'), value.get('email'), key[0], key[1]))
                            if connection.total_changes > 0:
                                print("Update was successful.")
                            else:
                                print(
                                    "No rows were affected. The update may not have matched any records.")
                        # only email
                        else:
                            cursor.execute(
                                'UPDATE VIPs SET Email = ? '
                                'WHERE FirstName = ? AND LastName = ?'
                                , (value.get('email'), key[0], key[1]))
                            if connection.total_changes > 0:
                                print("Update was successful.")
                            else:
                                print(
                                    "No rows were affected. The update may not have matched any records.")
                    else:
                        if value.get('phone', False):
                            # only phone
                            cursor.execute(
                                'UPDATE VIPs SET Phone = ? '
                                'WHERE FirstName = ? AND LastName = ?'
                                , (value.get('phone'), key[0], key[1]))
                            if connection.total_changes > 0:
                                print("Update was successful.")
                            else:
                                print(
                                    "No rows were affected. The update may not have matched any records.")
                        else:
                            print(f'value {value}  did not have phone or email')
                connection.commit()
            else:
                print("No cleaned data")

            if args.output_dir is not None:
                # Export db as csv
                if args.output_all:
                    cursor.execute(
                        '''
                        SELECT MSPs.ID, MSPs.Name, URL, CompanyNumber, VP.ID, FirstName, LastName, Title, Email, Phone
                        FROM MSPs
                        LEFT JOIN VIPs VP on MSPs.ID = VP.MSPID
                        WHERE 
                            Name IS NOT NULL
                            AND FirstName IS NOT NULL
                            AND LastName IS NOT NULL
                            AND Email IS NOT NULL
                        ''')

                    data = cursor.fetchall()

                    with open(args.output_dir, 'w', newline='') as csv_file:
                        csv_writer = csv.writer(csv_file)
                        csv_writer.writerow(
                            [i[0] for i in cursor.description])  # Write the column headers
                        csv_writer.writerows(data)
                # Export missing companies as csv
                if args.output_missing:
                    cursor.execute("WITH Numbers AS ("
                                   "SELECT 1 AS num "
                                   "UNION ALL "
                                   "SELECT num + 1 "
                                   "FROM Numbers "
                                   "WHERE num < 500"
                                   ") "
                                   "SELECT num "
                                   "FROM Numbers "
                                   "LEFT JOIN VIPs ON Numbers.num = VIPs.MSPID "
                                   "WHERE VIPs.MSPID IS NULL;")
                    keys = [key[0] for key in cursor.fetchall()]
                    cursor.execute(
                        f"SELECT * FROM main.MSPs WHERE ID IN ({','.join(map(str, keys))});")
                    missed_msps = cursor.fetchall()
                    with open(args.output_dir, 'w', newline='') as csv_file:
                        csv_writer = csv.writer(csv_file)
                        csv_writer.writerow(
                            [i[0] for i in cursor.description])  # Write the column headers
                        csv_writer.writerows(missed_msps)
            else:
                print('Not outputting data')

        # Persist changes to db
        connection.commit()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--clean_csv', help='loads cleaned csv from filepath into db')
    parser.add_argument('-w', '--wipe', help='wipes db and loads new data',
                        action='store_true')
    parser.add_argument('-rv', '--refresh_vips', help='refreshes VIPs table',
                        action='store_true')
    parser.add_argument('-rm', '--refresh_msps', help='refreshes MSPs table',
                        action='store_true')
    parser.add_argument('-su', '--seamless_update', help='scrapes seamless with n credits',
                        type=int)
    group = parser.add_mutually_exclusive_group()
    group.add_argument('-om', '--output_missing', help='outputs missing MSPs',
                       action='store_true')
    group.add_argument('-oa', '--output_all', help='outputs all data in a master csv',
                       action='store_true')
    parser.add_argument('-o', '--output_dir', help='full path to file location, with filename',
                        required=any(arg in sys.argv for arg in
                                     ['--output_all', '-oa', '--output_missing', '-om']
                                     ))
    args = parser.parse_args()
    main()
