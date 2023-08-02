from time import sleep
from selenium import webdriver
from selenium.webdriver.common.by import By
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
import pandas
import phonenumbers


class Seamless:
    companies_exact_match = 'true'
    titles_exact_match = 'true'

    buttons_path = (
        "/html/body/div[1]/div/div/div[2]/div[1]/div[2]/div[2]/table/tbody/tr/td[6]/div"
        "/button[count(.//*[name() = 'svg']/*[name() = 'circle'])]")
    data_super_path = "//div[@id='PageContainer']/div[2]/div[2]/table/tbody/tr"
    name_path = "./td[2]/div/div[2]/div[1]/div[1]"
    title_path = "./td[2]/div/div[2]/div[1]/div[2]"
    email_group_path = "./td[4]/div/div[1][not(contains(@class, 'Locked'))]"
    # "/html/body/div[1]/div/div/div[2]/div[1]/div[2]/div[2]/table/tbody/tr/td[4]/div/div[2]/div[2]/div/button"
    email_subpath = "./div/div/button"
    last_page_path = ("/html/body/div[1]/div/div/div[2]/div[1]/div[2]/div[2]/div[2]/div[2]/ul/"
                      "li[last()]/button")

    def __init__(self, username, password):
        """
        initializes class and launches web browser

        :param username: Seamless account username
        :type username: str
        :param password: Seamless account password
        :type password: str
        """
        self.driver: webdriver = webdriver.Firefox()
        self.username: str = username
        self.password: str = password

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.driver.quit()

    def seamless_scrape_vips(self, companies: list, titles: list, credit_budget: int):
        """
        Scrapes Seamless.ai for VIPs information

        :param companies: Companies list, by name and/or domain
        :type companies: list
        :param titles: Titles list of str
        :type titles: list
        :param credit_budget: number of credits allowed to be spent during the execution of the func
        :type credit_budget: int

        :return: scraped data and credits spent
        :rtype: tuple

        Examples::
            >>> from selenium import webdriver; from time import sleep
            >>> from selenium.webdriver.common.by import By
            >>> from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
            >>> import os; from dotenv import load_dotenv; load_dotenv()
            True
            >>> with Seamless(os.getenv('SEAMLESS_USER'), os.getenv('SEAMLESS_PASS')) as scraper:
            ...     scraper.seamless_scrape_vips(['acuative.com'], ['CEO', 'Chief Executive Officer'], 0)
            selenium won't work in doc test for some reason
        """

        try:
            self.__get_with_redirection_check(
                f'https://login.seamless.ai/search/contacts?'
                f'page=1&'
                f'companies={"|".join(companies)}&'
                f'companiesExactMatch={self.companies_exact_match}&'
                f'locations=1&'
                f'locationTypes=both&'
                f'seniorities=1&'
                f'titles={"|".join(titles)}&'
                f'titlesExactMatch={self.titles_exact_match}')
        except RedirectedError:

            # If we got redirected to log in, it's harmless to keep going
            if 'seamless.ai/login' in self.driver.current_url:
                self.__login()
            else:
                raise

        try:
            number_of_pages = int(self.driver.find_elements(By.XPATH, self.last_page_path)[
                                      0].text)
        except IndexError:
            number_of_pages = 0

        results = []
        while True:
            buttons = self.driver.find_elements(By.XPATH, self.buttons_path)
            print(f'Spending {len(buttons)} credits')
            credits_spent = 0

            for index, button in enumerate(buttons, start=1):

                # Check if we can spend credits
                if index > credit_budget:
                    break

                print(f'clicking button {button}')
                button.click()
                credits_spent += 1
                sleep(3)

            data = self.driver.find_elements(By.XPATH, self.data_super_path)
            for vip in data:
                email_groups = vip.find_elements(By.XPATH, self.email_group_path)
                emails = [[element.text for element in group.find_elements(
                    By.XPATH, self.email_subpath)] for group in email_groups]
                name = vip.find_elements(By.XPATH, self.name_path)[0].text
                title = vip.find_elements(By.XPATH, self.title_path)[0].text

                if not emails:

                    print(f'Locked data, please use more credits:\n'
                          f'\turl: {self.driver.current_url}'
                          f'\tname: {name}'
                          f'\ttitle: {title}'
                          )
                    continue
                else:
                    print(f'the fucking emails were ')
                    print(emails)
                    results.append(tuple(name.split(" ")) + (title, emails[0][0]))

            # Do for all pages until there are none
            if not self.__next_page(number_of_pages):
                break
        return results, credits_spent

    def __next_page(self, max_pages: int):
        """
        Loads the next page given the current one and the max number of pages that exist

        :param max_pages: how many pages there are (used to not query a page that doesn't exist)
        :type max_pages: int
        :return: whether next page exists and was loaded
        :rtype: bool
        """

        parsed_url = urlparse(self.driver.current_url)
        query_params = parse_qs(parsed_url.query)

        if (tmp := int(query_params['page'][0]) + 1) <= max_pages:
            query_params['page'] = [str(tmp)]
        else:
            return False

        updated_query = urlencode(query_params, doseq=True)
        updated_url = urlunparse(parsed_url._replace(query=updated_query))

        self.__get_with_redirection_check(updated_url)
        return True

    def __login(self):
        """
        Logs into Seamless

        Make sure you are on the login page before calling

        """
        self.driver.find_element(By.NAME, "username").click()
        self.driver.find_element(By.NAME, "username").send_keys(self.username)
        self.driver.find_element(By.NAME, "password").click()
        self.driver.find_element(By.NAME, "password").send_keys(self.password)
        sleep(0.2)
        self.driver.find_element(By.CSS_SELECTOR, ".rs-btn-primary").click()
        sleep(5)

    def __get_with_redirection_check(self, url: str):
        """
        Navigates to url and throws RedirectedError if redirected

        :param url: url to navigate to
        :type url: str
        :raise RedirectedError:
        """
        self.driver.get(url)
        sleep(1)
        if self.driver.current_url != url:
            raise RedirectedError(
                f'Current url:\n\t{self.driver.current_url}\nDifferent from requested url:\n\t{url}'
            )

    @staticmethod
    def extract_cleaned(csv_path: str, required_conf: int) -> dict:
        """
        Get a parsed dictionary of cleaned data given a csv from https://login.seamless.ai/enrich

        :param csv_path: full path, including file, of csv to extract from
        :type csv_path: str
        :param required_conf: % chance reqired to treat data as true (required_conf/100)
        :type required_conf: int
        :return: dictionary of emails and phone numbers keyed by tuple (first, last) name
        :rtype: dict

        Example::
            >>> import pandas; import phonenumbers
            >>> Seamless.extract_cleaned("/Users/adam/Downloads/cleanse_test-raw.csv", 70)
            ... # doctest: +ELLIPSIS
            {('Chuck', 'Bloodworth'): {'email': 'cbloodworth@1path.com'},...}
        """

        # load file and extracted needed columns
        first_ten = pandas.read_csv(csv_path)[
            ['First Name', 'Last Name'] +
            [f'Email {i} Total AI' for i in range(1, 11)] +
            [f'Email {i}' for i in range(1, 11)] +
            [f'Contact Phone {i} Total AI' for i in range(1, 11)] +
            [f'Contact Phone {i}' for i in range(1, 11)]
            ]

        # start results dictionary
        result_list = {}

        # iterate through the 10 columns each of phone and email
        for index in range(1, 11):

            # 'total ai format' is ##%, change to integer to preform '>' operation against required_conf
            temp_row = first_ten[first_ten[f'Email {index}'].notna()]
            if not temp_row.empty:
                first_ten[f'Email {index} Total AI'] = temp_row[
                    f'Email {index} Total AI'].str.replace('%', '').astype(int)

            temp_row = first_ten[first_ten[f'Contact Phone {index}'].notna()]
            if not temp_row.empty:
                first_ten[f'Contact Phone {index} Total AI'] = temp_row[
                    f'Contact Phone {index} Total AI'].str.replace('%', '').astype(int)

            # check if email and number meet criteria, and add them to our results
            emails = first_ten[first_ten[f'Email {index} Total AI'] > required_conf].apply(
                lambda row, idx:
                {(row['First Name'], row['Last Name']): (row[f'Email {idx}'])},
                args=(index,),
                axis=1).tolist()
            phones = first_ten[first_ten[f'Contact Phone {index} Total AI'] > required_conf].apply(
                lambda row, idx:
                {(row['First Name'], row['Last Name']): (row[f'Contact Phone {idx}'])},
                args=(index,),
                axis=1).tolist()

            for email_dict in emails:
                for key, value in email_dict.items():
                    if key not in result_list:
                        # print(f'eKey {key} does not exist in results')
                        result_list[key] = {}
                        result_list[key]['email'] = value
                    elif not result_list.get(key, {}).get('email', False):
                        # print(f'eKey {key} does exist in results without email')
                        result_list[key]['email'] = value
                    else:
                        pass
                        # print(f'eKey {key} does exist in results with email')

            for phone_dict in phones:
                for key, value in phone_dict.items():
                    if key not in result_list:
                        # print(f'pKey {key} does not exist in results')
                        result_list[key] = {}
                        result_list[key]['phone'] = phonenumbers.format_number(
                            phonenumbers.parse(value, 'US'),
                            phonenumbers.PhoneNumberFormat.E164)
                    elif not result_list.get(key, {}).get('phone', False):
                        # print(f'pKey {key} does exist in results without phone')
                        result_list[key]['phone'] = phonenumbers.format_number(
                            phonenumbers.parse(value, 'US'),
                            phonenumbers.PhoneNumberFormat.E164)
                    else:
                        pass
                        # print(f'pKey {key} does exist in results with phone')

        return result_list


class RedirectedError(Exception):
    def __init__(self, message):
        self.message = message
        super().__init__(message)
