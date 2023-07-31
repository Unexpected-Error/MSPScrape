from selenium import webdriver
from time import sleep
from selenium.webdriver.common.by import By
import pandas
import phonenumbers



def get_vips():
    """

    :return:

    Examples::
        >>> from selenium import webdriver; from time import sleep
        >>> from selenium.webdriver.common.by import By
        >>> get_vips()
    """

    # Attempt search
    driver = webdriver.Firefox()
    page = 1
    companies = ['google.com', 'acuative.com']
    companies_exact_match = 'false'
    titles = ['CEO']
    titles_exact_match = 'false'

    driver.get(
        f'https://login.seamless.ai/search/contacts?'
        f'page={str(page)}&'
        f'companies={"|".join(companies)}&'
        f'companiesExactMatch={companies_exact_match}&'
        f'locations=1&'
        f'locationTypes=both&'
        f'seniorities=1&'
        f'titles={"|".join(titles)}&'
        f'titlesExactMatch={titles_exact_match}')
    sleep(1)

    # Check for redirect to log in
    if 'seamless.ai/login' in driver.current_url:
        driver.find_element(By.NAME, "username").click()
        driver.find_element(By.NAME, "username").send_keys("atekle@seclytics.com")
        driver.find_element(By.NAME, "password").click()
        driver.find_element(By.NAME, "password").send_keys("S#FVbpJzH6mf2kgT6^Fc6D%")
        sleep(0.2)
        driver.find_element(By.CSS_SELECTOR, ".rs-btn-primary").click()
        sleep(2)



    buttons = driver.find_elements(By.XPATH,
                                   "/html/body/div[1]/div/div/div[2]/div[1]/div[2]/div[2]/table"
                                   "/tbody/tr/td[6]/div"
                                   "/button[count(.//*[name() = 'svg']/*[name() = 'circle'])]")

    print(f'Spending {len(buttons)} credits')

    # for button in buttons:
    #     sleep(3)
    #
    #     print(f'clicking button {button}')
    #     button.click()

    # "tr.Card-gcvsuc > td:nth-child(6) > "
    # "div:nth-child(1) > button:nth-child(1)"
    #
    # '/html/body/div[1]/div/div/div[2]/div[1]/div[2]/div[2]/table/tbody/tr[11]/td[6]/div/button'

    sleep(50)
    driver.quit()




def update_vips(name_key):
    print('hi')

def extract_cleaned(csv_path, required_conf):
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
        >>> extract_cleaned("/Users/adam/Downloads/cleanse_test-raw.csv", 70) # doctest: +ELLIPSIS
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
