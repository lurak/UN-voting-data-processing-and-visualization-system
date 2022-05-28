import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
from multiprocessing import Process, Manager
import pycountry_convert as pc

# Time in one parallel - 5972.65446305275 ~ 90 mins
# Time in 8 parallels - 1153.8270535469055 ~ 19 mins
STEP = 50
OUTPUT_FILE_NAME = "UNResult.csv"
START_DATE = 1991
END_DATE = 2022
NUMBER_OF_PARALLELS = 8


def get_ids_links(page_number, year):
    """
    Functions get ids of resolutions from main page from source
    :param page_number: number of page
    :param year: current year
    :return: list of resolutions ids
    """
    list_of_ids_links = list()
    resp = requests.get(
        url=f"https://digitallibrary.un.org/search?ln=en&c=Voting+Data&jrec={page_number}&fct__3={year}&cc=Voting+Data&fct__9=Vote&fct__9=Vote"
    )
    soup = BeautifulSoup(resp.content, "lxml")
    records = soup.find_all("div", class_="moreinfo")
    for record in records:
        voting_id = record.find("a", class_="moreinfo")
        list_of_ids_links.append(voting_id["href"])
    return list_of_ids_links


def find_info(records, record_name):
    """
    Function finds information about specific field
    :param records: piece of HTML page
    :param record_name: str with specific field
    :return: str with needed data
    """
    for record in records:
        record = record.find_all("span")
        if record[0].text == record_name:
            return record[1].text


def date_handler(date):
    """
    Function cut off 1981 year and other waste dates
    :param date: str with date
    :return: bool indication
    """
    try:
        if date[0:4] != "1981":
            return True
        else:
            return False
    except TypeError:
        return False


def transform_country_name(name_list):
    """
    Function to deal with edge cases in country name
    :param name_list: list of country name
    :return: list with correct country name
    """
    result_lst = list()
    for name in name_list:
        if name[0] == "(":
            result_name = name[2:].lower()
            result_name = name[:2] + result_name
        else:
            result_name = name[1:].lower()
            result_name = name[0] + result_name
        result_lst.append(result_name)
    return result_lst


def country_rename(country):
    """
    Function to clean up some countries name
    :param country: str with name of country
    :return: str name of country
    """
    if "Iran" in country:
        return "Iran"
    elif country == "Republic Of Korea":
        return "South Korea"
    elif country == "Democratic People's Republic Of Korea":
        return "North Korea"
    elif "Bosnia" in country:
        return "Bosnia and Herzegovina"
    elif "Moldova" in country:
        return "Republic of Moldova"
    elif country == "Democratic Republic Of The Congo":
        return "Democratic Republic of the Congo"
    elif country == "United Republic Of Tanzania":
        return "United Republic of Tanzania"
    elif country == "Cote D'ivoire":
        return "Ivory Coast"
    elif country == "Antigua And Barbuda":
        return "Antigua and Barbuda"
    elif country == "Trinidad And Tobago":
        return "Trinidad and Tobago"
    elif country == "Guinea-bissau":
        return "Guinea-Bissau"
    elif country == "Czechia":
        return "Czech Republic"
    else:
        return country


def get_page_info(link_id):
    """
    Function gets id of resolution, make GET request and parse HTML page to extract needed
    information
    :param link_id: str id of resolution
    :return: list of dicts
    """
    countries_data = list()
    page_url = "https://digitallibrary.un.org" + link_id
    resp = requests.get(
        url=page_url
    )
    soup = BeautifulSoup(resp.content, "lxml")
    metadata = soup.find_all("div", class_="metadata-row")
    title = find_info(metadata, "Title")
    resolution = find_info(metadata, "Resolution")
    vote_date = find_info(metadata, "Vote date")
    countries = metadata[-2].find_all("span", class_="value col-xs-12 col-sm-9 col-md-10")[-1]
    for br in countries.childGenerator():
        if not date_handler(vote_date):
            break
        if not str(br) == "<br/>":
            country = str(br)
            vote_results = country.split(" ")
            vote_results = [x for x in vote_results if x]
            if len(vote_results[0]) != 1:
                vote_results = transform_country_name(vote_results)
                data = {"country": " ".join(vote_results),
                        "vote result": "NV",
                        "title": title,
                        "vote date": vote_date[0:4],
                        "resolution": resolution}
            else:
                vote_result = vote_results[0]
                vote_results = transform_country_name(vote_results[1:])
                data = {"country": " ".join(vote_results),
                        "vote result": vote_result,
                        "title": title,
                        "vote date": vote_date[0:4],
                        "resolution": resolution}
            data["country"] = country_rename(data["country"])
            try:
                pc.country_name_to_country_alpha2(data["country"], cn_name_format="default")
                countries_data.append(data)
            except KeyError:
                continue
    return countries_data


def get_max_page_number(year):
    """
    Function get the max number of pages with resolutions for current
    year
    :param year: int value of year for which run function
    :return: int max number of pages
    """
    number_of_pages = 0
    resp = requests.get(
        url=f"https://digitallibrary.un.org/search?ln=en&c=Voting+Data&jrec=1&fct__3={year}&cc=Voting+Data&fct__9=Vote&fct__9=Vote"
    )
    soup = BeautifulSoup(resp.content, "lxml")
    number_info = soup.find_all("div", class_="checkbox")
    for record in number_info:
        if record.find_all("span")[0].text == "Vote":
            number_of_pages = int(record.find_all("span")[1].text)
            break
    return number_of_pages


def save_to_csv(countries_data):
    """
    Function saves list of dicts to .csv file
    :param countries_data: list of dicts
    :return: None
    """
    df = pd.DataFrame(countries_data)
    df = df.dropna()
    df.to_csv(OUTPUT_FILE_NAME, index=False)


def data_acquisition(year_list, final_list):
    """
    Function which perform acquisition. Will be passed to Thread
    :param year_list: List of years from which take data
    :param final_list: List where to put result
    :return: None
    """
    result_list = list()
    for year in year_list:
        loc_start = time.time()
        max_page = get_max_page_number(year)
        limit = max_page // STEP + 1
        for i in range(limit):
            ids_links_lst = get_ids_links(STEP * i + 1, year)
            for link in ids_links_lst:
                data_list = get_page_info(link)
                result_list.extend(data_list)
        loc_end = time.time()
        print(f"Year - {year}, Time - {loc_end - loc_start}")
    final_list.append(result_list)


if __name__ == "__main__":
    """
    Start the acquisition process in 8 Threads
    """
    gl_start = time.time()
    year_list = list(range(START_DATE, END_DATE + 1))
    limit = len(year_list) // NUMBER_OF_PARALLELS
    procs = []
    manager = Manager()
    final_list = manager.list()
    for i in range(NUMBER_OF_PARALLELS):
        proc = Process(target=data_acquisition, args=(year_list[i*limit:limit*(i+1)], final_list))
        procs.append(proc)
        proc.start()
    for proc in procs:
         proc.join()
    final_list = [item for sublist in final_list for item in sublist]
    save_to_csv(final_list)
    gl_end = time.time()
    print(f"The whole acquisition - {gl_end-gl_start}")
