import requests
import re
import time
from bs4 import BeautifulSoup
import pandas as pd

OUTPUT_FILE_NAME = "UNResultVeto.csv"
START_DATE = 1991


def topic_cutter(topic):
    """
    Function helps to reduce topics of resolutions to the key words in it
    :param topic: full topic name
    :return: string with key words in topic
    """
    if "Letter" in topic:
        return "Ukraine"
    elif "Palestinian" in topic:
        return "Palestine"
    elif "Syria" in topic:
        return "Syria"
    elif "Middle East" in topic:
        return "Middle East"
    elif "Central America" in topic:
        return "Central America"
    elif "Bosnia and Herzegovina" in topic:
        return "Bosnia and Herzegovina"
    elif "Venezuela" in topic:
        return "Venezuela"
    elif "international peace" in topic:
        return "International peace"
    elif "occupied Arab territories" in topic:
        return "Occupied Arab territories"
    elif "Yugoslav Republic of Macedonia" in topic:
        return "Yugoslav Republic of Macedonia"
    elif "Cyprus" in topic:
        return "Cyprus"
    else:
        return topic


def get_veto_info():
    """
    Function make GET request to the UN resource. The get HTML page
    and parse it to get needed data from it
    :return: list of dicts
    """
    result_list = list()
    result = requests.get("https://www.un.org/Depts/dhl/resguide/scact_veto_table_en.htm")
    soup = BeautifulSoup(result.content, "lxml")
    records = soup.find_all("tr")[3:]
    for record in records:
        data = record.find_all("td")
        date = data[0].text[-4:]
        if int(date) <= START_DATE:
            break
        resolution = data[1].text
        topic = topic_cutter(data[3].text)
        country = data[4].text
        if re.search("China", country) and len(country) > 5:
            country = ["China", "Russian Federation"]
        else:
            country = [country]
        for element in country:
            needed_data = {
                "vote date": date,
                "resolution": resolution,
                "topic": topic,
                "country": element
            }
            result_list.append(needed_data)
    return result_list


def save_to_csv(countries_data):
    """
    Function saves list of dicts to .csv file
    :param countries_data: list of dicts
    :return: None
    """
    df = pd.DataFrame(countries_data)
    df.to_csv(OUTPUT_FILE_NAME, index=False)


if __name__ == "__main__":
    """
    Start the acquisition process
    """
    start = time.time()
    data = get_veto_info()
    save_to_csv(data)
    end = time.time()
    print(f"The whole acquisition - {end-start}")
