import csv
import logging
import os

from datetime import datetime

import grequests
import xlsxwriter

from bs4 import BeautifulSoup
from tqdm import tqdm

REQUESTS_STEP = 50

# datetime object containing current date and time
now = datetime.now()
# dd/mm/YY H:M:S - "%Y_%m_%d-%I:%M:%S_%p"
dt_string = now.strftime("%d_%m_%Y %H-%M-%S")


BASE_DIR = os.path.dirname(os.path.realpath(__file__))
HEDERS = {
    "Accept": "*/*",
    "User-Agent": "Mozilla/5.0 (iPad; CPU OS 11_0 like Mac OS X) AppleWebKit/604.1.34 (KHTML, like Gecko) "
                  "Version/11.0 Mobile/15A5341f Safari/604.1"
}

LOG_FORMAT = '%(asctime)s %(levelname)s: %(funcName)s: - %(message)s'
logging.basicConfig(filename=os.path.join(BASE_DIR, "logs.txt"), level=logging.ERROR, format=LOG_FORMAT)
logging = logging.getLogger()


def import_urls_from_csv() -> list:
    try:
        with open('tab_urls.csv') as file:
            file_data = csv.reader(file)
            logging.info("Import urls from csv.")
            return [i[0] for i in file_data if len(i)>0]

    except FileNotFoundError:
        logging.info("Can't find a file 'tab_urls.csv'")
        print("Не смог найти файл или неверное имя - должно быть 'tab_urls.csv' ")

    except Exception as ex:
        print(ex)
        logging.error(ex, exc_info=True)


def create_price_exle(data: list[dict]):
    workbook = xlsxwriter.Workbook('tab_price.xlsx')
    worksheet = workbook.add_worksheet()
    worksheet.write(0, 0, "url")
    worksheet.write(0, 1, 'new_price')
    worksheet.write(0, 2, 'old_price')
    worksheet.write(0, 3, "article")

    row = 1
    col = 0
    # Iterate over the data and write it out row by row.
    for item in data:
        worksheet.write(row, col, item["url"])
        worksheet.write(row, col + 1, int(item["price_new"]))
        worksheet.write(row, col + 2, int(item["price_old"]))
        worksheet.write(row, col + 3, item["article"])
        row += 1
    workbook.close()

def create_price_csv(data: list[dict]):
    header = ["url", "article", 'new_price', 'old_price']
    data_to_file = []

    for row in data:
        data_to_file.append([row["url"], row["article"], row["price_new"], row["price_old"]])

    with open('tab_price.csv', 'w', encoding='UTF8', newline='') as file:
        writer = csv.writer(file)
        # write the header
        writer.writerow(header)
        # write multiple rows
        writer.writerows(data_to_file)

    #async  requests
def get_async_data(urls):
    reqs = [grequests.get(link, headers=HEDERS) for link in urls]
    responses = grequests.map(reqs)
    return responses


def get_price_from_url():
    wrong_url = []
    data_to_file = []
    bead_row = []
    urls: list = import_urls_from_csv()
    wrong_row = 1

    if urls:
        #progress bar
        new_tqdm: tqdm = tqdm(total=len(urls), desc="Get data from url")

        #list of urls by step + tail
        output = [urls[i:i + REQUESTS_STEP] for i in range(0, len(urls), REQUESTS_STEP)]
        for slice in output:
            for data_html in get_async_data(slice):
                if data_html:
                    soup = BeautifulSoup(data_html.text, "html.parser")
                    article = soup.find("div", class_="shop2-product-article").text.replace("Артикул:",
                                                                                                            "").strip()

                    price_new = soup.find("div", class_="form-add")
                    price_new = price_new.find("div", class_="price-current").text.replace("руб.", "").replace(" ",
                                                                                                          "").strip()
                    price_new = "".join([did for did in price_new if did.isalnum()])


                    price_old = soup.find("div", class_="form-add").find("div", class_="price-old")
                    if price_old:
                        price_old = price_old.text.replace("руб.", "").replace(" ",
                                                                                                              "").strip()
                        price_old = "".join([did for did in price_old if did.isalnum()])
                    else:
                        price_old = "0"

                    data_to_file.append(
                            {"article": article,
                             "price_new": price_new,
                             "price_old": price_old, "url": data_html.url}
                        )
                    new_tqdm.update(1)
                    wrong_row +=1
                else:
                    try:
                        wrong_url.append(data_html.url)
                        new_tqdm.update(1)
                    except:
                        logging.info(f"Bad row #{wrong_row}")
                        bead_row.append(f"Broken row #{wrong_row}")
                        new_tqdm.update(1)
                    wrong_row += 1

            create_price_csv(data_to_file)
            create_price_exle(data_to_file)

#wrong/bead urls collector
        if len(bead_row) > 0:
            wrong_url.append(str(bead_row))

    if len(wrong_url):
        with open(f'{dt_string} wrong_urls.txt', 'w') as f:
            for line in wrong_url:
                f.write(line)
                f.write('\n')


if __name__ == '__main__':
    get_price_from_url()
