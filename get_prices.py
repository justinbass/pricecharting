from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
import concurrent
import csv
import requests
import urllib3

CSV_IN_FILENAME = 'cards.csv'
CSV_OUT_FILENAME = 'prices.csv'

PSA_GRADING_PRICE = 30.00

UNGRADED = 'Ungraded'
PSA7 = 'Grade 7'
PSA8 = 'Grade 8'
PSA9 = 'Grade 9'
ALL_GRADES = [UNGRADED, PSA7, PSA8, PSA9]
GRADED_GRADES = [PSA7, PSA8, PSA9]

UNGRADED_CSV_ID = 'u'

CSV_GRADE_ID = {
    UNGRADED_CSV_ID: UNGRADED,
    '7': PSA7,
    '8': PSA8,
    '9': PSA9
}

UNGRADED_ID = 'used_price'
PSA7_ID = 'complete_price'
PSA8_ID = 'new_price'
PSA9_ID = 'graded_price'
ALL_GRADE_IDS = [UNGRADED_ID, PSA7_ID, PSA8_ID, PSA9_ID]

BASE_URL = 'https://www.pricecharting.com/game/'
PRICE_DATA_TABLE_ID = 'price_data'

def clean_price(raw):
    ret = raw
    for s in ['\\n', ' ', '-', '+']:
        ret = ret.replace(s, '')
    ret = ret[1:]
    ret = ret[:ret.find('$')]

    ret = ret if ret else 0

    try:
        ret = float(ret)
    except ValueError:
        ret = 0

    return ret

def get_prices(input_data):
    set_id, card_id, grade_id, count, url = input_data

    http = urllib3.PoolManager()
    r = http.request('GET', url)
    soup = BeautifulSoup(str(r.data), 'html.parser')

    price_data = soup.find('table', id=PRICE_DATA_TABLE_ID)

    price = dict()

    if price_data:
        for i in range(len(ALL_GRADES)):
            page_grade = ALL_GRADES[i]
            page_grade_id = ALL_GRADE_IDS[i]
            price_data_text = price_data.find('td', id=page_grade_id).text
            price[page_grade] = clean_price(price_data_text)

    return set_id, card_id, grade_id, count, url, price

def get_rows():
    rows = list()
    with open(CSV_IN_FILENAME) as csvfile:
        header_processed = False

        for row in csv.reader(csvfile):
            if not header_processed:
                header_processed = True
                continue

            set_id, card_id, grade_id, count, card_number = row

            grade_id = grade_id if grade_id and grade_id in ALL_GRADES else UNGRADED_CSV_ID
            grade_id = CSV_GRADE_ID[grade_id]

            count = int(count) if count else 1
            rows.append([set_id, card_id, grade_id, count])

    return rows

def get_prices_from_rows():
    input_data = list()
    rows = get_rows()
    for row in rows:
        set_id, card_id, grade_id, count = row
        url = BASE_URL + set_id + '/' + card_id
        input_data.append([set_id, card_id, grade_id, count, url])

    output_data = list()

    rows_processed = 0
    with ThreadPoolExecutor() as executor:
        future_to_url = { executor.submit(get_prices, input_datum) for input_datum in input_data }
        for future in concurrent.futures.as_completed(future_to_url):
            rows_processed += 1
            print(f'{rows_processed}/{len(rows)} processed')
            output_data.append(future.result())

    prices = list()
    for output_datum in output_data:
        gradeworthy = str(False)

        if not output_datum:
            prices.append([set_id, card_id, grade_id, count, 0, gradeworthy])
            continue

        set_id, card_id, grade_id, count, url, price = output_datum

        if not price:
            prices.append([set_id, card_id, grade_id, count, 0, gradeworthy])
            continue

        graded_price = price[grade_id] - PSA_GRADING_PRICE
        price = price[UNGRADED]

        if graded_price > price:
            gradeworthy = str(True)
            price = graded_price

        prices.append([set_id, card_id, grade_id, count, price, gradeworthy])

    return prices

def get_total():
    total = 0
    errors = list()

    prices = get_prices_from_rows()

    prices.sort(key=lambda l: l[1])

    with open(CSV_OUT_FILENAME, 'w') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(['set_id', 'card_id', 'grade_id', 'count', 'price', 'gradeworthy'])
        for i, data in enumerate(prices):
            set_id, card_id, grade_id, count, price, gradeworthy = data

            csvwriter.writerow(data)

            if price <= 0:
                errors.append(f'{i}: No price data: {set_id} {card_id}')
                continue

            print(f'{i}: {set_id}, {card_id}, {grade_id}, count: {count}: Gradeworthy: {gradeworthy}, ${price:.2f}')

            total += price * count

    print()
    for error in errors:
        print(error)
    print()

    print(f'Total: ${total:.2f}')

get_total()
