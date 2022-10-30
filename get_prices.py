from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
import concurrent
import csv
import requests
import sys
import urllib3

DEFAULT_CARD_PRICE = 1.00
PSA_GRADING_PRICE = 30.00
INVALID_PRICE = -1
INVALID_ROW_ID = -1

# Adjustment to ebay price: USPS Shipping + toploader + sleeve cost
OVERHEAD_PER_CARD = 0.6 + 0.17 + 0.08

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
    for s in ['\\n', ' ', '-', '+', ',']:
        ret = ret.replace(s, '')
    ret = ret[1:]
    ret = ret[:ret.find('$')]

    ret = ret if ret else 0

    try:
        ret = float(ret)
    except ValueError:
        if ret == 'N/A' or ret == '/A':
            ret = 0
        else:
            print(f'ERROR: Unexpected string: {ret}')
            ret = INVALID_PRICE

    return ret

def get_prices(input_data):
    row_id, set_id, card_id, grade_id, count, url, notes = input_data

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
    else:
        for i in range(len(ALL_GRADES)):
            page_grade = ALL_GRADES[i]
            price[page_grade] = INVALID_PRICE

    return row_id, set_id, card_id, grade_id, count, url, price, notes

def get_rows():
    rows = list()
    with open(sys.argv[1]) as csvfile:
        header_processed = False

        for row in csv.reader(csvfile):
            if not header_processed:
                header_processed = True
                continue

            set_id, card_id, grade_id, count, card_number, notes = row

            grade_id = grade_id if grade_id and str(grade_id) in CSV_GRADE_ID.keys() else UNGRADED_CSV_ID
            grade_id = CSV_GRADE_ID[grade_id]

            count = int(count) if count else 1

            if count:
                rows.append([set_id, card_id, grade_id, count, notes])

    return rows

def get_prices_from_rows():
    input_data = list()
    rows = get_rows()
    for row_id, row in enumerate(rows):
        set_id, card_id, grade_id, count, notes = row
        url = BASE_URL + set_id + '/' + card_id
        input_data.append([row_id, set_id, card_id, grade_id, count, url, notes])

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
            prices.append([INVALID_ROW_ID, set_id, card_id, grade_id, count, INVALID_PRICE, gradeworthy, None])
            continue

        row_id, set_id, card_id, grade_id, count, url, price, notes = output_datum

        if not price:
            prices.append([row_id, set_id, card_id, grade_id, count, 0, gradeworthy, notes])
            continue

        if price == INVALID_PRICE:
            prices.append([row_id, set_id, card_id, grade_id, count, INVALID_PRICE, gradeworthy, notes])
            continue

        graded_price = price[str(grade_id)] - PSA_GRADING_PRICE
        price = price[UNGRADED]

        if graded_price > price:
            gradeworthy = str(True)
            price = graded_price

        prices.append([row_id, set_id, card_id, grade_id, count, price, gradeworthy, notes])

    return prices

def get_total():
    if len(sys.argv) < 2:
        print('Error, not enough args. Try: get_prices.py in.csv')
        return

    total = 0
    total_adjusted = 0
    card_count = 0
    no_prices = list()
    errors = list()

    prices = get_prices_from_rows()

    # Sort on row id
    prices.sort(key=lambda l: l[0])

    with open('prices_' + sys.argv[1], 'w') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(['set_id', 'card_id', 'grade_id', 'count', 'price', 'gradeworthy', 'notes'])
        for i, data in enumerate(prices):
            row_id, set_id, card_id, grade_id, count, price, gradeworthy, notes = data

            print(f'{i}: {set_id}, {card_id}, {grade_id}, count: {count}: Gradeworthy: {gradeworthy}, ${price:.2f}')

            if price == 0:
                no_prices.append(f'{i}: No price data: {set_id} {card_id}, defaulting to ${DEFAULT_CARD_PRICE:.2f}')
                price = DEFAULT_CARD_PRICE

                # Leave CSV to show 0 for missing data. Total price will reflect default addition.
                # data = set_id, card_id, grade_id, count, price, gradeworthy

            elif price == INVALID_PRICE:
                errors.append(f'{i}: Card id not found: {set_id} {card_id}, defaulting to ${DEFAULT_CARD_PRICE:.2f}')
                price = DEFAULT_CARD_PRICE

                # Leave CSV to show -1 for missing data. Total price will reflect default addition.
                # data = set_id, card_id, grade_id, count, price, gradeworthy

            data = set_id, card_id, grade_id, count, price, gradeworthy, notes
            csvwriter.writerow(data)

            card_count += count
            total += price * count

        csvwriter.writerow(['total buy', '', '', '', total, ''])

        total_adjusted = total - card_count * OVERHEAD_PER_CARD
        csvwriter.writerow(['total sell', '', '', '', total_adjusted, ''])

    print()
    for no_price in no_prices:
        print(no_price)
    print()

    print()
    for error in errors:
        print(error)
    print()

    print(f'Total buy: ${total:.2f}')
    print(f'Total sell: ${total_adjusted:.2f}')

get_total()
