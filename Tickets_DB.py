import csv
import re
from pymongo import MongoClient
from pprint import pprint
from datetime import datetime


def read_data(csv_file, db):
    with open(csv_file, encoding='utf8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row_date in reader:
            regex_date = re.compile(r"(\d+)\.(\d{2})")
            day = regex_date.sub(r"\1", row_date['Дата'])
            month = regex_date.sub(r"\2", row_date['Дата'])
            event = {
                'artist': row_date['Исполнитель'],
                'price': int(row_date['Цена']),
                'location': row_date['Место'],
                'date': datetime(year=2020, month=int(month), day=int(day)),
            }
            result = db.insert_one(event)
    return result


def find_cheapest(db):
    result_sort = db.find().sort('price', 1)
    return list(result_sort)


def find_by_name(name, db):
    regex = re.compile(r"(.*)|(\w*)")
    search_name = regex.sub(r"\1", str(name))
    return list(db.find({'artist': {'$regex': search_name}}).sort('price', 1))

# Additional task


def find_by_date(date_to, date_from, db):
    regex_date = re.compile(r"(\d+)\.(\d{2})")
    day_to = regex_date.sub(r"\2", str(date_to))
    month_to = regex_date.sub(r"\1", str(date_to))
    search_dt = datetime(year=2020, month=int(month_to), day=int(day_to))
    day_from = regex_date.sub(r"\2", str(date_from))
    month_from = regex_date.sub(r"\1", str(date_from))
    search_df = datetime(year=2020, month=int(month_from), day=int(day_from))
    result = db.find({"date": {"$lt": search_df, "$gte": search_dt}}).sort('date', 1)
    return list(result)


if __name__ == '__main__':
    client = MongoClient()
    date_db = client['_concert_']
    artists = date_db['artists']
    # read_data('artists.csv', artists)
    # pprint(list(artists.find()))
    # find_cheapest(artists)
    # pprint(find_cheapest(artists))
    # pprint(find_by_name('Seconds to ', artists))
    pprint(find_by_date(2.15, 11.22, artists))


