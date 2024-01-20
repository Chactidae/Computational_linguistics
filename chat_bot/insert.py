import pymongo
import csv
import datetime
from aiogram.dispatcher import Dispatcher
from aiogram import Bot, types
from aiogram.utils import executor

# Подключение к MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017/")
# Создание базы данных
database_name = "news_database"
db = client[database_name]
# Создание коллекции
collection_name = "news_collection"
collection = db[collection_name]
csv_file_path = "file10k.csv"
with open(csv_file_path, 'r', encoding='utf-8') as csv_file:
     # Создаем объект чтения CSV
     csv_reader = csv.reader(csv_file)

     # Пропускаем заголовок, если он есть
     next(csv_reader, None)

     # Читаем и вставляем данные в базу данных
     for row in csv_reader:
         url, title, date_str, text = row

         # Преобразуем строку даты в объект datetime
         date = datetime.datetime.strptime(date_str, '%Y-%m-%d')

         # Создаем объект новости
         news_item = {
             "link": url,
             "title": title,
             "date": date,
             "text": text
             }
         # Вставляем новость в коллекцию
         collection.insert_one(news_item)
