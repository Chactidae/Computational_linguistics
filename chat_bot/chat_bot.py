import requests
from bs4 import BeautifulSoup
import pandas
import pymongo
import csv
from aiogram.utils.markdown import text
import datetime
import time
import sys
from pyspark.sql import SparkSession
from pyspark.ml.feature import Word2Vec
from pyspark.ml import Pipeline
from pyspark.ml.feature import Tokenizer
sys.path.append('/home/olegsnk/tomita-parser/build/bin')
import parse
from aiogram.dispatcher import Dispatcher
from aiogram import Bot, types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.utils import executor
from nltk.stem.wordnet import WordNetLemmatizer
from nltk import FreqDist, classify, NaiveBayesClassifier
from nltk.corpus import stopwords
from nltk.tag import pos_tag
from nltk.tokenize import word_tokenize
import asyncio

import re, string, random
import pymorphy2

weather_token = "620c50a921a12724a03ca6231810bb08"
tg_bot_token = "6189166384:AAFz2IWE--uyi2BkXxrI9Pu6qgSUgLlXsUI"
image_Clear = "https://img2.joyreactor.cc/pics/post/красивые-картинки-кто-автор-shinkai_makoto-Shinkai-Makoto-3385185.jpeg"
image_Clouds = "https://images.kinorium.com/movie/shot/1627013/w1500_3380527.jpg"
image_Rain = "https://i.ytimg.com/vi/h0MU7siSLxc/maxresdefault.jpg"

bot = Bot(token=tg_bot_token)
dp = Dispatcher(bot)
skip = 10
global count

#Взять 10 новостей из базы данных
async def get_latest_news(url_last, url_first):
    # Подключение к MongoDB
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    database_name = "news_database"
    db = client[database_name]
    collection_name = "news_collection"
    collection = db[collection_name]
    if url_last and url_first:
       result = collection.find({'link': url_first}).limit(10)
    else:
       result = collection.find({'date': '2024-01-20'}).limit(10)
    #result = collection.find({'date': '2024-01-19'}).limit(10)
    return result

#Реврайтер
async def rewriter(text):
    url = 'https://api.aicloud.sbercloud.ru/public/v2/rewriter/predict'

    data = {
        "instances": [
            {
            "text": text[:1000],
            "temperature": 0.9,
            "top_k": 50,
            "top_p": 0.7,
            "range_mode": "bertscore"
            }
        ]
    }

    response = requests.post(url=url, json=data)

    return response.json()['prediction_best']['bertscore']

#Суммаризатор
async def summarise(text):
    url = 'https://api.aicloud.sbercloud.ru/public/v2/summarizator/predict'

    data = {
        "instances": [
            {
                "text": text[:1000],
                "num_beams": 5,
                "num_return_sequences": 3,
                "length_penalty": 0.5
            }
        ]
    }

    response = requests.post(url=url, json=data)

    return response.json()['prediction_best']['bertscore']


async def get_tone(content):
    morph = pymorphy2.MorphAnalyzer()
    def remove_noise(tweet_tokens, stop_words=()):
        cleaned_tokens = []

        for token, tag in pos_tag(tweet_tokens):
            token = re.sub('http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+#]|[!*\(\),]|'\
                       '(?:%[0-9a-fA-F][0-9a-fA-F]))+', '', token)
            token = re.sub("(@[A-Za-z0-9_]+)", "", token)

            pos = morph.parse(token)[0].tag.POS
            if pos:
                if pos.startswith("N"):
                    pos = 'n'
                elif pos.startswith('V'):
                    pos = 'v'
                else:
                    pos = 'a'

                lemmatizer = WordNetLemmatizer()
                token = lemmatizer.lemmatize(token, pos)

            if len(token) > 0 and token not in string.punctuation and token.lower() not in stop_words:
                cleaned_tokens.append(token.lower())

        return cleaned_tokens

    def get_all_words(cleaned_tokens_list):
        for tokens in cleaned_tokens_list:
            for token in tokens:
                yield token

    def get_tweets_for_model(cleaned_tokens_list):
        for tweet_tokens in cleaned_tokens_list:
            yield dict([token, True] for token in tweet_tokens)

    top_words = stopwords.words('russian')

    positive_tweet_tokens = ["позитивный", "текст", "пример"]
    negative_tweet_tokens = ["негативный", "текст", "пример"]

    positive_cleaned_tokens_list = [remove_noise(tokens, top_words) for tokens in [positive_tweet_tokens]]
    negative_cleaned_tokens_list = [remove_noise(tokens, top_words) for tokens in [negative_tweet_tokens]]

    all_pos_words = get_all_words(positive_cleaned_tokens_list)
    freq_dist_pos = FreqDist(all_pos_words)
    #print(freq_dist_pos.most_common(10))

    positive_tokens_for_model = get_tweets_for_model(positive_cleaned_tokens_list)
    negative_tokens_for_model = get_tweets_for_model(negative_cleaned_tokens_list)

    positive_dataset = [(tweet_dict, "Хорошая новость") for tweet_dict in positive_tokens_for_model]
    negative_dataset = [(tweet_dict, "Плохая новость") for tweet_dict in negative_tokens_for_model]

    dataset = positive_dataset + negative_dataset
    random.shuffle(dataset)

    train_data = dataset[:1]
    test_data = dataset[1:]

    classifier = NaiveBayesClassifier.train(train_data)

    #print("Accuracy is:", classify.accuracy(classifier, test_data))
    #print(classifier.show_most_informative_features(10))

    custom_tokens = remove_noise(word_tokenize(content), top_words)
    return classifier.classify(dict([token, True] for token in custom_tokens))

# Ответ на команду /start
@dp.message_handler(commands=["start"])
async def start_command(message: types.Message):
    await message.reply("Привет! Введи /news чтобы узнать последние новости Волжского")

# Ответ на команду /news
@dp.message_handler(lambda message: 'news' in message.text.lower(), content_types=['text'])
async def get_news(message: types.Message):
    global count
    # Запрос данных из коллекции (просто для демонстрации)
    res = await get_latest_news(0, 0)
    count = 0
    #print(persons)
    image = image_Rain
    for news_item in res:
        #id_, url, title, date, content = news_item
        news_text = text(f"<b>{news_item['title']}</b>\nДата: {news_item['date']}\nСсылка: {news_item['link']}\n\n{news_item['text']}")
        persons, places = await parse.parse(news_text)
        persons = list(set(persons))
        places = list(set(places))
        outTon = await get_tone(news_item['text'])
        summ = await summarise(news_item['text'])
        rew = await rewriter(news_item['text'])
        await bot.send_message(message.chat.id, news_text[:4090], parse_mode=types.ParseMode.HTML)
        if (places):
           text_places = '\n'.join(map(lambda x: "Достопримечательность: " + x, places))
           await bot.send_message(message.chat.id, text_places, parse_mode=types.ParseMode.HTML)
        else:
           await bot.send_message(message.chat.id, "Нет упоминания мест", parse_mode=types.ParseMode.HTML)
        if (persons):
           text_persons = '\n'.join(map(lambda x: "VIP персона: " + x, persons))
           await bot.send_message(message.chat.id, text_persons, parse_mode=types.ParseMode.HTML)
           await bot.send_message(message.chat.id, outTon, parse_mode=types.ParseMode.HTML)
        else:
           await bot.send_message(message.chat.id, "Нет упоминания персон", parse_mode=types.ParseMode.HTML)
        await bot.send_message(message.chat.id, "Аннотация: " + summ, parse_mode=types.ParseMode.HTML)
        await bot.send_message(message.chat.id, "Пересказ: " + rew, parse_mode=types.ParseMode.HTML)
    # Кнопка "Загрузить еще 10 новостей"
    keyboard = InlineKeyboardMarkup(row_width=1)
    button = InlineKeyboardButton("Загрузить последние новости", callback_data='load_more')
    keyboard.add(button)
    await bot.send_message(message.chat.id, "Выберите действие:", reply_markup=keyboard)



async def get_ten_urds(link):
    response = requests.get(link)
    soup = BeautifulSoup(response.content, "html.parser")
    dates_list = []
    urls_list = []
    namings_list = []

    # забираем даты
    dates = soup.select("main > div[class='holder'] > ul > li > span")
    # челы вместо дат ставят "сегодня" и "вчера", надо отфильтровать. Форматирую чтобы добавить потом в бд в формате даты
    for date in dates:
        if "сегодня" in date.text:
            dates_list.append(datetime.date.today().strftime("%Y-%m-%d"))
        elif "вчера" in date.text:
            _ = datetime.date.today() - datetime.timedelta(days=1)
            dates_list.append(_.strftime("%Y-%m-%d"))
        else:
            _ = datetime.datetime.strptime(date.text.strip(), "%d.%m.%Y")
            dates_list.append(_.strftime("%Y-%m-%d"))

    # находим все элементы ссылки, у которых указан url в main элементе сайта
    hyperlinks = soup.select("main > div[class='holder'] > ul > li > a[href]")
    for hyperlink in hyperlinks:
        # забираем ссылку
        href = hyperlink['href']
        if "/news" in href and not "#comments" in href and not href.endswith("/") and not href in urls_list:
            urls_list.append(href)
            namings_list.append(hyperlink.text)

    # добавляем путь сайта к ссылке
    for i in range(len(urls_list)):
        urls_list[i] = "https://bloknot-volzhsky.ru" + urls_list[i]

    return urls_list, namings_list, dates_list


async def get_text(link):
    while True:
        # забираем данные по ссылке на новость
        response = requests.get(link)
        soup = BeautifulSoup(response.content, "html.parser")

        # Проверяем, найден ли элемент с классом 'news-text'
        news_text_element = soup.find('div', class_='news-text')
        if news_text_element:
            # Если элемент найден, вызываем getText
            news_text = news_text_element.getText(' ', strip=True)
            break
        else:
            print("Элемент не найден. Повторяем запрос.")

    # убираем все ненужные элементы (чтобы текст новости заканчивался перед автором новости и инфой о фото)
    _ = list(news_text)
    a = len(_) - 1
    while a >= 0 and _[a] != ".":
        _.pop()
        a -= 1

    # Проверяем, что список не пустой
    if _:
        news_text = ''.join(_)
    else:
        # Если список пуст, присваиваем news_text строку до первой точки (включительно)
        news_text = news_text.split('.', 1)[0]

    # почему-то оно не может декодировать пробелы время от времени
    return news_text.replace("\xa0", " ")


async def updateNews():
    urls_final = []
    namings = []
    dates = []
    text_within_links = []
    iter_count = 1

    # поиск ссылок, названий и дат (изначальное заполнение бд циклом, потом будем гонять while пока не дойдет до ссылки которая есть в бд)
    for i in range(2):
        start_url = f"https://bloknot-volzhsky.ru/?PAGEN_1={i + 1}"
        urls_buffer, namings_buffer, dates_buffer = await get_ten_urds(start_url)
        if (len(namings_buffer) == len(dates_buffer)):
            urls_final.extend(urls_buffer)
            namings.extend(namings_buffer)
            dates.extend(dates_buffer)
            print(len(urls_final),"ссылок ", len(namings),"заголовков ", len(dates),"дат")
        else:
            print("Отсутствуют некоторые данные, переход на следующую страницу")

    # отбор текста
    for url in urls_final:
        text_within_links.append(await get_text(url))
        print(f"Запись новости N{iter_count}")
        iter_count += 1
        time.sleep(0.1)

    # записываем всё в csv файл
    datadict = {'urldata': urls_final, 'namingsdata': namings, 'datesdata': dates, 'textdata': text_within_links}
    datacsv = pandas.DataFrame(datadict)
    datacsv.to_csv("file.csv", index=False)

    # Подключение к MongoDB
    client = pymongo.MongoClient("mongodb://localhost:27017/")

    # Создание базы данных
    database_name = "news_database"
    db = client[database_name]

    # Создание коллекции
    collection_name = "news_collection"
    collection = db[collection_name]

    # Путь к CSV файлу
    csv_file_path = "file.csv"

    with open(csv_file_path, 'r', encoding='utf-8') as csv_file:
        # Создаем объект чтения CSV
        csv_reader = csv.reader(csv_file)

        # Пропускаем заголовок, если он есть
        next(csv_reader, None)
        count = 0
        first_url = ""
        # Читаем и вставляем данные в базу данных
        for row in csv_reader:
            if not count:
               url, title, date_str, text = row
               first_url = url
               count += 1
            url, title, date_str, text = row

            # Проверяем, существует ли новость с таким URL
            if (collection.count_documents({"link": url}) < 1):

                # Создаем объект новости
                news_item = {
                    "link": url,
                    "title": title,
                    "date": date_str,
                    "text": text
                }

                # Вставляем новость в коллекцию
                collection.insert_one(news_item)

            else:
                print(f"Новость с URL {url} уже существует в базе данных. Прекращаем добавление новостей.")
                return url, first_url
                break

@dp.callback_query_handler(lambda callback_query: callback_query.data == 'load_more')
async def load_more_news(callback_query: types.CallbackQuery):
    await bot.answer_callback_query(callback_query.id)
    url_last, url_first = await updateNews()
    time.sleep(5)
    res = await get_latest_news(url_last, url_first)
    for news_item in res:
        #id_, url, title, date, content = news_item
        news_text = text(f"<b>{news_item['title']}</b>\nДата: {news_item['date']}\nСсылка: {news_item['link']}\n\n{news_item['text']}")
        persons, places = await parse.parse(news_text)
        persons = list(set(persons))
        places = list(set(places))
        summ = await summarise(news_item['text'])
        rew = await rewriter(news_item['text'])
        outTon = await get_tone(news_item['text'])
        await bot.send_message(callback_query.from_user.id, news_text[:4090], parse_mode=types.ParseMode.HTML)
        if (places):
           text_places = '\n'.join(map(lambda x: "Достопримечательность: " + x, places))
           await bot.send_message(callback_query.from_user.id, text_places, parse_mode=types.ParseMode.HTML)
        else:
           await bot.send_message(callback_query.from_user.id, "Нет упоминания мест", parse_mode=types.ParseMode.HTML)
        if (persons):
           text_persons = '\n'.join(map(lambda x: "VIP персона: " + x, persons))
           await bot.send_message(callback_query.from_user.id, text_persons, parse_mode=types.ParseMode.HTML)
           await bot.send_message(callback_query.from_user.id, outTon, parse_mode=types.ParseMode.HTML)
        else:
           await bot.send_message(callback_query.from_user.id, "Нет упоминания персон", parse_mode=types.ParseMode.HTML) 
        #await bot.send_message(callback_query.from_user.id, outTon, parse_mode=types.ParseMode.HTML)
        await bot.send_message(callback_query.from_user.id, "Аннотация: " + summ, parse_mode=types.ParseMode.HTML)
        await bot.send_message(callback_query.from_user.id, "Пересказ: " + rew, parse_mode=types.ParseMode.HTML)       
#await bot.send_message(callback_query.from_user.id, news_text, parse_mode=types.ParseMode.HTML)
     
    # Кнопка "Загрузить еще 10 новостей"
    keyboard = InlineKeyboardMarkup(row_width=1)
    button = InlineKeyboardButton("Загрузить последние новости", callback_data='load_more')
    keyboard.add(button)
    await bot.send_message(callback_query.from_user.id, "Выберите действие:", reply_markup=keyboard)

async def sinWords(input_word):
    # Инициализация SparkSession
    spark = SparkSession.builder.appName("Word2VecExample").getOrCreate()
    
    # Подключение к MongoDB с использованием pymongo
    mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")
    mongo_db = mongo_client["news_database"]
    mongo_collection = mongo_db["news_collection"]
    
    # Загрузка данных из MongoDB с использованием pymongo
    mongo_data = mongo_collection.find()
    data = [entry['text'] for entry in mongo_data]
    print(data)
    # Создание Spark DataFrame
    df = spark.createDataFrame([(text,) for text in data], ["text"])
    
    # Токенизация текста
    tokenizer = Tokenizer(inputCol="text", outputCol="words")
    words_df = tokenizer.transform(df)
    
    # Обучение модели Word2Vec
    word2Vec = Word2Vec(vectorSize=100, minCount=5, inputCol="words", outputCol="result")
    pipeline = Pipeline(stages=[tokenizer, word2Vec])
    
    # Обучаем модель
    model = pipeline.fit(df)
    result = model.transform(df)
    
    # Введенное слово для поиска синонимов
    #input_word = "бочаров"
    
    # Находим синонимы в модели Word2Vec
    try:
       # Находим синонимы в модели Word2Vec
       synonyms = model.stages[1].findSynonyms(input_word, 5)
       # Преобразуем синонимы в список
       synonyms_list = [row['word'] for row in synonyms.collect()]
       # Выводим результат
       print(f"Синонимы для слова '{input_word}':")
       return synonyms_list
    except Exception as e:
       if "not in vocabulary" in str(e):
          return None
    else:
        raise e

@dp.message_handler(lambda message: 'sin' in message.text.lower(), content_types=['text'])
async def get_sinWord(message: types.Message):
    #print(message.text[5:])
    sinWordUser = await sinWords(message.text[5:].lower())
    if (sinWordUser):
       text_representation = '\n'.join(sinWordUser)
       await bot.send_message(message.chat.id, str(text_representation), parse_mode=types.ParseMode.HTML)
    else:
       await bot.send_message(message.chat.id, "Синонимы не найдены.", parse_mode=types.ParseMode.HTML)
    #print(sinWordsUser)

if __name__ == '__main__':
    executor.start_polling(dp)
