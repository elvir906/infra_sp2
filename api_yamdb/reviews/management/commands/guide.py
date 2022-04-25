"""
Кастомный скрипт следует расположить в директории
management/commands/ в директории любого зарегистрированного
в INSTALLED_APPS приложения. В директориях management и commands
нужно дополнительно создать файлы __init__.py.

Начиная с 3.4 версии появился модуль pathlib,
который позволяет вытворять самые приятные вещи с путями к файлу,
стоит только импортировать его класс Path.
Можно почитать https://webtort.ru/как-задать-путь-к-файлу-в-python/.
"""
from pathlib import Path

# класс для рботы с таблицами csv
import csv

# класс для рботы с таблицами sqlite
import sqlite3

# класс для работы с консольными коммандами manage.py
from django.core.management.base import BaseCommand


class Command(BaseCommand):
    help = 'writing hello words and imorting data from csv to sqlite3'

    """
    Логика работы с кастомными командами для manage.py описывается
    в методе handle класса Command.
    """
    def handle(self, *args, **kwargs):

        # Path.cwd() — возвращает путь к рабочей директории
        dir_path = Path.cwd()

        # присвоили db_sqlite3 путь от рабочей директории к файлу db.sqlite3
        db_sqlite3 = Path(dir_path, 'db.sqlite3',)

        # paths - словарь, где ключи - названия таблиц, а значения - пути до *csv
        # файлов с соответсвующими таблицами. 'static', 'data', здесь
        # типа /static/data/. То есть описывается путь
        # от рабочей папки dir_path до самого файла
        paths = {
            'reviews_category': Path(
                dir_path, 'static', 'data', 'category.csv'
            ),
            'reviews_comments': Path(
                dir_path, 'static', 'data', 'comments.csv'
            ),
            'reviews_genretitle': Path(
                dir_path, 'static', 'data', 'genre_title.csv'
            ),
            'reviews_genre': Path(dir_path, 'static', 'data', 'genre.csv'),
            'reviews_review': Path(dir_path, 'static', 'data', 'reviews.csv'),
            'reviews_title': Path(dir_path, 'static', 'data', 'titles.csv'),
            'users_user': Path(dir_path, 'static', 'data', 'users.csv'),
        }

        # это назавния таблиц в списке
        tables = [
            'reviews_category',
            'reviews_comments',
            'reviews_genretitle',
            'reviews_genre',
            'reviews_review',
            'reviews_title',
            'users_user',
        ]

        # короче, забегая вперед надо сказать, что csv таблицы в скрипте открываются два раза:
        # первый раз - для того, чтобы собрать специальные строки из имен таблиц (из первой строки)
        # второй раз - для того, чтобы непосредственно скопировать данные из csv в sqlite

        # по всем таблицам из списка...
        for table in tables:

            # создаётся сединение connection с базой sqlite3
            connection = sqlite3.connect(db_sqlite3)

            # создаётся курсор для базы sqlite3
            cursor = connection.cursor()

            # открываются (первый раз) файлы для чтения с таблицей CSV, сразу происходит
            # перобразование в формат utf-8.
            # Открывется и задается ему имя wrapper
            with open(paths[table], 'r', encoding='utf-8') as wrapper:

                # нашу таблицу *.csv, которую обозвали wrapper'ом,
                # считывает специальный класс DictReader
                dr = csv.DictReader(wrapper)

                # ниже в цикле собираются строка fieldsline из имен таблиц типа (col1, col2, col3)
                # и строка answer_sign_line типа (?, ?, ?, ?), где количество ? равно
                # количеству столбцов csv таблицы
                fieldsline = ''
                answer_sign_line = ''

                # dr.fieldnames считывает имя столбца из csv таблицы, которая находится в переменной dr
                # тут просто собираются имена через запятую.
                for fields in dr.fieldnames:
                    if fields != dr.fieldnames[-1]:
                        fieldsline += fields + ', '
                        answer_sign_line += '?, '
                    else:
                        fieldsline += fields
                        answer_sign_line += '?'

            # Второй раз открываю csv таблицы непосредственно для копировани
            # данных в таблицу sqlite
            with open(paths[table], 'r', encoding='utf-8') as wrapper:
                reader = csv.DictReader(wrapper)

                # migration_to_db - переменная,куда помещаются данныеиз csv таблиц
                # она представляет собой список, который собирается из кортежей и компрехеншена (цикла) 
                # чтобы вот такой список получился [(i['col1'], i['col2']) for i in reader], где i == имя таблицы
                # самая сложная часть для понимания
                migrations_to_db = [
                    tuple(
                        i[fields] for fields in reader.fieldnames
                    ) for i in reader
                ]

            # а это уже sql команда
            # типа ("INSERT INTO t (col1, col2) VALUES (?, ?);", to_db)
            # которая переносит файлы в таблицу из переменной migrations_to_db
            cursor.executemany(
                f'INSERT INTO {table} ({fieldsline}) '
                f'VALUES ({answer_sign_line});',
                migrations_to_db
            )

            # сохраняем, закрываем
            connection.commit()
            connection.close()

        # в случае успеха, выводим сообщение в консоль.
        self.stdout.write(
            'Успешно! Данные из *.csv теперь в базе.'
        )
