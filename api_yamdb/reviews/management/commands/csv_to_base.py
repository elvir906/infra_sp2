from pathlib import Path
import csv
import sqlite3

from django.core.management.base import BaseCommand


class Command(BaseCommand):
    help = 'writing hello words and imorting data from csv to sqlite3'

    def handle(self, *args, **kwargs):
        dir_path = Path.cwd()
        db_sqlite3 = Path(dir_path, 'db.sqlite3',)
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
        tables = [
            'reviews_category',
            'reviews_comments',
            'reviews_genretitle',
            'reviews_genre',
            'reviews_review',
            'reviews_title',
            'users_user',
        ]
        for table in tables:
            connection = sqlite3.connect(db_sqlite3)
            cursor = connection.cursor()
            with open(paths[table], 'r', encoding='utf-8') as wrapper:
                dr = csv.DictReader(wrapper)
                fieldsline = ''
                answer_sign_line = ''
                for fields in dr.fieldnames:
                    if fields != dr.fieldnames[-1]:
                        fieldsline += fields + ', '
                        answer_sign_line += '?, '
                    else:
                        fieldsline += fields
                        answer_sign_line += '?'
            with open(paths[table], 'r', encoding='utf-8') as wrapper:
                reader = csv.DictReader(wrapper)
                migrations_to_db = [
                    tuple(
                        i[fields] for fields in reader.fieldnames
                    ) for i in reader
                ]
            cursor.executemany(
                f'INSERT INTO {table} ({fieldsline}) '
                f'VALUES ({answer_sign_line});',
                migrations_to_db
            )
            connection.commit()
            connection.close()
        self.stdout.write(
            'Успешно! Данные из *.csv теперь в базе.'
        )
