"""
Scapes movie data from IMDB website, creates a sqllite database and inserts the data into it
"""

import sqlite3
import urllib.request
import re
from bs4 import BeautifulSoup


def movie_data():
    """
    A generator for looping through the IMDB top 1000 movies website. Will generate a list
    containing 11 elements for each movie of the 1000. The 11 elements will contain all of
    the details of the movie. Uses Beautiful soup to parse the html.
    :return: a list.
    """
    for page in range(1, 1001, 100):
        html = urllib.request.urlopen('https://www.imdb.com/search/title/?groups=top_1000&'
                                      'sort=user_rating,desc&count=100&start={}&ref_=adv_nxt'
                                      .format(page)).read()
        soup = BeautifulSoup(html, "lxml", multi_valued_attributes=None)

        for i in soup.find_all('div', class_='lister-item mode-advanced'):

            content = i.find('div', class_='lister-item-content')
            movie_title = content.h3.a.text
            movie_year = content.h3.find('span', class_=re.compile('year')).text
            movie_year = int(re.search('[0-9]{4}', movie_year).group())

            try:
                age_rating = content.p.find('span', class_='certificate').text
            except:
                age_rating = None

            run_time = content.p.find('span', class_='runtime').text
            run_time = int(re.search('[0-9]+', run_time).group())
            movie_genre = content.p.find('span', class_='genre').text

            imdb_score = float(content.div.div['data-value'])
            try:
                metascore = int(content.div.find('div', class_=re.compile('.+metascore$'))
                                .span.text)
            except:
                metascore = None

            movie_director = None
            movie_cast = None

            for num, anchor in enumerate(content.find_all('p')[2].find_all('a')):
                if num == 0:
                    movie_director = anchor.text
                elif num == 1:
                    movie_cast = anchor.text
                else:
                    movie_cast += ',' + anchor.text

            movie_votes = None
            movie_gross = None
            num = 0
            for tag in content.find('p', class_=re.compile('^sort-num')).find_all('span'):
                if tag.has_attr('name') and tag['name'] == 'nv':
                    value_num = int(tag['data-value'].replace(',', ''))

                    if num == 0:
                        movie_votes = value_num
                    elif num == 1:
                        movie_gross = value_num
                    num += 1

            yield [movie_title, movie_year, age_rating, run_time, movie_genre, imdb_score,
                   metascore, movie_director, movie_cast, movie_votes, movie_gross]


def main():
    """
    Main procedure. Opens up a sqllite connection and creates a new table to store the scraped data
    :return: None
    """
    conn = sqlite3.connect('movies.sqlite')
    cur = conn.cursor()
    cur.execute('DROP TABLE IF EXISTS imdb')

    cur.execute('CREATE TABLE imdb ('
                'id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE, '
                'title TEXT, '
                'year INTEGER, '
                'age_rating TEXT,'
                'run_time INTEGER,'
                'genre TEXT,'
                'imdb_score REAL,'
                'metascore INTEGER,'
                'director TEXT,'
                'cast TEXT,'
                'votes INTEGER,'
                'gross INTEGER)')

    for line in movie_data():
        cur.execute('INSERT INTO imdb (title, year, age_rating, run_time, genre, imdb_score, '
                    'metascore, director, cast, votes, gross) '
                    'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);',
                    (line[0], line[1], line[2], line[3], line[4], line[5],
                     line[6], line[7], line[8], line[9], line[10]))

    cur.close()
    conn.commit()
    conn.close()


if __name__ == '__main__':
    main()
