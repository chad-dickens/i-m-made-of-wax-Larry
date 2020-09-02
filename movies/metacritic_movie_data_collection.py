"""
Scrapes movie data from the Metacritic website, creates a sqllite
database and inserts the data into it
"""

import sqlite3
import urllib
import re
from urllib.request import Request, urlopen
from bs4 import BeautifulSoup


def return_metacritic(url):
    """
    Scrapes a metacritic movie page and returns 9 pieces of information from it
    :param url: The metacritic website url for a specific movie
    :return: A tuple containing nine elements
    """
    req = Request(url, headers={'User-Agent': 'Mozilla/5.0'})
    html = urlopen(req)
    soup = BeautifulSoup(html, "lxml", multi_valued_attributes=None)

    main_content = soup.find('div', class_=re.compile("phead_summary"))

    # Title
    movie_title = main_content.find('div', class_=re.compile("pad_btm1")).div.h1.text

    # Year
    movie_year = int(main_content.find('div', class_=re.compile("pad_btm1")).span.text)

    # Metascore
    metascore = int(main_content.find('div', class_='ms_wrapper')
                    .find('a', class_='metascore_anchor').span.text)

    # User Score
    try:
        user_score = float(main_content.find('div', class_='us_wrapper')
                           .find('a', class_='metascore_anchor').span.text)
    except:
        user_score = None

    # Cast
    movie_cast = None
    try:
        for num, name in enumerate(soup.find('div', class_=re.compile('^summary_cast'))
                                   .find('span', attrs={'class': None}).find_all('a')):
            if num == 0:
                movie_cast = name.text
            else:
                movie_cast += ',' + name.text
    except:
        # Skip errors
        pass

    for each in soup.find_all('div', class_='details_section'):
        if each.find('div', class_='director'):
            detail_section = each
            break

    # Director
    director = detail_section.find('div', class_='director').a.span.text

    # Genre
    genre = None
    for num, cat in enumerate(detail_section.find('div', class_='genres')
                              .find('span', attrs={'class': None}).find_all('span')):
        if num == 0:
            genre = cat.text
        else:
            genre += ',' + cat.text

    # Age rating
    try:
        age_rating = detail_section.find('div', class_='rating')\
                    .find('span', attrs={'class': None}).text.strip()
    except:
        age_rating = None

    # Run time
    run_time = detail_section.find('div', class_='runtime')\
        .find('span', attrs={'class': None}).text
    run_time = int(re.search('[0-9]+', run_time).group())

    return (movie_title, movie_year, metascore, user_score, movie_cast, director, genre,
            age_rating, run_time)


def movie_data():
    """
    A generator that returns each line of movie data to go into the database.
    :return: A tuple
    """
    for page in range(0, 133):
        print('Page: {}'.format(page))
        root_url = 'https://www.metacritic.com'
        url = '{}/browse/movies/score/metascore/all/filtered?page={}'.format(root_url, page)
        req = Request(url, headers={'User-Agent': 'Mozilla/5.0'})

        while True:
            try:
                html = urlopen(req)
                break
            except urllib.error.HTTPError as err:
                print(url, 'HTTP Error', err)

        soup = BeautifulSoup(html, "lxml", multi_valued_attributes=None)

        for link in soup.find_all('a', class_='title', href=re.compile('^/movie/')):
            # This variable is to store the number of times a link has been attempted
            cnt = 0
            while True:
                try:
                    cnt += 1
                    yield return_metacritic(root_url + link['href'])
                    break
                except urllib.error.HTTPError:
                    if cnt > 5:
                        print('Too many HTTP errors for {}'.format(link['href']))
                        break
                except Exception as err:
                    print(err, link['href'])
                    break


def main():
    """
    Main procedure. Opens up a sqllite connection and creates a new table to store the scraped data
    :return: None
    """
    conn = sqlite3.connect('movies.sqlite')
    cur = conn.cursor()
    cur.execute('DROP TABLE IF EXISTS metacritic')

    cur.execute('CREATE TABLE metacritic ('
                'id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE, '
                'title TEXT, '
                'year INTEGER, '
                'meta_score INTEGER,'
                'user_score REAL,'
                'cast TEXT,'
                'director TEXT,'
                'genre TEXT,'
                'age_rating TEXT,'
                'run_time INTEGER)')

    for line in movie_data():
        cur.execute('INSERT INTO metacritic (title, year, meta_score, user_score, cast, '
                    'director, genre, age_rating, run_time) '
                    'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);',
                    (line[0], line[1], line[2], line[3], line[4], line[5],
                     line[6], line[7], line[8]))
        conn.commit()

    cur.close()
    conn.close()


if __name__ == '__main__':
    main()
