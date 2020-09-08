"""Scrapes player data from Rugby AU website for all clubs"""

import sqlite3
import urllib
from urllib.request import Request, urlopen
from bs4 import BeautifulSoup


def get_players_pages():
    """
    Will return a dictionary with keys that are all the clubs in Super Rugby AU.
    The corresponding values will be the url of that club's players.
    """
    root_link = 'https://www.rugby.com.au'
    req = Request(root_link + '/competitions/super%20rugby%20au',
                  headers={'User-Agent': 'Mozilla/5.0'})
    html = urlopen(req)
    soup = BeautifulSoup(html, "lxml", multi_valued_attributes=None)

    # Getting team names and links
    teams_section = soup.find('div', id='teams')
    team_links = tuple([link.get('href') for link in teams_section.find_all('a', href=True)])
    team_names = tuple([link.find('span', class_='link-text').text for link in
                        teams_section.find_all('a', href=True)])

    # Looping through the team links
    team_dict = {}
    for num, link in enumerate(team_links):
        req = Request(root_link + link, headers={'User-Agent': 'Mozilla/5.0'})
        html = urlopen(req)
        soup = BeautifulSoup(html, "lxml", multi_valued_attributes=None)

        player_section = soup.find('div', id='players')
        player_links = tuple([player.get('href') for player in
                              player_section.find_all('a', href=True)])

        team_dict[team_names[num]] = player_links

    return team_dict


def player_info(player_link):
    """
    Returns the information of a player based off of their link using BeautifulSoup
    """
    root_link = 'https://www.rugby.com.au'
    req = Request(root_link + player_link, headers={'User-Agent': 'Mozilla/5.0'})
    html = urlopen(req)
    soup = BeautifulSoup(html, "lxml", multi_valued_attributes=None)

    player_dict = {'Name': soup.find('h1').text, 'Height': None, 'Weight': None,
                   'Position': None, 'Date of birth': None}

    table_info = soup.find('table', class_='table player-details__table')

    for row in table_info.find_all('tr'):
        player_dict[row.th.text] = row.td.text

    return player_dict


def main():
    """
    Main procedure. Opens up a sqlite connection and creates a new table to store the scraped data
    """

    conn = sqlite3.connect('rugby.sqlite')
    cur = conn.cursor()

    cur.execute('DROP TABLE IF EXISTS super_rugby_au')

    cur.execute('CREATE TABLE super_rugby_au ('
                'id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE, '
                'name TEXT, '
                'position TEXT, '
                'height TEXT, '
                'weight TEXT, '
                'dob TEXT, '
                'club TEXT)')

    # Getting the player links for each team
    master_dict = get_players_pages()

    # Iterating through the dictionary
    for club, links in master_dict.items():
        for player_link in links:

            # If a HTTP error occurs, attempting the link 5 times
            cnt = 0
            while True:
                try:
                    info = player_info(player_link)
                    break
                except urllib.error.HTTPError:
                    cnt += 1

                    if cnt > 4:
                        print('Too many HTTP errors for {}'.format(player_link))
                        break

                except Exception as err:
                    print('Something unexpected happened with {}. The error was {}'
                          .format(player_link, err))
                    cnt = 5
                    break

            if cnt > 4:
                continue

            # Inserting into sqlite
            cur.execute('INSERT INTO super_rugby_au ('
                        'name, '
                        'position, '
                        'height, '
                        'weight, '
                        'dob, '
                        'club) '
                        'VALUES (?, ?, ?, ?, ?, ?)',
                        (info['Name'], info['Position'], info['Height'], info['Weight'],
                         info['Date of birth'], club))

            conn.commit()

    cur.close()
    conn.close()


if __name__ == '__main__':
    main()
