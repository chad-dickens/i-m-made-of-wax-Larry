"""Scrapes player data from NRL website for all clubs"""

import sqlite3
import re
from urllib.request import Request, urlopen
from bs4 import BeautifulSoup
from selenium.webdriver import Chrome
from selenium.webdriver.chrome.options import Options


def get_nrl_players_pages():
    """
    Will return a dictionary with keys that are all the clubs in the NRL.
    The corresponding values will be the url of that clubs players on the NRL website.
    """
    opts = Options()
    opts.headless = True
    browser = Chrome(options=opts)
    team_dict = {}
    cnt = 0

    while True:
        browser.get('https://www.nrl.com/players/')
        browser.find_element_by_xpath("//button[@class='filter-dropdown-button' and "
                                      "@aria-describedby='team-filter-button']").click()
        tally = cnt
        for team_name in browser.find_elements_by_xpath(
                "//li[contains(@class, 'filter-dropdown-item--team')]"):
            if team_name.text != 'All teams' and team_name.text not in team_dict:
                save_name = team_name.text
                team_name.click()
                browser.get(browser.current_url)
                player_links = [x.get_attribute('href') for x in browser
                    .find_elements_by_xpath("//a[contains(@class, 'card-themed-hero-profile')]")]

                team_dict[save_name] = tuple(player_links)
                cnt += 1
                break

        if tally == cnt:
            break

    browser.quit()
    return team_dict


def player_info(player_link):
    """
    Returns the information of a player based off of their link using BeautifulSoup
    """
    req = Request(player_link, headers={'User-Agent': 'Mozilla/5.0'})
    html = urlopen(req)
    soup = BeautifulSoup(html, "lxml", multi_valued_attributes=None)

    for num, item in enumerate(soup.find('h1').find_all('span')):
        if num == 0:
            name = item.text
        else:
            name += ' ' + item.text

    player_dict = {'Name': name, 'Position':
                   soup.find('p', class_='club-card__position').text.strip()}

    for charac in ('Height', 'Weight', 'Date of Birth', 'Birthplace', 'Debut Club',
                   'Date', 'Appearances', 'Tries', 'Previous Club', 'Junior Club'):
        try:
            player_dict[charac] = soup.find('dt', text=re.compile(charac))\
                .find_next_sibling().text.strip()
        except:
            player_dict[charac] = None

    return player_dict


def get_club_names():
    """
    Will return a tuple containing the full names for all of the teams in the NRL
    :return: A tuple
    """
    req = Request('https://www.nrl.com/clubs/', headers={'User-Agent': 'Mozilla/5.0'})
    html = urlopen(req)
    soup = BeautifulSoup(html, "lxml", multi_valued_attributes=None)

    my_list = []
    for club in soup.find_all('h2', class_='club-card__title h4'):
        if club.text.strip() == 'Warriors':
            my_list.append('New Zealand Warriors')
        else:
            my_list.append(club.text.strip().replace('\r\n', ' '))

    return tuple(my_list)


def main():
    """
    Main procedure. Opens up a sqlite connection and creates a new table to store the scraped data
    :return: None
    """
    conn = sqlite3.connect('rugby.sqlite')
    cur = conn.cursor()

    cur.execute('DROP TABLE IF EXISTS nrl')

    cur.execute('CREATE TABLE nrl ('
                'id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE, '
                'name TEXT, '
                'position TEXT, '
                'height TEXT, '
                'weight TEXT, '
                'dob TEXT, '
                'birth_place TEXT, '
                'debut_club TEXT, '
                'debut_date TEXT, '
                'previous_club TEXT, '
                'junior_club TEXT, '
                'career_appearances TEXT, '
                'career_tries TEXT, '
                'current_team TEXT)')

    # Getting the player links for each team
    master_dict = get_nrl_players_pages()

    # Storing club names
    club_names = get_club_names()

    for key in master_dict.keys():

        club_name = 0
        for club in club_names:
            if key in club:
                club_name = club
                break

        for player in master_dict[key]:
            info = player_info(player)

            # Inserting into players table
            cur.execute('INSERT INTO nrl ('
                        'name, '
                        'position, '
                        'height, '
                        'weight, '
                        'dob, '
                        'birth_place, '
                        'debut_club, '
                        'debut_date, '
                        'previous_club, '
                        'junior_club, '
                        'career_appearances, '
                        'career_tries, '
                        'current_team) '
                        'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                        (info['Name'], info['Position'], info['Height'], info['Weight'],
                         info['Date of Birth'], info['Birthplace'], info['Debut Club'],
                         info['Date'], info['Previous Club'], info['Junior Club'],
                         info['Appearances'], info['Tries'], club_name))

            conn.commit()

    cur.close()
    conn.close()


if __name__ == '__main__':
    main()
