"""
FIFA World Cup data scrapper

     Author: Daniel Vazquez

Description: This program scraps online tables and data related to the
             rosters, teams, fixtures, and main stats from the FIFA World Cup
             tournaments from 2002 to 2018. Rosters and groups information
             are scrapped from the tournament's respective entry in Wikipedia,
             while match results are scrapped from many other sources.The
             program follows the following procedure:

             1. Access the html code for the websites to scrap using the
                BeautifulSoup libraries.

             2. Scrap the html code looking for the following information:

                a. Team groups
                b. Participating countries
                c. Players' names
                d. Players' birthdays
                e. Match locations
                f. Match dates
                g. Match results
                h. Match home and away sides
                i. Match groups and stages.

             3. Save all scrapped dataframes to csv files.

      To do: > Finish '02 match scrapper
               - Scrap group stage data
               - Format 'Stage' field
               - Scrap penalty kick scores
             > Scrap match results for the '06, '10, and '14 tournaments.

"""

# -----------------------------------------------------------------------------

# Import libraries
from urllib import response
from bs4 import BeautifulSoup as bs
import requests
import pandas as pd
import itertools as itl

# Webpages to scrap
wc2018 = 'https://en.wikipedia.org/wiki/2018_FIFA_World_Cup_squads'
wc2014 = 'https://en.wikipedia.org/wiki/2014_FIFA_World_Cup_squads'
wc2010 = 'https://en.wikipedia.org/wiki/2010_FIFA_World_Cup_squads'
wc2006 = 'https://en.wikipedia.org/wiki/2006_FIFA_World_Cup_squads'
wc2002 = 'https://en.wikipedia.org/wiki/2002_FIFA_World_Cup_squads'
results2002 = 'https://en.wikipedia.org/wiki/2002_FIFA_World_Cup'
results2018 = 'https://www.foxsports.com/soccer/fifa-world-cup/schedule'

def main(): # -----------------------------------------------------------------
    
    # Define lists to store dataframes
    rosters_ds = []
    groups_ds = []
    matches_ds = []

    # Scrapping Korea/Japan 2002 data ----------------------------------------

    wc = wc2002

    # Obtain html from 2002 rosters' webpage
    page = requests.get(wc)
    soup = bs(page.content, 'html.parser')

    # Obtain html from 2002 results' webpage
    page2 = requests.get(results2002)
    soup2 = bs(page2.content, 'html.parser')

    print('Obtaining 2002 rosters...')
    roster_scrapper(soup, wc, rosters_ds)
    print('Obtaining 2002 groups...')
    groups_scrapper(soup, wc, groups_ds)
    print('Obtaining 2002 fixtures...')
    matches_scrapper_2002(soup2, matches_ds)

    # Scrapping Germany 2006 data ---------------------------------------------

    wc = wc2006

    # Obtain html from 2006 rosters' webpage
    page = requests.get(wc)
    soup = bs(page.content, 'html.parser')

    print('Obtaining 2006 rosters...')
    roster_scrapper(soup, wc, rosters_ds)
    print('Obtaining 2006 groups...')
    groups_scrapper(soup, wc, groups_ds)

    # Scrapping South Africa 2010 data ----------------------------------------

    wc = wc2010

    # Obtain html from 2010 rosters' webpage
    page = requests.get(wc)
    soup = bs(page.content, 'html.parser')

    print('Obtaining 2010 rosters...')
    roster_scrapper(soup, wc, rosters_ds)
    print('Obtaining 2010 groups...')
    groups_scrapper(soup, wc, groups_ds)

    # Scrapping Brazil 2014 data ----------------------------------------------

    wc = wc2014

    # Obtain html from 2014 rosters' webpage
    page = requests.get(wc)
    soup = bs(page.content, 'html.parser')

    print('Obtaining 2014 rosters...')
    roster_scrapper(soup, wc, rosters_ds)
    print('Obtaining 2014 groups...')
    groups_scrapper(soup, wc, groups_ds)

    # Scrapping Russia 2018 data ----------------------------------------------

    wc = wc2018

    # Obtain html from rosters' webpage
    page = requests.get(wc)
    soup = bs(page.content, 'html.parser')
    
    # Obtain html from 2018 results' webpage
    page2 = requests.get(results2018)
    soup2 = bs(page2.content, 'html.parser')

    print('Obtaining 2018 rosters...')
    roster_scrapper(soup, wc, rosters_ds)
    print('Obtaining 2018 groups...')
    groups_scrapper(soup, wc, groups_ds)
    print('Obtaining 2018 fixtures...')
    matches_scrapper_2018(soup2, matches_ds)

    # -------------------------------------------------------------------------

    # Combine all df's into one
    rosters_ds = pd.concat(rosters_ds)
    groups_ds = pd.concat(groups_ds)
    matches_ds = pd.concat(matches_ds)
    print('Data sets complete. Saving...')

    # Save full data sets to csv files
    rosters_ds.to_csv(r'csv_files/FIFA_wc_players.csv',
                      index = False, encoding = 'utf-8-sig')
    groups_ds.to_csv(r'csv_files/FIFA_wc_groups.csv',
                     index = False, encoding = 'utf-8-sig')
    matches_ds.to_csv(r'csv_files/FIFA_wc_matches.csv',
                      index = False, encoding = 'utf-8-sig')

    print('Done')

def roster_scrapper(soup, wc, rosters_ds): # ----------------------------------

    # Define lists
    Country = [] # Participating countries
    Birthday = [] # Player's birthday

    # Import participating countries
    countries = soup.findAll('h3')
    # Some webpages have more tables than others. Unnecessary data needs to be
    # removed.
    if wc2018 in wc or wc2010 in wc: 
        countries = countries[:-6]
    elif wc2014 in wc:
        countries = countries[:-5]
    elif wc2002 in wc or wc2006 in wc:
        countries = countries[:-1]
    for i in countries:
        a = i.get_text()
        it = a.replace('[edit]', '')
        Country.append(it)
    
    # Import players' birthdays
    bdays = soup.findAll('span', attrs = {'class':'bday'})
    for i in bdays:
        a = i.get_text()
        Birthday.append(a)

    # # Scrapping player data ---------------------------------------------------

    # Import rosters
    tables = soup.findAll('table', attrs = {'class':'wikitable'})
    rosters = pd.read_html(str(tables)) # Create list of dataframes (df)
    # Some webpages have more tables than others. Unnecessary data needs to be
    # removed.
    if wc2018 in wc or wc2010 in wc:
        rosters = rosters[:-4]
    elif wc2014 in wc:
        rosters = rosters[:-3]
    elif wc2006 in wc:
        rosters = rosters[:-2]
    elif wc2002 in wc:
        rosters = rosters[:-1]
    df = pd.concat(rosters) # Merge all df's in 'rosters' list into one df

    # Update 'Date of Birth' field from df, since it appears as NaN in df
    df = df.rename(columns = {'Date of birth (age)': 'Date of Birth'})
    df['Date of Birth'] = Birthday
    
    # Add torunament to every players' entry
    if wc2002 in wc:
        tournament = ['Korea/Japan 2002']
    elif wc2006 in wc:
        tournament = ['Germany 2006']
    elif wc2010 in wc:
        tournament = ['South Africa 2010']
    elif wc2014 in wc:
        tournament = ['Brazil 2014']
    elif wc2018 in wc:
        tournament = ['Russia 2018']
    tournament = list(itl.chain.from_iterable(itl.repeat(i, 736) for i in tournament))

    # 23 players per team, so items in 'Country' have to be repeated 23 times
    Country = list(itl.chain.from_iterable(itl.repeat(i, 23) for i in Country))

    df['Tournament'] = tournament # Add 'Tournament' column to df
    df['Country'] = Country # Add 'Country' column to df
    if wc2018 in wc: # Drop unnecessary column from 2018 df
        df.drop('Goals', axis = 1, inplace = True)
    else:
        pass
    df = df.iloc[:, [6,7,0,1,2,3,4,5]] # Fix column order

    print('Rosters obtained...')
    rosters_ds.append(df)

def groups_scrapper(soup, wc, groups_ds): # -----------------------------------

    # Define lists
    Group = []
    Country = []

    # Import groups
    # Some webpages have more tables than others. Unnecessary data needs to be
    # removed.
    groups = soup.findAll('h2')
    if wc2002 in wc:
        groups = groups[1:-3]
    elif wc2006 in wc:
        groups = groups[1:-4]
    else:
        groups = groups[1:-5]
    for i in groups:
        a = i.get_text()
        it = a.replace('[edit]', '')
        Group.append(it)

    # Populating 'Country'
    countries = soup.findAll('h3')
    # Some webpages have more tables than others. Unnecessary data needs to be
    # removed.
    if wc2018 in wc or wc2010 in wc: 
        countries = countries[:-6]
    elif wc2014 in wc:
        countries = countries[:-5]
    elif wc2002 in wc or wc2006 in wc:
        countries = countries[:-1]
    for i in countries:
        a = i.get_text()
        it = a.replace('[edit]', '')
        Country.append(it)

    # 4 teams per group, so items in 'Group' have to be repeated 4 times
    Group = list(itl.chain.from_iterable(itl.repeat(i, 4) for i in Group))
    
    if wc2002 in wc:
        tournament = ['Korea/Japan 2002']
    elif wc2006 in wc:
        tournament = ['Germany 2006']
    elif wc2010 in wc:
        tournament = ['South Africa 2010']
    elif wc2014 in wc:
        tournament = ['Brazil 2014']
    elif wc2018 in wc:
        tournament = ['Russia 2018']
    tournament = list(itl.chain.from_iterable(itl.repeat(i, 32) for i in tournament))

    # Dictionary with groups and teams
    df = {
        'Tournament': tournament,
        'Group': Group,
        'Teams': Country
    }
    
    df = pd.DataFrame.from_dict(df)
    groups_ds.append(df)
    print('Groups obtained...')

def matches_scrapper_2018(soup2, matches_ds): # -------------------------------

    # Define lists
    Location = []
    Date = []
    Result = []
    Home = []
    Away = []
    Stage = []
    Group = []

    # Import match locations
    table = soup2.findAll('div', attrs = {'class': 'wc-schedule-location'})
    for i in table:
        Location.append(i.get_text())

    # Import match dates
    raw_dates = []
    table = soup2.findAll('div', attrs = {'class': 'wc-schedule-game-details'})
    for i in table:
        raw_dates.append(i.findAll('div', attrs = {'class': None}))
    # Clean up raw_dates for Date field, which was obtained as a nested list
    for i in raw_dates:
        for j in i:
            it = j.get_text()
            Date.append(it)

    # Get match results
    table = soup2.findAll('a', attrs = {'href': '#'})
    table = table[8:]
    for i in table:
        Result.append(i.get_text().strip())

    # Get home and away sides
    table = soup2.findAll('div', attrs = {'class': 'wc-team-name'})
    cnt = 0
    for i in table:
        if cnt % 2 == 0:
            Home.append(i.get_text())
            cnt += 1
        else:
            Away.append(i.get_text())
            cnt += 1

    # Obtain match groups and stages
    table = soup2.findAll('div', attrs = {'class': 'wc-schedule-group'})
    for i in table:
        Group.append(i.get_text())
        Stage.append(i.get_text())
    for i in range(len(Group)):
        if i < 48:
            Stage[i] = 'Group stage'
        else:
            Group[i] = 'Knockout stage'


    wc = ['Russia 2018']
    wc = list(itl.chain.from_iterable(itl.repeat(i, 64) for i in wc))

    df = {
        'Tournament': wc,
        'Stage': Stage,
        'Group': Group,
        'Date': Date,
        'Home': Home,
        'Result': Result,
        'Away': Away,
        'Location': Location
    }
    
    # Turn dictionary into df
    df = pd.DataFrame.from_dict(df)
    matches_ds.append(df)
    print('Matches obtained...')

# Work zone
def matches_scrapper_2002(soup2, matches_ds): # -------------------------------

    # Define lists
    Date = []
    Home = []
    Away = []
    Result = []
    Location = []
    Stage = []
    Group = []

    # tables = soup2.find_all('table', attrs = {'':'width:100%'})
    # matches = pd.read_html(str(tables))

    # print(matches[0])

    # Importing knockout round dates
    dates = soup2.findAll('div', attrs = {'class':'fdate'})
    for i in dates:
        Date.append(i.get_text())

    # Importing knockout round home and away sides
    teams = []
    teams = soup2.findAll('span', attrs = {'itemprop':'name'})
    cnt = 0
    for i in teams:
        if cnt % 2 == 0:
            Home.append(i.get_text(strip = True))
            cnt += 1
        else:
            Away.append(i.get_text(strip = True))
            cnt += 1

    # Importing knockout round results
    results = soup2.findAll('th', attrs = {'class':'fscore'})
    for i in results:
        Result.append(i.get_text(strip = True))

    # Importing knockout round locations
    loc = soup2.findAll('div', attrs = {'itemprop':'location'})
    for i in loc:
        Location.append(i.get_text(strip = True))

    # Importing stage
    st = soup2.findAll('span', attrs = {'class':'mw-headline'})
    mover = [st[7],st[17],st[18],st[19],st[20],st[21]]
    for i in mover:
        Stage.append(i.get_text(strip = True))

main()