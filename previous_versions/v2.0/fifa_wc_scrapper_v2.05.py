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

      To do: > Scrap group stage data for location and dates.
             > Scrap penalty kick scores for all tournaments.
             > Scrap match results for the '06 and '18 tournaments.

"""

# -----------------------------------------------------------------------------

# Import libraries
from asyncore import dispatcher_with_send
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
results2006 = 'https://en.wikipedia.org/wiki/2006_FIFA_World_Cup'
results2010 = 'https://en.wikipedia.org/wiki/2010_FIFA_World_Cup'
results2014 = 'https://en.wikipedia.org/wiki/2014_FIFA_World_Cup'
results2018 = 'https://en.wikipedia.org/wiki/2018_FIFA_World_Cup'

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
    matches_scrapper(soup2, wc, matches_ds)

    # Scrapping Germany 2006 data ---------------------------------------------

    wc = wc2006

    # Obtain html from 2006 rosters' webpage
    page = requests.get(wc)
    soup = bs(page.content, 'html.parser')

    # Obtain html from 2006 results' webpage
    page2 = requests.get(results2006)
    soup2 = bs(page2.content, 'html.parser')

    print('Obtaining 2006 rosters...')
    roster_scrapper(soup, wc, rosters_ds)
    print('Obtaining 2006 groups...')
    groups_scrapper(soup, wc, groups_ds)
    print('Obtaining 2006 fixtures...')
    matches_scrapper(soup2, wc, matches_ds)

    # Scrapping South Africa 2010 data ----------------------------------------

    wc = wc2010

    # Obtain html from 2010 rosters' webpage
    page = requests.get(wc)
    soup = bs(page.content, 'html.parser')

    # Obtain html from 2014 results' webpage
    page2 = requests.get(results2010)
    soup2 = bs(page2.content, 'html.parser')

    print('Obtaining 2010 rosters...')
    roster_scrapper(soup, wc, rosters_ds)
    print('Obtaining 2010 groups...')
    groups_scrapper(soup, wc, groups_ds)
    print('Obtaining 2010 fixtures...')
    matches_scrapper(soup2, wc, matches_ds)

    # Scrapping Brazil 2014 data ----------------------------------------------

    wc = wc2014

    # Obtain html from 2014 rosters' webpage
    page = requests.get(wc)
    soup = bs(page.content, 'html.parser')

    # Obtain html from 2014 results' webpage
    page2 = requests.get(results2014)
    soup2 = bs(page2.content, 'html.parser')

    print('Obtaining 2014 rosters...')
    roster_scrapper(soup, wc, rosters_ds)
    print('Obtaining 2014 groups...')
    groups_scrapper(soup, wc, groups_ds)
    print('Obtaining 2014 fixtures...')
    matches_scrapper(soup2, wc, matches_ds)

    # Scrapping Russia 2018 data ----------------------------------------------

    wc = wc2018

    # Obtain html from rosters' webpage
    page = requests.get(wc)
    soup = bs(page.content, 'html.parser')
    
    # # Obtain html from 2018 results' webpage
    page2 = requests.get(results2018)
    soup2 = bs(page2.content, 'html.parser')

    print('Obtaining 2018 rosters...')
    roster_scrapper(soup, wc, rosters_ds)
    print('Obtaining 2018 groups...')
    groups_scrapper(soup, wc, groups_ds)
    print('Obtaining 2018 fixtures...')
    matches_scrapper(soup2, wc, matches_ds)

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

    # Scrapping player data ---------------------------------------------------

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

def matches_scrapper(soup2, wc, matches_ds): # -------------------------------

    # Define lists
    Date = [] # WORKS FOR '02,'10,'14 (ONLY HAVE KNOCKOUT DATA)
    Home = [] # WORKS FOR '02,'10,'14
    Away = [] # WORKS FOR '02,'10,'14
    Result = [] # WORKS FOR '02,'10,'14
    Location = [] # WORKS FOR '02,'10,'14 (ONLY HAVE KNOCKOUT DATA)
    Stage = [] # WORKS FOR '02,'10,'14
    Group = [] # WORKS FOR '02,'10,'14

    # Group stage data scrap --------------------------------------------------

    # Import tables containing all group matches data
    tables = soup2.findAll('table', attrs = {'style':'width:100%'})

    # Import data from 'tables'
    for i in tables:
        # Import group stage home sides
        x = i.findAll('td', attrs = {'align':'right'})
        for j in x:
            Home.append(j.get_text(strip = True))
        # Import group stage away sides
        x = i.findAll('span', attrs = {'style':'white-space:nowrap'})
        for j in x:
            Away.append(j.get_text(strip = True))
        # Import group stage results
        x = i.findAll('td', attrs = {'align':'center'})
        for j in x:
            Result.append(j.get_text(strip = True))

    # Knockout stage data -----------------------------------------------------

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

    # Importing stages and groups
    st = soup2.findAll('span', attrs = {'class':'mw-headline'})

    # mover: defines a list composed of the group stages
    # mover2: defines a list of group match labels
    if wc2002 in wc:
        mover = [st[7],st[17],st[18],st[19],st[20],st[21]]
        mover2 = [st[8],st[9],st[10],st[11],st[12],st[13],st[14],st[15]]
    if wc2010 in wc:
        mover = [st[14],st[24],st[25],st[26],st[27],st[28]]
        mover2 = [st[15],st[16],st[17],st[18],st[19],st[20],st[21],st[22]]
    if wc2014 in wc:
        mover = [st[15],st[25],st[26],st[27],st[28],st[29]]
        mover2 = [st[16],st[17],st[18],st[19],st[20],st[21],st[22],st[23]]

    # 48 group stage games, 8 Round of 16, 4 Quarter finals, 2 Semifinals, 
    # 1 3rd place game, and the final game.
    Stage.extend(itl.repeat(mover[0].get_text(strip = True), 48))
    Stage.extend(itl.repeat(mover[1].get_text(strip = True), 8))
    Stage.extend(itl.repeat(mover[2].get_text(strip = True), 4))
    Stage.extend(itl.repeat(mover[3].get_text(strip = True), 2))
    Stage.append(mover[4].get_text(strip = True))
    Stage.append(mover[5].get_text(strip = True))

    for i in mover2: Group.append(i.get_text(strip = True))
    Group = list(itl.chain.from_iterable(itl.repeat(i, 6) for i in Group))
    Group.extend((itl.repeat('Knockout Stage', 16)))

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
    tournament = list(itl.chain.from_iterable(itl.repeat(i, 64) for i in tournament))

    df = {
        'Tournament': tournament,
        'Stage': Stage,
        'Group': Group,
        # 'Date': Date,
        'Home': Home,
        'Result': Result,
        'Away': Away,
        # 'Location': Location
    }

    df = pd.DataFrame.from_dict(df)
    matches_ds.append(df)

main()
