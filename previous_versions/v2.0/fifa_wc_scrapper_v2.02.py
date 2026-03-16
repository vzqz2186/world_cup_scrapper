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

      To do: - Fix the match scrapper function for the 2018 tournament and
               incorporate it to the main function.
             - Scrap match results for the '02, '06, '10, and '14 tournaments.

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
results2018 = 'https://www.foxsports.com/soccer/fifa-world-cup/schedule'

def main(): # -----------------------------------------------------------------
    
    # Define lists to store dataframes
    rosters_ds = []
    groups_ds = []

    # Scrapping Korea/Japan 2002 data ----------------------------------------

    wc = wc2002

    # Obtain html from 2002 rosters' webpage
    page = requests.get(wc)
    if page.status_code == 200:
        print('2002 rosters response sucessfull...')
    else:
        print('2002 rosters response failed...')
    soup = bs(page.content, 'html.parser')

    print('Obtaining 2002 rosters...')
    roster_scrapper(soup, wc, rosters_ds)
    print('Obtaining 2002 groups...')
    groups_scrapper(soup, wc, groups_ds)

    # Scrapping Germany 2006 data ---------------------------------------------

    wc = wc2006

    # Obtain html from 2006 rosters' webpage
    page = requests.get(wc)
    if page.status_code == 200:
        print('2006 rosters response sucessfull...')
    else:
        print('2006 rosters response failed...')
    soup = bs(page.content, 'html.parser')

    print('Obtaining 2006 rosters...')
    roster_scrapper(soup, wc, rosters_ds)
    print('Obtaining 2006 groups...')
    groups_scrapper(soup, wc, groups_ds)

    # Scrapping South Africa 2010 data ----------------------------------------

    wc = wc2010

    # Obtain html from 2010 rosters' webpage
    page = requests.get(wc)
    if page.status_code == 200:
        print('2010 rosters response sucessfull...')
    else:
        print('2010 rosters response failed...')
    soup = bs(page.content, 'html.parser')

    print('Obtaining 2010 rosters...')
    roster_scrapper(soup, wc, rosters_ds)
    print('Obtaining 2010 groups...')
    groups_scrapper(soup, wc, groups_ds)

    # Scrapping Brazil 2014 data ----------------------------------------------

    wc = wc2014

    # Obtain html from 2014 rosters' webpage
    page = requests.get(wc)
    if page.status_code == 200:
        print('2014 rosters response sucessfull...')
    else:
        print('2014 rosters response failed...')
    soup = bs(page.content, 'html.parser')

    print('Obtaining 2014 rosters...')
    roster_scrapper(soup, wc, rosters_ds)
    print('Obtaining 2014 groups...')
    groups_scrapper(soup, wc, groups_ds)

    # Scrapping Russia 2018 data ----------------------------------------------

    wc = wc2018

    # Obtain html from rosters' webpage
    page = requests.get(wc)
    if page.status_code == 200:
        print('2018 rosters response sucessfull...')
    else:
        print('2018 rosters response failed...')
    soup = bs(page.content, 'html.parser')
    
    # # Obtain html from results' webpage
    # page2 = requests.get(results2018)
    # if page2.status_code == 200:
    #     print('2018 fixtures response sucessfull...')
    # else:
    #     print('2018 fixtures response failed...')
    # soup2 = bs(page2.content, 'html.parser')

    print('Obtaining 2018 rosters...')
    roster_scrapper(soup, wc, rosters_ds)
    print('Obtaining 2018 groups...')
    groups_scrapper(soup, wc, groups_ds)

    # -------------------------------------------------------------------------

    # Combine all df's into one
    rosters_ds = pd.concat(rosters_ds)
    groups_ds = pd.concat(groups_ds)
    print('Data sets complete. Saving...')

    # Save full data sets to csv files
    rosters_ds.to_csv(r'FIFA_wc_players.csv',
                      index = False, encoding = 'utf-8-sig')
    groups_ds.to_csv(r'FIFA_wc_groups.csv',
                     index = False, encoding = 'utf-8-sig')

    print('Done')

# Scrapping player data
def roster_scrapper(soup, wc, rosters_ds): # ----------------------------------

    # Define lists
    Country = [] # Countries
    Birthday = [] # Player's birthday

    # Importing countries
    countries = soup.findAll('h3')
    # Some webpages have more tables than others
    if wc2018 in wc or wc2010 in wc: 
        countries = countries[:-6] # Remove unnecessary data
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

    # Obtain rosters
    tables = soup.findAll('table', attrs = {'class':'wikitable'})
    rosters = pd.read_html(str(tables)) # Create list of dataframes (df)
    # Some webpages have more tables than the others
    if wc2018 in wc or wc2010 in wc:
        rosters = rosters[:-4] # Remove unnecessary df's from list
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
    if wc2018 in wc: # Drop unnecessary column from df
        df.drop('Goals', axis = 1, inplace = True)
    else:
        pass
    df = df.iloc[:, [6,7,0,1,2,3,4,5]] # Fix column order

    print('Rosters obtained...')
    rosters_ds.append(df)

# Scrapping group data
def groups_scrapper(soup, wc, groups_ds): # -----------------------------------

    # Define lists
    Group = []
    Country = []

    # Populating 'Group'
    groups = soup.findAll('h2')
    if wc2002 in wc:
        groups = groups[1:-3] # Remove unnecessary data
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
    # Some webpages have more tables than the others
    if wc2018 in wc or wc2010 in wc: 
        countries = countries[:-6] # Remove unnecessary data
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
    
    # Turn dictionary into df and add it to main list
    df = pd.DataFrame.from_dict(df)
    groups_ds.append(df)
    print('Groups obtained...')

# Scrapping match results 
def matches_scrapper(Location, Date, Result, Home, Away, Group2, Stage): # ----

    # Define lists
    Location = []
    Date = []
    Result = []
    Home = []
    Away = []
    Stage = []
    Group2 = []

    # Get match locations
    table = soup2.findAll('div', attrs = {'class': 'wc-schedule-location'})
    for i in table:
        Location.append(i.get_text())
    print('Match locations obtained...')

    # Get match dates
    raw_dates = []
    table = soup2.findAll('div', attrs = {'class': 'wc-schedule-game-details'})
    for i in table:
        raw_dates.append(i.findAll('div', attrs = {'class': None}))
    # Clean up raw_dates for Date field, which was obtained as a nested list
    for i in raw_dates:
        for j in i:
            it = j.get_text()
            Date.append(it)
    print('Match dates obtained...')

    # Get match results
    table = soup2.findAll('a', attrs = {'href': '#'})
    table = table[8:]
    for i in table:
        Result.append(i.get_text().strip())
    print('Match results obtained...')

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
    print('Home and away sides obtained...')

    # Obtain match groups and match stages
    table = soup2.findAll('div', attrs = {'class': 'wc-schedule-group'})
    for i in table:
        Group2.append(i.get_text())
        Stage.append(i.get_text())
    for i in range(len(Group2)):
        if i < 48:
            Stage[i] = 'Group Stage'
        else:
            Group2[i] = 'N/As'
    print('Match groups and stages obtained...')


    wc = ['Russia 2018']
    wc = list(it.chain.from_iterable(it.repeat(i, 64) for i in wc))

    df = {
        'Tournament': wc,
        'Stage': Stage,
        'Group': Group2,
        'Date': Date,
        'Home': Home,
        'Result': Result,
        'Away': Away,
        'Location': Location
    }
    
    # Turn dictionary into df
    df = pd.DataFrame.from_dict(df)
    print('Matches dataset complete...')

    # Save df to csv file
    df.to_csv(r'csv_Files/FIFA_wc_2018_matches.csv',
              index = False, encoding = 'utf-8-sig')

main()