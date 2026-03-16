"""
Russia 2018 FIFA World Cup data scrapper

     Author: Daniel Vazquez
Description: This program scraps online tables and data related to the
             rosters, teams, fixtures, and main stats from the FIFA World Cup
             organized in Russia for 2018. The program follows the following
             procedure:

             1. Access the html code for the websites to scrap using the
                BeautifulSoup libraries.

             2. Scrap different data from tables and other places from the
                html code to fill lists to be used in completing pandas
                dataframes. The tables contain information on:

                a. Team groups
                b. Participating countries
                c. Players' birthdays
                d. Match locations
                e. Players' birthdays
                f. Match dates
                g. Match results
                h. Match home and away sides
                i. Match groups and stages.

             3. Scrap rosters data from the 2018 World Cup squads entry in
                Wikipedia.

             4. Scrap Group data from the same webpage as Step 3, detailing the
                different groups and the teams that conform make them up.

             5. Scrap match results compiled by Fox Sports.

             6. Save all scrapped dataframes to csv files.

      To do: Scrap player stats (goals, assists, yellow and red cards, saves)
              and add them to the players dataframe.

"""

# Import libraries
from urllib import response
from bs4 import BeautifulSoup as bs
import requests
import pandas as pd
import itertools as it

# Webpage to scrap
wc2018 = 'https://en.wikipedia.org/wiki/2018_FIFA_World_Cup_squads'
results = 'https://www.foxsports.com/soccer/fifa-world-cup/schedule'


def main(): # -----------------------------------------------------------------
    
    # Define lists
    ctry = [] # Countries
    Group = []
    Bday = [] # Player's birthday
    Location = []
    Date = []
    Result = []
    Home = []
    Away = []
    Stage = []
    Group2 = []
    
    # Obtain html from rosters' webpage
    page = requests.get(wc2018)
    if page.status_code == 200:
        print('Page 1 response sucessfull...')
    else:
        print('Page 1 said nope')
    soup = bs(page.content, 'html.parser')
    
    # Obtain html from results' webpage
    page2 = requests.get(results)
    if page2.status_code == 200:
        print('Page 2 response sucessfull...')
    else:
        print('Page 2 said nope')
    soup2 = bs(page2.content, 'html.parser')

    lists(soup, soup2, ctry, Group, Bday, Location, Date, Result, Home, Away, Stage, Group2)
    players_scrapper(soup, ctry, Bday)
    wc_groups_scrapper(Group, ctry)
    matches_scrapper(Location, Date, Result, Home, Away, Group2, Stage)

    print('Done')

# Populating lists to be used by the other functions # ------------------------
def lists(soup, soup2, ctry, Group, Bday, Location, Date, Result, Home, Away, Stage, Group2):

    # Populating 'Group'
    groups = soup.findAll('h2')
    groups = groups[1:-5] # Remove unnecessary data
    for i in groups:
        a = i.get_text()
        it = a.replace('[edit]', '')
        Group.append(it)
    print('Groups obtained...')

    # Populating 'Country'
    countries = soup.findAll('h3')
    countries = countries[:-6] # Remove unnecessary data
    for i in countries:
        a = i.get_text()
        it = a.replace('[edit]', '')
        ctry.append(it)
    print('Countries obtained...')
    
    # Import players' birthdays
    bdays = soup.findAll('span', attrs = {'class':'bday'})
    for i in bdays:
        a = i.get_text()
        Bday.append(a)
    print('Birthdays obtained...')

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

# Scrapping player data
def players_scrapper(soup, ctry, Bday): # -------------------------------------
    
    # Obtain rosters
    tables = soup.findAll('table', attrs = {'class':'wikitable'})
    rosters = pd.read_html(str(tables)) # Create list of dataframes (df)
    rosters = rosters[:-4] # Remove unnecessary df's from list
    df = pd.concat(rosters) # Merge all df's in 'rosters' list into one df
    print('Rosters obtained...')

    # Update 'Date of Birth' field from df, since it appears as NaN in df
    df = df.rename(columns = {'Date of birth (age)': 'Date of Birth'})
    df['Date of Birth'] = Bday
    
    # Add torunament to every players' entry
    wc = ['Russia 2018']
    wc = list(it.chain.from_iterable(it.repeat(i, 736) for i in wc))

    # 23 players per team, so items in 'Country' have to be repeated 23 times
    Country = list(it.chain.from_iterable(it.repeat(i, 23) for i in ctry))

    df['Tournament'] = wc
    df['Country'] = Country # Add 'Country' column to df
    df = df.iloc[:, [7,8,0,1,2,3,4,5,6]]

    print('Player dataset complete...')

    # Save df to csv file
    df.to_csv(r'csv_files/FIFA_wc_2018_players.csv',
              index = False, encoding = 'utf-8-sig')

# Scrapping group data
def wc_groups_scrapper(Group, ctry): # ----------------------------------------

    # 4 teams per group, so items in 'Group' have to be repeated 4 times
    Group = list(it.chain.from_iterable(it.repeat(i, 4) for i in Group))
    
    wc = ['Russia 2018']
    wc = list(it.chain.from_iterable(it.repeat(i, 32) for i in wc))

    # Dictionary with groups and teams
    df = {
        'Tournament': wc,
        'Group': Group,
        'Teams': ctry
    }
    
    # Turn dictionary into df
    df = pd.DataFrame.from_dict(df)
    print('Groups dataset complete...')

    # Save df to csv file
    df.to_csv(r'csv_Files/FIFA_wc_2018_groups.csv',
              index = False, encoding = 'utf-8-sig')

# Scrapping match results 
def matches_scrapper(Location, Date, Result, Home, Away, Group2, Stage): # ----

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