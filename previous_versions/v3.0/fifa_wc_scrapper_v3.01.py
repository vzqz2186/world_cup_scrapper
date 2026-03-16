"""
FIFA World Cup data scrapper

     Author: Daniel Vazquez

Description: This program scraps online tables and data related to the
             rosters, teams, fixtures, and main stats for the FIFA World Cup
             tournaments from 2002 to 2022. All data is scrapped from their
             respective entries in Wikipedia. Since its very uniformilly
             organized throughout all pages, it allows easy scalability.
             The program follows the following procedure:

             1. Access the html code for the websites to scrape using the
                BeautifulSoup libraries.

             2. Scrape the html code looking for the following information:

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

      To do: > Add '22 edition
             > Test all functions work:
               - roster_scraper:
                 ~ Quatar '22       [YES]
                 ~ Russia '18       [YES]
                 ~ Brazil '14       [YES]
                 ~ South Africa '10 [YES]
                 ~ Germany '06      [NO]
                 ~ Korea/Japan '02  [NO]
               - groups_scraper:
                 ~ Quatar '22       []
                 ~ Russia '18       []
                 ~ Brazil '14       []
                 ~ South Africa '10 []
                 ~ Germany '06      []
                 ~ Korea/Japan '02  []
               - matches_scraper:
                 ~ Quatar '22       []
                 ~ Russia '18       []
                 ~ Brazil '14       []
                 ~ South Africa '10 []
                 ~ Germany '06      []
                 ~ Korea/Japan '02  []
             > Automate SQL Server upload
             > Update GitHub repository
                 
"""

# -----------------------------------------------------------------------------

# Import libraries
from bs4 import BeautifulSoup as bs
import requests
import pandas as pd
import itertools as itl
from io import StringIO
import numpy as np

headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36'}

# Webpages to scrap
squads02 = 'https://en.wikipedia.org/wiki/2002_FIFA_World_Cup_squads'
squads06 = 'https://en.wikipedia.org/wiki/2006_FIFA_World_Cup_squads'
squads10 = 'https://en.wikipedia.org/wiki/2010_FIFA_World_Cup_squads'
squads14 = 'https://en.wikipedia.org/wiki/2014_FIFA_World_Cup_squads'
squads18 = 'https://en.wikipedia.org/wiki/2018_FIFA_World_Cup_squads'
squads22 = 'https://en.wikipedia.org/wiki/2022_FIFA_World_Cup_squads'

results02 = 'https://en.wikipedia.org/wiki/2002_FIFA_World_Cup'
results06 = 'https://en.wikipedia.org/wiki/2006_FIFA_World_Cup'
results10 = 'https://en.wikipedia.org/wiki/2010_FIFA_World_Cup'
results14 = 'https://en.wikipedia.org/wiki/2014_FIFA_World_Cup'
results18 = 'https://en.wikipedia.org/wiki/2018_FIFA_World_Cup'
results22 = 'https://en.wikipedia.org/wiki/2022_FIFA_World_Cup'

wc_squads = [squads10, squads14, squads18, squads22] # squads02, squads06, 
wc_matches = [results10, results14, results18, results22] # results02, results06, 
wc_editions = ['South Africa 2010','Brazil 2014', 'Russia 2018', 'Quatar 2022'] # 'Korea/Japan 2002', 'Germany 2006', 


def main(): # -----------------------------------------------------------------
    
    # Define lists to store dataframes
    rosters_ds = []
    groups_ds = []
    matches_ds = []

    # Scrapping tournament data ----------------------------------------

    for squads, games, edition in zip(wc_squads, wc_matches, wc_editions):
        
        # Obtain html from 2002 rosters' webpage
        page = requests.get(squads, headers = headers)
        soup = bs(page.content, 'html.parser')

        # Obtain html from 2002 results' webpage
        page2 = requests.get(games, headers = headers)
        soup2 = bs(page2.content, 'html.parser')

        print(f'Obtaining {edition} rosters...')
        roster_scraper(soup, squads, edition, rosters_ds)    
        #print(f'Obtaining {edition} groups...')
        #groups_scraper(soup, wc, groups_ds)
        #print(f'Obtaining {edition} fixtures...')
        #matches_scraper(soup2, wc, matches_ds)

    # -------------------------------------------------------------------------

    # Combine all df's into one
    rosters_ds = pd.concat(rosters_ds)
    #groups_ds = pd.concat(groups_ds)
    #matches_ds = pd.concat(matches_ds)
    print(rosters_ds) #print('Data sets complete. Saving...')

    """
    # Save full data sets to csv files
    rosters_ds.to_csv(r'csv_files/FIFA_wc_players.csv',
                      index = False, encoding = 'utf-8-sig')
    
    groups_ds.to_csv(r'csv_files/FIFA_wc_groups.csv',
                     index = False, encoding = 'utf-8-sig')
    matches_ds.to_csv(r'csv_files/FIFA_wc_matches.csv',
                      index = False, encoding = 'utf-8-sig')
    """

    #print('Done')
    
def roster_scraper(soup, squads, edition, rosters_ds): # ------------------------------

    # Define lists
    Country = [] # Participating countries
    Birthday = [] # Player's birthday

    # Import participating countries ------------------------------------------
    countries = soup.find_all('h3')

    # Some webpages have more tables than others. Unnecessary data needs to be removed.
    if squads == squads10 or squads == squads18:
        countries = countries[:-5]
    elif squads == squads14 or squads == squads06:
        countries = countries[:-4]
    elif squads == squads02:
        countries = countries[:-3]
    elif squads == squads22:
        countries = countries[:-7]

    # Import countries
    for i in countries:
        a = i.get_text()
        it = a.replace('[edit]', '')
        Country.append(it)

    # Scrapping player data ---------------------------------------------------

    # Import rosters
    tables = soup.find_all('table', attrs = {'class':'wikitable'})
    rosters = pd.read_html(StringIO(str(tables))) # Create list of dataframes (df)

    # Some webpages have more tables than others. Unnecessary data needs to be removed.
    if squads == squads18 or squads == squads10:
        rosters = rosters[:-4]
    elif squads == squads14:
        rosters = rosters[:-3]
    elif squads == squads06:
        rosters = rosters[:-2]
    elif squads == squads02:
        rosters = rosters[:-1]
    elif squads == squads22:
        rosters = rosters[:-6]

    df = pd.concat(rosters) # Merge all df's in 'rosters' list into one df
    
    # Import players' birthdays
    bdays = soup.find_all('span', attrs = {'class':'bday'})
    for i in bdays:
        a = i.get_text()
        Birthday.append(a)

    # Update 'Date of Birth' field from df, since it appears as NaN in df
    df = df.rename(columns = {'Date of birth (age)': 'Date of Birth'})
    df['Date of Birth'] = Birthday

    # Add torunament to every players' entry
    tournament = [edition]
    
    if squads == squads22: # 2022 edition increased team size from 23 to 26
        tournament = list(itl.chain.from_iterable(itl.repeat(i, 831) for i in tournament))
    else:
        tournament = list(itl.chain.from_iterable(itl.repeat(i, 736) for i in tournament))

    df['Tournament'] = tournament # Add 'Tournament' column to df

    # Add players' nationalities
    if squads == squads22: # 2022 edition increased team size from 23 to 26. Iran presented 25 players instead of 26.
        Country = list(itl.chain.from_iterable(itl.repeat(i, 25 if i == 'Iran' else 26) for i in Country))
    else:
        Country = list(itl.chain.from_iterable(itl.repeat(i, 23) for i in Country))

    #print("Lenght of df: ", len(df), "\nLen of Tournament: ", len(tournament), "\nLen of Country: ", len(Country))
    #print(pd.Series(Country).value_counts(), f'\nTotal: {len(Country)}')

    df['Country'] = Country # Add 'Country' column to df

    # Add Goals column to tables that don't have it and fix column order
    if squads == squads18 or squads == squads22:
        df = df.iloc[:, [0,1,2,3,4,5,6,8,7]]
    else:
        df['Goals'] = np.nan
        df = df.iloc[:, [0,1,2,3,4,5,6,8,7]]

    #print(df)

    print('Rosters obtained...')
    rosters_ds.append(df)
       
def groups_scraper(soup, wc, groups_ds): # -----------------------------------

    # Define lists
    Group = []
    Country = []

    # Import groups
    # Some webpages have more tables than others. Unnecessary data needs to be
    # removed.
    groups = soup.find_all('h2')
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
    countries = soup.find_all('h3')
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

def matches_scraper(soup2, wc, matches_ds): # -------------------------------

    # Define lists
    Date = []     # WORKS FOR '02,'06,'10,'14 (ONLY HAVE KNOCKOUT DATA)
    Home = []     # WORKS FOR '02,'06,'10,'14,'18
    Away = []     # WORKS FOR '02,'06,'10,'14,'18
    Result = []   # WORKS FOR '02,'06,'10,'14,'18
    Location = [] # WORKS FOR '02,'06,'10,'14 (ONLY HAVE KNOCKOUT DATA)
    Stage = []    # WORKS FOR '02,'06,'10,'14,'18
    Group = []    # WORKS FOR '02,'06,'10,'14,'18

    # Group stage data scrap --------------------------------------------------

    # Import tables containing all group matches data
    if wc2006 in wc:
        group_tables = soup2.find_all('table', attrs = {'style':'width:100%;'})
    elif wc2018 in wc:
        group_tables = soup2.find_all('tr', attrs = {'itemprop':'name'})
    else:
        group_tables = soup2.find_all('table', attrs = {'style':'width:100%'})

    # Import data from 'tables'
    for group in group_tables:

        # Import group stage home sides
        if wc2006 in wc:
            x = group.find_all('td', attrs = {'style':'text-align:right;'})
        elif wc2018 in wc:
            x = group.find_all('th', attrs = {'class':'fhome'})
        else:
            x = group.find_all('td', attrs = {'align':'right'})
        for j in x:
            Home.append(j.get_text(strip = True))

        # Import group stage away sides
        if wc2018 in wc:
            x = group.find_all('th', attrs = {'class':'faway'})
        else:
            x = group.find_all('span', attrs = {'style':'white-space:nowrap'})
        for j in x:
            Away.append(j.get_text(strip = True))

        # Import group stage results
        if wc2006 in wc:
            x = group.find_all('td', attrs = {'style':'text-align:center;'})
        elif wc2018 in wc:
            x = group.find_all('th', attrs = {'class':'fscore'})
        else:
            x = group.find_all('td', attrs = {'align':'center'})
        for j in x:
            Result.append(j.get_text(strip = True))

    # Knockout stage data -----------------------------------------------------

    # # Importing knockout round dates
    # dates = soup2.find_all('div', attrs = {'class':'fdate'})
    # for i in dates:
    #     Date.append(i.get_text(strip = True))

    # Lines 330-366 also complete this purpose for 2018, so lines 379-394 are
    # for all other tournaments.
    if wc2018 in wc:
        pass
    else:
        # Importing knockout round home and away sides
        teams = []
        teams = soup2.find_all('span', attrs = {'itemprop':'name'})
        cnt = 0
        for i in teams:
            if cnt % 2 == 0:
                Home.append(i.get_text(strip = True))
                cnt += 1
            else:
                Away.append(i.get_text(strip = True))
                cnt += 1

        # Importing knockout round results
        results = soup2.find_all('th', attrs = {'class':'fscore'})
        for i in results:
            Result.append(i.get_text(strip = True))

    # # # Importing knockout round locations
    # # loc = soup2.find_all('div', attrs = {'itemprop':'location'})
    # # for i in loc:
    # #     Location.append(i.get_text(strip = True))

    # Importing stages and groups
    st = soup2.find_all('span', attrs = {'class':'mw-headline'})

    # mover: defines a list composed of the tournament stages
    # mover2: defines a list of group match labels
    if wc2002 in wc:
        mover = [st[7],st[17],st[18],st[19],st[20],st[21]]
        mover2 = [st[8],st[9],st[10],st[11],st[12],st[13],st[14],st[15]]
    if wc2006 in wc:
        mover = [st[17],st[27],st[28],st[29],st[30],st[31]]
        mover2 = [st[18],st[19],st[20],st[21],st[22],st[23],st[24],st[25]]
    if wc2010 in wc:
        mover = [st[14],st[24],st[25],st[26],st[27],st[28]]
        mover2 = [st[15],st[16],st[17],st[18],st[19],st[20],st[21],st[22]]
    if wc2014 in wc:
        mover = [st[15],st[25],st[26],st[27],st[28],st[29]]
        mover2 = [st[16],st[17],st[18],st[19],st[20],st[21],st[22],st[23]]
    if wc2018 in wc:
        mover = [st[18],st[29],st[30],st[31],st[32],st[33]]
        mover2 = [st[19],st[20],st[21],st[22],st[23],st[24],st[25],st[26]]

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

    # Create label for the entry's respective tournament.
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
        'Away': Away
        # 'Location': Location
    }

    df = pd.DataFrame.from_dict(df)
    matches_ds.append(df)

main()