import asyncio
import aiohttp
from bs4 import BeautifulSoup
import pandas as pd
import os
import re
import numpy as np
import time

async def main():
    '''
    Main function to start the scraper.
    i is currently set to 394 for the amount of pages at the time of last execution
    '''
    # start timer
    main_start = time.time()
    # Create directories to store dataframes/csvs
    print("Creating directories to store scraped data.")
    print()
    os.makedirs('not_clean/', exist_ok=True)  
    os.makedirs('not_clean/economy/', exist_ok=True)
    os.makedirs('not_clean/performance/', exist_ok=True)
    os.makedirs('not_clean/scoreboards/', exist_ok=True)
    os.makedirs('not_clean/rounds/', exist_ok=True)
    os.makedirs('not_clean/misc/', exist_ok=True)
    
    print("Starting async with aiohttp client session.")
    print()

    # start a loop to create a new session for every page. 
    # was running into a bug where session would time out
    i=2
    while i < 3:
        print(i)
        # Start the timer on each page
        page_start = time.time()
        # Start the new session
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=800)) as session:
            
            print("Creating list of links from the results page")
            print()
            results_list = create_results_links(start_page=i, end_page=i)
            print(f'Done: {results_list}')
            print()
            print("Grabbing all matches from the each link in results link list (different from games).")
            print()
            text_requests = [await get_text(session, url) for url in results_list]
            results_tasks = [await grab_matches_from_results_links(text, url) for url, text in [results_list, text_requests]][0]
            #results_tasks = [await grab_matches_from_results_links(session, results_page) for results_page in results_list][0]
            print(f'Done: {results_tasks}')
            print()
            print("Grabbing all games from collected matches (BO1, BO3, BO5).")
            print()
            text_requests = [await get_text(session, url) for url in results_list]
            match_tasks = [await grab_games_from_match_links(text, match_page) for match_page, text in [results_tasks, text_requests]]
            match_tasks = list(filter(None, match_tasks))
            print(f'Done: {match_tasks}')
            print()
            
            print("Looping over each match and outputting the information as a csv.")
            print()
            for page in match_tasks:
                print('Starting new match.')
                print(page)
                print()
                for URL in page:
                    print('Starting new game.')
                    print(URL)
                    print()
                    text = await get_text(session, URL)
                    await output_scoreboard(text, URL)
        print(f"{(time.time() - page_start):.2f} seconds")
        i += 1
    print(f"{(time.time() - main_start):.2f} seconds")
        
async def grab_matches_from_results_links(text, URL):
    '''
    :param session:
    :param URL:
    Gets all the matches on the results page. These matches may be a BO1, BO3 or BO5
    Returns a list of these links to matches
    '''    

    soup = BeautifulSoup(text, "lxml") # parsing with soup
    urls = [a['href'] for a in soup.select('a[href]')] # selecting the urls on the page with list comprehension
    links_list = ["https://www.vlr.gg" + x for x in urls if '-' in x] # adding the prefix of the link with list comprehension
    return links_list

async def grab_games_from_match_links(text, URL):
    '''
    Async function to extract games from passed URL
    :param session: current asyncio session
    :param URL: current match to grab games from. A match can be a best of 1, 3 or 5.
    Scrapes the passed URL for the games on the page and returns the game links found
    returns List of links to individual games.
    '''
    soup = BeautifulSoup(text, "lxml") # parsing with beautiful soup
    divs = soup.select('div.vm-stats-gamesnav-item.js-map-switch[data-game-id]') # selecting the container for games
    if divs: # if games are found
        game_ids = [URL + '/?game=' + y['data-game-id'] for y in divs] # list comprehension to grab games
        game_ids = [ele for ele in game_ids if ele[-3:] != 'all' and ele is not None] # list comprehension to strip when it is a 'all' game or None
        return game_ids
    
def create_results_links(end_page=1, start_page=1):
    '''
    :param end_page: the last page on the matches/results/?page= that you want to scrape
    :param start_page: the first page on the matches/results/?page= that you want to scrape
    URL = 'https://www.vlr.gg/matches/results/?page=' Template for URL that is being processed
    Creates a list of these "pages" where matches are so we can query those pages for the links to the games
    Returns list of links from the parsing
    '''
    # Create list to return list of results links
    results_links = []
    # Iterate from start_page to end_page and create a list of these links to pass to async function
    for i in range(start_page, end_page+1): # currently 393 pages of matches
        results_links.append(('https://www.vlr.gg/matches/results/?page='+str(i)))
    return results_links

def misc_data(text, game_id) -> pd.DataFrame:
    '''
    :param soup: soup parsed text from the current game
    :param game_id: the current game_id.
    This extracts miscellanous information available on the page wanted or needed for the dataframe
    There is a lot of If, Else error catching
    '''
    misc_data_list = [] # List of 'data' that will end up in dataframe
    columns = [] # List of column names that will end up in the dataframe
    soup = BeautifulSoup(text, "lxml") # parse with beautiful soup (html.parser)
    rounds = soup.find('div', class_='vlr-rounds') # Getting round information
    if rounds: # if soup found something
        rounds = rounds.text.split() # would error splitting None
    else:
        print("Error: Round data was not found in soup.find")
        print()
    
    teams = soup.findAll('div', class_='team-name') #Getting team information
    if teams: # Some of these operatons would be erroring from trying it on None
        # Processing the text elements
        team_one = teams[0].text.split()
        team_two = teams[1].text.split()
        team_one =' '.join(team_one)
        team_two = ' '.join(team_two)
        # adding team_one data and team_two data to data list as well as appending list of columns
        misc_data_list.append(team_one)
        misc_data_list.append(team_two)
        columns.append('team_one')
        columns.append('team_two')
    else:
        print("Error: Team data was not found in soup.findAll")
        print()

    patch = soup.findAll(class_='match-header-date') # Getting patch information
    if patch: # Error catching
        # processing
        patch = patch[-1] 
        date_and_patch = patch.text.split() 
        patch = date_and_patch[-1]
        date = date_and_patch[:-2]
        date = ' '.join(date)
        # Adding patch and date to dataframe
        misc_data_list.append(patch)
        misc_data_list.append(date)
        columns.append('patch')
        columns.append('date')
        print(patch)
    else:
        print("Error: Patch information was not found in soup.findAll")

    _map = soup.find('div', class_='map') # getting map information
    if _map: # error checking
        _map = _map.text.split()[0]
    else:
        print("Error: Map information was not found with soup.find")
        print()
    ct_rounds = soup.find_all(class_="mod-ct") # first index is left team, second index is right corresponds to first and second index later
    if ct_rounds: # error checking
        # processing
        ct_rounds = ct_rounds[0:2]
        t1_ct_rounds = ct_rounds[0].text.split()
        t2_ct_rounds = ct_rounds[1].text.split()
        t1_ct_rounds =''.join(t1_ct_rounds)
        t2_ct_rounds =''.join(t2_ct_rounds)

    else:
        print("Error: CT Round information was not found with soup.find_all")
        print()

    t_rounds = soup.find_all(class_="mod-t") # Grabbing offense round information
    if t_rounds: # error checking
        #processing
        t_rounds = t_rounds[0:2]
        t1_t_rounds = t_rounds[0].text.split()
        t2_t_rounds = t_rounds[1].text.split()
        t1_t_rounds =''.join(t1_t_rounds)
        t2_t_rounds =''.join(t2_t_rounds)

    else:
        print("Error: T Round information was not found with soup.find_all")
        print()

    if ct_rounds and t_rounds: # error checking
        # processing and feature engineering
        t1_total = t1_t_rounds + t1_ct_rounds
        t2_total = t2_t_rounds + t2_ct_rounds
        # adding data to dataframe
        misc_data_list.append(t1_ct_rounds)
        misc_data_list.append(t2_ct_rounds)
        misc_data_list.append(t1_t_rounds)
        misc_data_list.append(t2_t_rounds)
        misc_data_list.append(t1_total)
        misc_data_list.append(t2_total)
        columns.append('t1_ct_rounds')
        columns.append('t2_ct_rounds')
        columns.append('t1_t_rounds')
        columns.append('t2_t_rounds')
        columns.append('t1_total')
        columns.append('t2_total')

    match_type = soup.findAll(class_='match-header-vs-note') # getting match type information
    if match_type: # error checking
        # processing
        match_type = match_type[-1].text.split()[0]
        # adding to dataframe
        misc_data_list.append(match_type)
        columns.append('match_type')
    else:
        print("Error: Match type failed to parse.")
        print()
    # finally adds game_id at the end
    misc_data_list.append(game_id)
    columns.append('game_id')
    # transposes the data to fit the correct shape
    data = np.array(misc_data_list).reshape(1,-1)
    # creates an array for the columns
    columns = np.array(columns)
    # creating misc dataframe
    misc = pd.DataFrame(columns=columns, data=data)
    # creating rounds dataframe
    rounds = pd.DataFrame(data=rounds)

    print("Printing Misc DataFrame and then Round Dataframe.")
    print()
    print(misc)
    print()
    print(rounds)
    print()
    return misc, rounds

async def get_text(session, url):
    async with session.get(url) as resp: # async session get url resp
        text = await resp.text() # get text
    return text.result()


async def output_scoreboard(text, game_url):
    '''
    :param session: asyncio session passed to from main()
    :param game_url: string representing a url/link to the game currently being proccessed
    This function serves as a partial pre-process to the scoreboard.
    Calls resp to get text from the passed url
    grabs table data representing the scoreboard of a valorant match
    saves the information for the match into three seperate CSVs
    :returns: None
    '''
    for i in range(len(game_url)): # Old school C style string processing. Looking for =  and the game_id that follows
        if game_url[i] == '=': # found the =
            game_id = game_url[i+1:] # grabbing the index after the = to get game id
            break # backing out of for loop. no need to increment further
    if game_id: # if it grabbed the id properly (can't handle errors)
        soup = BeautifulSoup(text, "lxml") # parse with beautiful soup (html.parser)
        misc, round_data = misc_data(soup, game_id) # call to get misc data and round_data
        misc.to_csv("not_clean/misc/"+(str(game_id)+"_misc.csv")) # Saving Misc dataframe to not_clean/misc/{game_id}_misc.csv
        round_data.to_csv("not_clean/rounds/"+(str(game_id)+"_rounds.csv")) # Saving Rounds dataframe to not_clean/roudns/{game_id}_rounds.csv
        # grabs agent names
        titles = [img.get('title') for img in soup.select('div.vm-stats-game.mod-active td.mod-agents span.stats-sq img')] 
        if titles: # further error catching, can't afford for scraper to randomly stop on page 250
            team_one_agents = titles[0:5] # Grabs team one agents
            team_two_agents = titles[5:10] # Grabs team two agents
            table = soup.select('table.wf-table-inset.mod-overview') # Grabs table information for scoreboard
            try: # Further error catching. Try all below -> print if fails. Allows scraper to continue upon a failed parse or scrape. May result in an 'incomplete' dataset
                df_one = pd.read_html(str(table))[0].dropna(axis='columns') # Grab 'first' team table
                df_two = pd.read_html(str(table))[1].dropna(axis='columns') # Grab 'second' team table
                # Adding column names (the same) to both dataframes
                df_one.columns, df_two.columns = ['IGN', 'R', 'ACS', 'K', 'D', 'A', 'P_M_ONE', 'KAST', 'ADR', 'HS', 'FK', 'FD', 'P_M_2'], ['IGN', 'R', 'ACS', 'K', 'D', 'A', 'P_M_ONE', 'KAST', 'ADR', 'HS', 'FK', 'FD', 'P_M_2']
                # Adding in agents to dataframe
                df_one['AGENTS'], df_two['AGENTS'] = team_one_agents, team_two_agents
                # merging the dataframes
                scoreboard_df = pd.concat([df_one, df_two])
                print("Saving scoreboard as csv.")
                print()
                scoreboard_df.to_csv("not_clean/scoreboards/"+(str(game_id)+"_scoreboard.csv"))
            except:
                print('Error: Issue grabbing results from match.')

        else:
            print('Error: Nothing was found in titles (there would be no agent data).')
            return None
    else:
        print('Error: Game ID was not found from the re.search')
        return None

if __name__ == "__main__":
    await main()