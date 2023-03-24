import asyncio
import aiohttp
import re
import pandas as pd
from bs4 import BeautifulSoup
import re
import numpy as np
import time
import glob
import os

async def extract_game_data(URL, game_id):
    # URL = 'https://www.vlr.gg/165323/rebels-gaming-vs-movistar-riders-challengers-league-spain-rising-split-1-r9/?game=110615&tab=overview'
    # Creating list with all tables
    page = ''
    while page == '':
        try:
            page = requests.get(URL)
            break
        except:
            print("Connection refused by the server..")
            print("Let me sleep for 5 seconds")
            print("ZZzzzz...")
            time.sleep(5)
            print("Was a nice sleep, now let me continue...")
            continue

    soup = BeautifulSoup(page.content, "html.parser")
    #try:
    misc = misc_data(soup, game_id)
    #rounds, misc = misc_data(soup, game_id)
    #round_data = pd.DataFrame(data=rounds)
    misc.to_csv("not_clean/misc/"+(str(game_id)+"_misc.csv")) 
        #round_data.to_csv("not_clean/rounds/"+(str(game_id)+"_rounds.csv")) 
    #except:
        #print("Misc Dataframe Error Outputting Misc:")
        #print(f'Misc: {misc} \ngame_id: {game_id}\nType(Misc): {type(misc)}\nType(game_id): {type(game_id)}')
        #print()
    try:
        test = soup.findAll('div', class_='vm-stats-game')
        titles = []
        for td in soup.findAll(class_='vm-stats-game mod-active'):
            for test in td.select('td.mod-agents'):
                img = test.select_one('span.stats-sq img')
                if img:
                    titles.append(img.get('title'))


        team_one_agents = titles[0:5]
        team_two_agents = titles[5:10]
        table = soup.findAll('table', class_='wf-table-inset mod-overview') 

        df_one = pd.read_html(str(table))[0].dropna(axis='columns')
        df_two = pd.read_html(str(table))[1].dropna(axis='columns')
        df_one.columns = ['IGN', 'R', 'ACS', 'K', 'D', 'A', 'P_M_ONE', 'KAST', 'ADR', 'HS', 'FK', 'FD', 'P_M_2']
        df_two.columns = ['IGN', 'R', 'ACS', 'K', 'D', 'A', 'P_M_ONE', 'KAST', 'ADR', 'HS', 'FK', 'FD', 'P_M_2']
        print(team_one_agents, team_two_agents)
        df_one['AGENTS'] = team_one_agents
        df_two['AGENTS'] = team_two_agents
    except:
        print('Last try in extract game data broke.')
    return df_one, df_two

async def grab_games_from_match(URL):
    page = ''
    while page == '':
        try:
            page = requests.get(URL+'/?game=all&tab=overview')
            break
        except:
            print("Connection refused by the server..")
            print("Let me sleep for 5 seconds")
            print("ZZzzzz...")
            time.sleep(5)
            print("Was a nice sleep, now let me continue...")
            continue

    soup = BeautifulSoup(page.content, "html.parser")
    divs = soup.findAll('div', class_='vm-stats-gamesnav-item js-map-switch')
    game_ids = []
    for y in divs:
        game_ids.append(URL+'/?game='+y["data-game-id"])
    return game_ids

async def grab_matches_from_results_page(URL):
    page = ''
    while page == '':
        try:
            page = requests.get(URL)
            break
        except:
            print("Connection refused by the server..")
            print("Let me sleep for 5 seconds")
            print("ZZzzzz...")
            time.sleep(5)
            print("Was a nice sleep, now let me continue...")
            continue
    soup = BeautifulSoup(page.content, features="lxml")
    urls = []
    for a in soup.find_all('a', href=True):
        urls.append(a['href'])
    link_list = []
    words_list = urls
    letters = set('-')

    for word in words_list:
        if letters & set(word):
            link_list.append(word)

    return link_list

async def round_parser(round_list):
    parsed_round_list = []
    for item in round_list:
        if re.search('[0-9]?[-][0-9]?', item):
            parsed_round_list.append(item)
    return parsed_round_list

async def round_list_to_df(parsed_round_list):
    rounds_stripped = [i.split('-', 1) for i in round_parser(parsed_round_list)]
    df = pd.DataFrame(columns={rounds[0], rounds[1]}, data=rounds_stripped)
    # feature idea: tempo, something to deal with how many rounds were won in succession compared to back to back.
    return df

# round_list_to_df(round_parser(rounds))
# #team on the left appears to be  Offense and team on the right appears to be Defense

async def misc_data(soup, game_id) -> pd.DataFrame:
    #rounds = soup.find('div', class_='vlr-rounds').text.split()
    teams = soup.findAll('div', class_='team-name')
    team_one = teams[0].text.split()
    team_two = teams[1].text.split()
    team_one =' '.join(team_one)
    team_two = ' '.join(team_two)
    patch = soup.findAll(class_='match-header-date')[-1]
    date_and_patch = patch.text.split()
    patch = date_and_patch[-1]
    date = date_and_patch[:-2]
    date = ' '.join(date)
    _map = soup.find('div', class_='map').text.split()[0]
    ct_rounds = soup.find_all(class_="mod-ct")[0:2] # first index is left team, second index is right corresponds to first and second index later
    t_rounds = soup.find_all(class_="mod-t")[0:2]
    match_type = soup.findAll(class_='match-header-vs-note')
    match_type = match_type[-1].text.split()[0]
    t1_ct_rounds = ct_rounds[0].text.split()
    t1_t_rounds = t_rounds[0].text.split()
    t2_ct_rounds = ct_rounds[1].text.split()
    t2_t_rounds = t_rounds[1].text.split()
    t1_ct_rounds =''.join(t1_ct_rounds)
    t1_t_rounds =''.join(t1_t_rounds)
    t2_ct_rounds =''.join(t2_ct_rounds)
    t2_t_rounds =''.join(t2_t_rounds)
    t1_total = t1_t_rounds + t1_ct_rounds
    t2_total = t2_t_rounds + t2_ct_rounds

    data = np.array([team_one, t1_ct_rounds, t1_t_rounds, team_two, t2_ct_rounds, t2_t_rounds, _map, date, patch, game_id, t1_total>t2_total, t2_total>t1_total])
    columns = ['team_one', 'defense_rounds', 'offense_rounds', 'team_two', 'defense_rounds', 'offense_rounds', 'map', 'date', 'patch', 'game_id', 'team_one_win', 'team_two_win']
    misc = pd.DataFrame(data=data)
    return misc
URL = "https://www.vlr.gg/164064/enigma-gaming-vs-kizuna-esports-challengers-league-malaysia-singapore-split-1-d13/?game=109086&tab=overview"
#misc_data(URL, 2)


def get_links(num_pages=393, i_start=1):
    #URL = 'https://www.vlr.gg/matches/results/?page='
    results_links = []
    import os  
    os.makedirs('not_clean/', exist_ok=True)  
    os.makedirs('not_clean/economy/', exist_ok=True)
    os.makedirs('not_clean/performance/', exist_ok=True)
    os.makedirs('not_clean/scoreboards/', exist_ok=True)
    os.makedirs('not_clean/rounds/', exist_ok=True)
    for i in range(i_start, num_pages+1): # currently 393 pages of matches
        URL = 'https://www.vlr.gg/matches/results/?page='+str(i)
        #print()
        #print()
        #print(URL)
        #print()
        results_links.append(URL)
    return results_links

async def grab_games_from_match(session, URL):
    async with session.get(URL) as resp:
        text = await resp.text()
    soup = BeautifulSoup(text, "lxml")
    divs = soup.select('div.vm-stats-gamesnav-item.js-map-switch[data-game-id]')
    game_ids = [URL + '/?game=' + y['data-game-id'] for y in divs]
    tasks = [output_scoreboard(session, link) for link in game_ids]
    await asyncio.gather(*tasks)

async def grab_matches_from_results_page(session, URL):
    async with session.get(URL) as resp:
        text = await resp.text()
    soup = BeautifulSoup(text, "lxml")
    urls = [a['href'] for a in soup.select('a[href]')]
    link_list = ["https://www.vlr.gg" + x for x in urls if '-' in x]
    tasks = []
    for link in link_list:
        #print(link)
        tasks.append(grab_games_from_match(session, link))
   # tasks = [grab_games_from_match(session, link) for link in link_list]
    await asyncio.gather(*tasks)

async def output_scoreboard(session, game_url):
    start_time = time.time()
    #print(game_url, 'test')
    game_id = re.search('[0-9]*', game_url)
    if game_id:
        # try:
        #print(game_url)
        game_id = game_id.groups()
        async with session.get(game_url) as resp:
            text = await resp.text()
        soup = BeautifulSoup(text, "html.parser")
        #try:
        tasks = misc_data(soup, game_id)
        misc = await asyncio.gather(tasks)
        #print(misc)
        #print(type(misc))
        #print(type(misc))
        #round_data = pd.DataFrame(data=rounds)
        #misc.to_csv("not_clean/misc/"+(str(game_id)+"_misc.csv")) 
        #round_data.to_csv("not_clean/rounds/"+(str(game_id)+"_rounds.csv")) 
        #except Exception as e:
            #print(f"Misc Dataframe Error: {e}")
            #print()

        titles = [img.get('title') for img in soup.select('div.vm-stats-game.mod-active td.mod-agents span.stats-sq img')]
        team_one_agents = titles[0:5]
        team_two_agents = titles[5:10]
        table = soup.select('table.wf-table-inset.mod-overview') 

        df_one = pd.read_html(str(table))[0].dropna(axis='columns')
        df_two = pd.read_html(str(table))[1].dropna(axis='columns')
        df_one.columns = ['IGN', 'R', 'ACS', 'K', 'D', 'A', 'P_M_ONE', 'KAST', 'ADR', 'HS', 'FK', 'FD', 'P_M_2']
        df_two.columns = ['IGN', 'R', 'ACS', 'K', 'D', 'A', 'P_M_ONE', 'KAST', 'ADR', 'HS', 'FK', 'FD', 'P_M_2']
        print(team_one_agents, team_two_agents)
        df_one['AGENTS'] = team_one_agents
        df_two['AGENTS'] = team_two_agents
        scoreboard_df = pd.concat([df_one, df_two])
        print(f"{(time.time() - start_time):.2f} seconds")
        print()
        #print(scoreboard_df)
        print()
        scoreboard_df.to_csv("not_clean/scoreboards/"+(str(game_id)+"_scoreboard.csv"))
        # except:
        #     print('Try inside output_scoreboard broke')
        
    else:
        pass

#async def main():
async with aiohttp.ClientSession() as session:
    # tasks = [grab_matches_from_results_page(session, url) for url in get_links()]
    # # print(tasks)
    # matches = await asyncio.gather(*tasks)
    # # matches = []
    # # with open('array.txt') as f:
    # #     lines = f.readlines()
    # # matches = map(lambda lines: lines.strip(), lines)

    # # matches =  ["https://www.vlr.gg/" + x for x in matches]
    # #print(lines)
    # tasks = []
    # #for i in range(5):
    #     #print(matches[i])
    #     #tasks.append(grab_games_from_match(session, str(matches[i])))
    # tasks = [grab_games_from_match(session, url) for url in matches]
    # games = await asyncio.gather(*tasks)
    # [await output_scoreboard(session, url) for url in matches]
    for url in get_links():  
        tasks = [grab_matches_from_results_page(session, url)]
    matches = await asyncio.gather(*tasks)
    tasks = [grab_games_from_match(session, url) for url in matches]
    games = await asyncio.gather(*tasks)
    [output_scoreboard(session, url) for url in games]

#asyncio.run(main())
