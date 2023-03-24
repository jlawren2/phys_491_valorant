import pandas as pd
import os
import numpy as np
import re

def process_files():
    scoreboard_dir = 'not_clean/scoreboards'
    misc_dir = 'not_clean/misc'
    os.makedirs('clean/', exist_ok=True)  
    #complete_scoreboard = pd.DataFrame()
    for filename in os.scandir(scoreboard_dir):
        scoreboard = pd.read_csv(filename.path) # reads in scoreboard
        game_id = re.search('[0-9]+', str(filename.path)) # grabs game id from filename
        if game_id:
            game_id = game_id.group()
            game_id_array = np.full(((scoreboard.index).shape), game_id) # creates a game_id key to create database
            scoreboard['game_id'] = game_id_array
        try:
            current_misc = misc_dir +'/' + game_id + '_misc.csv'
            misc = pd.read_csv(current_misc)
            scoreboard = split_tn_ign(scoreboard)
            try:
                scoreboard[['patch']] = np.full(((scoreboard.index).shape), misc['patch'].values)
            except:
                print(f"Patch information not in Misc: {misc}")
                continue
            try:
                scoreboard['date'] = np.full(((scoreboard.index).shape), misc['date'].values)
            except:
                print(f"Date information not in Misc: {misc}")
                continue
            try:
                scoreboard['map'] = np.full(((scoreboard.index).shape), misc['map'].values)
            except:
                print(f"Map information not in Misc: {misc}")
                continue
            #scoreboard = add_winner(scoreboard, misc)
            #scoreboard = add_full_team(scoreboard, misc)
        except OSError as e:
            print('File does not exist')
            print()
            print("Misc Dir: "+ misc_dir)
            print()
            print("Scoreboard Dir: "+ filename.path)
            print()
            print(game_id)
            print()
            print('These should all match.')
            continue
        scoreboard.to_csv('clean/'+str(game_id)+'.csv')

            # print('Made it through once')
            #complete_scoreboard = pd.concat([complete_scoreboard, scoreboard])
    #complete_scoreboard.to_csv('complete_scoreboard.csv')

def add_full_team(scoreboard, misc):
    t1 = misc['team_one'].values
    t2 = misc['team_two'].values
    team_array = np.array([t1,t1,t1,t1,t1,t2,t2,t2,t2,t2])
    scoreboard['full_tn'] = team_array
    #print(misc)
    t1_d = misc['t1_t_rounds'].values
    t1_o = misc['t1_ct_rounds'].values
    t2_d = misc['t2_ct_rounds'].values
    t2_o = misc['t2_t_rounds'].values
    d_rounds_array = np.array([t1_d, t1_d, t1_d, t1_d, t1_d, t2_d, t2_d, t2_d, t2_d, t2_d])
    o_rounds_array = np.array([t1_o, t1_o, t1_o, t1_o, t1_o, t2_o, t2_o, t2_o, t2_o, t2_o])
    scoreboard['offense_rounds_won'] = o_rounds_array
    scoreboard['defense_rounds_won'] = d_rounds_array
    return scoreboard

def split_tn_ign(scoreboard):
    team_names = np.array([])
    igns = np.array([])
    for team_and_ign in scoreboard['IGN'].values:
        team_names = np.append(team_names, team_and_ign.split()[1])
        igns = np.append(igns, team_and_ign.split()[0])
    scoreboard = scoreboard.drop(columns=['IGN', 'Unnamed: 0'])
    scoreboard.insert(loc = 0,
                      column = 'team',
                      value = team_names)
    scoreboard.insert(loc = 1,
                      column = 'IGN',
                      value = igns)
    scoreboard['team'] = team_names
    scoreboard['ign'] = igns
    return scoreboard

def add_winner(scoreboard_df, misc_df):
    scoreboard_df['team_one_win'] = np.full(((scoreboard_df.index).shape), int(misc_df['t1_ct_rounds'] + misc_df['t1_t_rounds']) > (misc_df['t2_ct_rounds'] + misc_df['t2_t_rounds']))
    scoreboard_df['team_two_win'] = np.full(((scoreboard_df.index).shape), int(misc_df['t1_ct_rounds'] + misc_df['t1_t_rounds']) < (misc_df['t2_ct_rounds'] + misc_df['t2_t_rounds']))
    if scoreboard_df['team_one_win'].values.all(): # team one won
        win_label = np.array([1,1,1,1,1,0,0,0,0,0])
    else:
        win_label = np.array([0,0,0,0,0,1,1,1,1,1])
    scoreboard_df['game_result'] = win_label
    return scoreboard_df


process_files()