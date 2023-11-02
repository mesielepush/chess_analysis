import os
from dotenv import load_dotenv
import psycopg2
import tempfile
import pandas as pd
import numpy as np
import requests
load_dotenv()

def get_ask_connection():
    CONN_STRING = os.getenv("PSYCOPG2_CONN_STRING")
    return psycopg2.connect(CONN_STRING, port = 5433)
def player_exists_at_db(player_name: str):
    result = open_request(f"""
                    select player_name from player
                    where player_name='{player_name}'
                            """)
    if len(result) == 1:
        return True
    return False
def number_of_games(player_name):
    n_games = open_request(f"""
            SELECT COUNT(game) FROM game
            where game.black = '{player_name}'
            or game.white = '{player_name}';
            """)
    return n_games[0][0]
def open_request(sql_question:str):
    conn = get_ask_connection()
    with conn.cursor() as curs:
        curs.execute(
            sql_question
        )
        result = curs.fetchall()
    return result
def read_sql_tmpfile(query):
    conn = get_ask_connection()
    with tempfile.TemporaryFile() as tmpfile:
        copy_sql = "COPY ({query}) TO STDOUT WITH CSV {head}".format(
           query=query, head="HEADER"
        )
        cur = conn.cursor()
        cur.copy_expert(copy_sql, tmpfile)
        tmpfile.seek(0)
        df = pd.read_csv(tmpfile)
        return df
def get_games_for_month(player_name):
    dates =read_sql_tmpfile(f"""
            SELECT year, month FROM game
            Where game.black = '{player_name}'
            OR game.white = '{player_name}'
            """)
    dates = dates.pivot_table(index = ['year', 'month'], aggfunc ='size')
    dates_hist = np.array(
        [
            [
                f'{x[0]}_{x[1]}',
                int(dates[x])            ]
            for x in dates.index
        ]
    )
    return dates_hist
def post_profile(player_name):
    POST_PLAYER = os.getenv("POST_PLAYER_STRING")
    myobj = {"player_name":player_name}
    response = requests.post(POST_PLAYER, json = myobj)
    response = response.content.decode('utf-8')
    if 'RESPONSE' in response:
        return False
    return True
def read_pd_profile(player_name):
    profile = read_sql_tmpfile(f"""
            SELECT * FROM player
            Where player.player_name = '{player_name}'
            """)
    return profile.to_dict()
def get_profile(player_name):
    player_name = player_name.lower()
    if not player_exists_at_db(player_name):
        response = post_profile(player_name)
        if not response:
            return f'No such player {player_name} exists'
    profile = read_pd_profile(player_name)
    if profile['joined'] == 0:
        post_profile(player_name)
        profile = read_pd_profile(player_name)
    return profile