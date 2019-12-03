import pandas as pd
from nba_api.stats.static import teams, players
from nba_api.stats.endpoints import leaguegamefinder, playbyplayv2, playbyplay
import logging
from datetime import datetime
from airflow_dags.scraping_utilities.utilities import get_proxy_cycle
from retry import retry
from typing import List
from doltpy.etl import get_df_table_writer, get_table_transfomer, get_dolt_loader
from doltpy.core import Dolt
from functools import partial
import json

logger = logging.getLogger(__name__)
proxy_cycle = get_proxy_cycle()
logger.setLevel(logging.DEBUG)


def games_to_play_by_play(games_df: pd.DataFrame):
    game_ids = set([str(el) for el in games_df['GAME_ID'].to_list()])
    return _get_play_by_play(list(game_ids))


def get_games_for_date_builder(game_date: datetime):
    def inner(repo: Dolt):
        raw = repo.read_table('games')
        return raw[raw['GAME_DATE'] == game_date.strftime('%Y-%m-%d')]

    return inner


def _get_play_by_play(game_ids: List[str]):
    result = []

    # Note that the play-by-play endpoint requires that we
    for game_id in game_ids:
        # logger.info('Getting game play-by-play data for game ID {}'.format(game_id))
        print('Getting game play-by-play data for game ID {}'.format(game_id))
        result.append(_get_play_by_play_for_game(game_id))

    return pd.concat(result)


@retry(delay=1, backoff=2, max_delay=60)
def _get_play_by_play_for_game(game_id: str):
    # v2 of the play-by-play endpoint is valid only for games with 10 digit game IDs, which is a super set of the old
    # API, thus we try for a 10 digit game ID, and then fall back onto the original before combining the data.
    if len(game_id) == 10:
        # temp for backfill
        # result = playbyplayv2.PlayByPlayV2(game_id, proxy=next(proxy_cycle))
        return pd.DataFrame()
    else:
        # try:
        result = playbyplayv2.PlayByPlayV2(game_id, proxy=next(proxy_cycle))
        # except json.decoder.JSONDecodeError as _:
        #     logger.warning('N')
        #     return pd.DataFrame()
        # temp
        dfs = result.get_data_frames()
        if len(dfs) != 2:
            raise ValueError('Got a bad result from API endpoint for game ID {}'.format(game_id))

    return dfs[0]


def get_games_df_builder(date_from: datetime, date_to: datetime):
    def inner():
        result = []
        for team in teams.get_teams():
            team_name, team_id = team['full_name'], team['id']
            # logger.info('Retrieving data for team {}, ID {}'.format(team_name, team_id))
            print('Retrieving data for team {}, ID {}'.format(team_name, team_id))
            result.append(_get_games_for_team(team_id, date_from, date_to))

        return pd.concat(result)

    return inner


# def _get_score_board_helper(game_date: datetime):
#
#     scoreboardv2.ScoreboardV2(game_date=game_date, league_id=LeagueID.nba)

@retry(delay=1, backoff=2, max_delay=60)
def _get_games_for_team(team_id: str, date_from: datetime, date_to: datetime):
    gamefinder = leaguegamefinder.LeagueGameFinder(team_id_nullable=team_id,
                                                   date_from_nullable=date_from,
                                                   date_to_nullable=date_to,
                                                   proxy=next(proxy_cycle))
    dfs = gamefinder.get_data_frames()
    if not dfs:
        logger.warning('Missing data for team with ID {}'.format(team_id))
        return pd.DataFrame()
    if len(dfs) > 1:
        raise ValueError('Endpoint returned more than one DataFrame object')

    return dfs[0]


# def get_backfill_loader():
#     table_writers = []
#     get_games()

# Method for combining team games, lifted from here
# https://github.com/swar/nba_api/blob/master/docs/examples/Finding%20Games.ipynb

def get_game_table_loaders(date_from: datetime, date_to: datetime):
    games_loaders = [get_df_table_writer('games', get_games_df_builder(date_from, date_to), ['GAME_ID', 'TEAM_ID'])]
    return [get_dolt_loader(games_loaders, True, 'Append games between {} and {}'.format(date_from, date_to))]


def get_play_by_play_table_loaders(game_date: datetime):
    play_by_play_loaders = [get_table_transfomer(get_games_for_date_builder(game_date),
                                                 'play_by_play',
                                                 # figure out primary key for play-by-play
                                                 [],
                                                 games_to_play_by_play)]
    return [get_dolt_loader(play_by_play_loaders,
                            True,
                            'Updated play_by_play for game date {}'.format(game_date.strftime('%Y-%m-%d')))]
