import pandas as pd
from nba_api.stats.static import teams, players
from nba_api.stats.endpoints import leaguegamefinder, playbyplayv2
import logging
from datetime import datetime
from airflow_dags.scraping_utilities.utilities import get_proxy_cycle
from retry import retry
from typing import List

logger = logging.getLogger(__name__)
proxy_cycle = get_proxy_cycle()
logger.setLevel(logging.DEBUG)


def get_play_by_play(game_ids: List[str]):
    result = []

    # Note that the play-by-play endpoint requires that we
    for game_id in [game_id for game_id in game_ids if len(game_id) == 10]:
        # logger.info('Getting game play-by-play data for game ID {}'.format(game_id))
        print('Getting game play-by-play data for game ID {}'.format(game_id))
        result.append(_get_play_by_play_for_game(game_id))

    return pd.concat(result)


@retry(delay=1, backoff=2, max_delay=60)
def _get_play_by_play_for_game(game_id: str):
    result = playbyplayv2.PlayByPlayV2(game_id, proxy=next(proxy_cycle))
    dfs = result.get_data_frames()
    if len(dfs) != 2:
        raise ValueError('Got a bad result from API endpoint for game ID {}'.format(game_id))

    return dfs[0]


def get_games(date_from: datetime, date_to: datetime):
    result = []
    for team in teams.get_teams():
        team_name, team_id = team['full_name'], team['id']
        # logger.info('Retrieving data for team {}, ID {}'.format(team_name, team_id))
        print('Retrieving data for team {}, ID {}'.format(team_name, team_id))
        result.append(_get_games_for_team(team_id, date_from, date_to))

    return pd.concat(result)


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