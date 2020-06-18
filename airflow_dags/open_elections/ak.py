from airflow_dags.open_elections.load_by_state import StateMetadata

PRECINCT_VOTE_PKS = ['election_id', 'precinct', 'party', 'candidate']

PRECINCT_VOTES_COLS = ['votes']

VOTE_COUNT_COLS = ['votes']

df_transformers = [lambda df: df.drop(columns=['community','state_legislative_district','delegates'], errors='ignore')]

metadata = StateMetadata(None,
                         'ak',
                         VOTE_COUNT_COLS,
                         df_transformers=df_transformers)
