from airflow_dags.open_elections.load_by_state import StateMetadata

VOTE_COUNT_COLS = ['votes']

df_transformers = [lambda df: df.rename(columns={'parish': 'county'}, errors='ignore')]

metadata = StateMetadata(None, 'la', VOTE_COUNT_COLS, df_transformers=df_transformers)
