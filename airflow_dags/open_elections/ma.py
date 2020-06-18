from airflow_dags.open_elections.load_by_state import StateMetadata

VOTE_COUNT_COLS = ['votes']

metadata = StateMetadata(None, 'ma', VOTE_COUNT_COLS, None, None)
