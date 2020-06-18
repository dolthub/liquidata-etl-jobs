from airflow_dags.open_elections.load_by_state import StateMetadata

PRECINCT_VOTE_PKS = ['election_id', 'precinct', 'party', 'candidate']

PRECINCT_VOTES_COLS = ['county',
                       'precinct',
                       'office',
                       'district',
                       'party',
                       'candidate',
                       'votes',
                       'state',
                       'year',
                       'date',
                       'election',
                       'special']

COUNTY_VOTE_PKS = ['election_id', 'county', 'party', 'candidate']

COUNTY_VOTE_COLS = ['county',
                    'office',
                    'district',
                    'party',
                    'candidate',
                    'votes',
                    'state',
                    'year',
                    'date',
                    'election',
                    'special']

VOTE_COUNT_COLS = ['votes']

metadata = StateMetadata(None,
                         'al',
                         VOTE_COUNT_COLS)
