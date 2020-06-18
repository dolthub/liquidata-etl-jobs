from datetime import datetime
import os
import pandas as pd
from typing import List, Tuple, Any, Callable, Union
import re
import argparse
from doltpy.core import Dolt
from doltpy.core.write import import_list
from doltpy.etl.loaders import insert_unique_key
import logging
import importlib
import sys
from pprint import pprint

logger = logging.getLogger(__name__)

# Table of elections across all states,
ELECTIONS_RAW_COLS = [
    'year',
    'date',
    'election',
    'special',
    'office',
    'district',
    'state'
]



PRECINCT_VOTE_PKS = ['election_id', 'precinct', 'party', 'candidate']
COUNTY_VOTE_PKS = ['election_id', 'county', 'party', 'candidate']

DEFAULT_PK_VALUE = 'NA'


def ensure_election_pk_not_null(df: pd.DataFrame) -> pd.DataFrame:
    """
    Sometimes district and office are not set, breaking a write, we set them to a default value.
    :param df:
    :return:
    """
    def mapper(value: Any):
        if pd.isna(value):
            return DEFAULT_PK_VALUE
        elif type(value) in (float, int):
            return str(int(value))
        elif type(value) == str:
            return value
        else:
            raise ValueError('The value {} is not valid for district column'.format(value))

    if 'district' in df:
        return df.assign(district=df['district'].map(mapper), office=df['office'].map(mapper))

    else:
        return df


def clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Some columns have erroneous capitalization and trailing spaces, we remove those.
    :param df:
    :return:
    """
    temp = df.copy()

    for column in temp.columns:
        if column.startswith('Unnamed'):
            temp = temp.drop([column], axis=1)

    return temp.rename(columns={col: col.rstrip().lower() for col in temp.columns})


class VoteFile:

    DATE_POS = 0
    STATE_POS = 1
    ELECTION_POS = 2

    DEFAULT_TRANSFORMERS = [clean_column_names, ensure_election_pk_not_null]

    def __init__(self,
                 path: str,
                 state: str,
                 year: int,
                 date: datetime,
                 election: str,
                 is_special: bool,
                 voter_count_cols: List[str],
                 df_transformers: List[Callable[[pd.DataFrame], pd.DataFrame]]):
        self.path = path
        self.date = date
        self.state = state
        self.election = election
        self.year = year
        self.is_special = is_special
        self.voter_count_cols = voter_count_cols
        self.df_transformers = self.DEFAULT_TRANSFORMERS + df_transformers if df_transformers else self.DEFAULT_TRANSFORMERS

    def to_enriched_df(self) -> pd.DataFrame:
        logger.info('Parsing file {}'.format(self.path))
        try:
            df = pd.read_csv(self.path)
        except (pd.errors.ParserError, UnicodeDecodeError) as e:
            logger.error(str(e))
            return pd.DataFrame()
        deduplicated = df.drop_duplicates(keep='first')
        # Add some columns that we extracted from the filepath, and the filepath for debugging
        enriched_df = deduplicated.assign(state=self.state.upper(),
                                          year=self.year,
                                          date=self.date,
                                          election=self.election,
                                          special=self.is_special,
                                          filepath=self.path)

        temp = enriched_df.copy()
        if self.df_transformers:
            for transformer in self.df_transformers:
                temp = transformer(temp)

        return temp


class PrecinctFile(VoteFile):
    SPLIT_NAME_LENGTHS = [4, 5]


class CountyFile(VoteFile):
    SPLIT_NAME_LENGTHS = [3]


class OfficeFile(VoteFile):
    SPLIT_NAME_LENGTHS = [4]


class StateMetadata:

    def __init__(self,
                 source_dir: Union[None, str],
                 state: str,
                 vote_count_cols: List[str] = None,
                 df_transformers: List[Callable[[pd.DataFrame], pd.DataFrame]] = None,
                 row_cleaners: Callable[[dict], dict] = None):
        self._source_dir = source_dir
        self.state = state
        self.vote_count_cols = vote_count_cols
        self.df_transformers = df_transformers
        self.row_cleaners = row_cleaners

    @property
    def source_dir(self):
        return self._source_dir

    def set_source_dir(self, value: str):
        self._source_dir = value


def load_to_dolt(dolt_dir: str,
                 state_metadata: StateMetadata,
                 load_elections: bool,
                 load_precinct_votes: bool,
                 load_county_votes: bool):
    all_elections, all_precinct_votes, all_county_votes = parse_for_state(state_metadata)

    repo = Dolt(dolt_dir)

    if load_elections:
        logger.info('Loading election data to "elections" in Dolt repo in {}'.format(dolt_dir))
        import_list(repo, 'elections', all_elections, import_mode='update')
    if load_precinct_votes:
        logger.info('Loading voting data to "{}_precinct_votes" in Dolt repo in {}'.format(state_metadata.state,
                                                                                           dolt_dir))
        import_list(repo,
                    '{}_precinct_votes'.format(state_metadata.state),
                    all_precinct_votes,
                    import_mode='update',
                    chunk_size=100000)
    if load_county_votes:
        logger.info('Loading voteing data to "{}_county_votes" in Dolt repo in {}'.format(state_metadata.state,
                                                                                          dolt_dir))
        import_list(repo, '{}_county_votes'.format(state_metadata.state), all_county_votes, import_mode='update')


def print_columns(state_metadata: StateMetadata):
    all_county_data, all_precinct_data = build_state_dataframes(state_metadata)

    def filter_cols(cols: List[str], pks: List[str]):
        return [col for col in cols if col not in pks and col != 'filepath' and col not in ELECTIONS_RAW_COLS]

    potential_county_vote_cols = filter_cols(all_county_data.columns, COUNTY_VOTE_PKS)
    potential_precinct_vote_cols = filter_cols(all_precinct_data.columns, PRECINCT_VOTE_PKS)

    logger.info('Printing raw column names for state {} in source die {}'.format(state_metadata.state,
                                                                                 state_metadata.source_dir))
    output = '''
Potential vote count columns for county data are:
    {}
{}_county_votes should have PK {}

Potential vote count columns for precinct data are:
    {}
{}_county_votes should have PK {}

This columns should be put into metadata in the state's module.
    '''.format(potential_county_vote_cols,
               state_metadata.state,
               COUNTY_VOTE_PKS,
               potential_precinct_vote_cols,
               state_metadata.state,
               PRECINCT_VOTE_PKS,
               'airflow_dags.open_elections.{}.py'.format(state_metadata.state)).lstrip()

    logger.info(output)


def parse_for_state(state_metadata: StateMetadata) -> Tuple[List[dict], List[dict], List[dict]]:
    all_county_data, all_precinct_data = build_state_dataframes(state_metadata)

    all_county_votes     = pd.DataFrame()
    all_county_elections = pd.DataFrame()
    if not all_county_data.empty:
        logger.info('Extracting election and vote tables from county data')
        all_county_elections, all_county_votes = extract_tables(all_county_data, state_metadata, False)
    logger.info('Extracting election and vote tables from precinct data')
    all_precinct_elections, all_precinct_votes = extract_tables(all_precinct_data, state_metadata, True)

    logger.info('De-duplicating election for statewide election table')
    all_elections = deduplicate_elections(all_county_elections, all_precinct_elections)

    return all_elections, all_precinct_votes, all_county_votes


def build_state_dataframes(state_metadata: StateMetadata) -> Tuple[pd.DataFrame, pd.DataFrame]:
    precinct_vote_objs, county_votes_objs = build_file_objects(state_metadata)

    logger.info('Combining DataFrame objects from individual files in directory {}'.format(state_metadata.source_dir))
    if precinct_vote_objs:
        all_precinct_data = pd.concat([precinct_vote_obj.to_enriched_df() for precinct_vote_obj in precinct_vote_objs])
    else:
        all_precinct_data = pd.DataFrame()
    if county_votes_objs:
        all_county_data = pd.concat([county_votes_obj.to_enriched_df() for county_votes_obj in county_votes_objs])
    else:
        all_county_data = pd.DataFrame()

    # Some NY county data has precinct in it
    if 'precinct' in all_county_data:
        all_county_data = all_county_data.drop(columns=['precinct'])

    return all_county_data, all_precinct_data


def build_vote_file_object(year: int,
                           path: str,
                           file_name: str,
                           vote_count_cols: List[str],
                           df_transformers: List[Callable[[pd.DataFrame], pd.DataFrame]]):
    split = file_name.split('.')[0].split('__')

    special = 'special' in split
    if special:
        split.remove('special')

    # Deals with the case of files formatted like:
    #   20160913__ny__republican__primary__richmond__precinct.csv
    # since it will have election type 'primary' and party is populated, this is redundant
    if 'primary' in split:
        if 'republican' in split:
            split.remove('republican')
        if 'democrat' in split:
            split.remove('democrat')

    if len(split) in PrecinctFile.SPLIT_NAME_LENGTHS:
        file_type = PrecinctFile
    elif len(split) in CountyFile.SPLIT_NAME_LENGTHS:
        file_type = CountyFile
    elif len(split) in OfficeFile.SPLIT_NAME_LENGTHS:
        file_type = OfficeFile
    else:
        raise ValueError('File with name {} cannot be processed'.format(file_name))

    state = split[file_type.STATE_POS]
    date_str = split[file_type.DATE_POS]
    election = split[file_type.ELECTION_POS]

    return file_type(os.path.join(path, file_name),
                     state,
                     year,
                     datetime.strptime(date_str, '%Y%m%d'),
                     election,
                     special,
                     vote_count_cols,
                     df_transformers)


def gather_files(base_dir: str) -> List[Tuple[int, str, str]]:
    year_dirs = [(os.path.join(base_dir, subdir), subdir)
                 for subdir in os.listdir(base_dir) if re.match(r'\d\d\d\d', subdir)]

    result = []
    for year_dir, year in year_dirs:
        for dirpath, _, filenames in os.walk(year_dir):
            for filename in filenames:
                if filename.endswith('csv'):
                    result.append((int(year), dirpath, filename))

    return result


def build_file_objects(state_metadata: StateMetadata) -> Tuple[List[CountyFile], List[PrecinctFile]]:
    files = gather_files(state_metadata.source_dir)

    precinct_votes, county_votes = [], []
    for year, dirpath, filename in files:
        result = build_vote_file_object(year,
                                        dirpath,
                                        filename,
                                        state_metadata.vote_count_cols,
                                        state_metadata.df_transformers)
        if type(result) == PrecinctFile:
            precinct_votes.append(result)
        elif type(result) == CountyFile:
            county_votes.append(result)

    # return precinct_votes, county_votes
    return precinct_votes, county_votes


def extract_tables(enriched_df: pd.DataFrame, state_metadata: StateMetadata, precinct: bool) -> Tuple[List[dict], List[dict]]:
    # Grab the nationwide election table data, insert a unique row key, and index it on unique attributes
    elections_df = enriched_df[ELECTIONS_RAW_COLS]
    keyed_elections = insert_unique_key(elections_df).drop_duplicates()
    keyed_elections_with_index = keyed_elections.set_index(ELECTIONS_RAW_COLS)

    # Index the original DataFrame with unique vote counts by unique election attributes
    enriched_with_index = enriched_df.set_index(ELECTIONS_RAW_COLS)

    # Left join the elections back to the enriched frame containing votes so each vote count has an election ID
    # and the rename the column and drop the row count from the election table
    vote_counts_with_election_id = enriched_with_index.merge(keyed_elections_with_index,
                                                             left_index=True,
                                                             right_index=True,
                                                             how='left').reset_index(drop=True)
    clean_votes = (vote_counts_with_election_id
                   .rename(columns={'hash_id': 'election_id'})
                   .drop(columns=['count', 'filepath']))

    if precinct:
        clean_votes = clean_votes.drop(columns=['county']).drop_duplicates(subset=PRECINCT_VOTE_PKS)
    else:
        clean_votes = clean_votes.drop_duplicates(subset=COUNTY_VOTE_PKS)

    # Convert the frames to dictionaries and remove Panda Poop
    votes = votes_df_to_dicts(clean_votes, state_metadata.vote_count_cols)
    elections_dict = election_df_to_dict(keyed_elections)
    return elections_dict, votes


def election_df_to_dict(df: pd.DataFrame) -> List[dict]:
    dicts = df.to_dict('records')

    result = []
    for dic in dicts:
        for key, value in dic.items():
            if key == 'office':
                if value is None or pd.isna(value):
                    dic[key] = DEFAULT_PK_VALUE
            elif pd.isna(value):
                dic[key] = None

        result.append(dic)

    return result


def votes_df_to_dicts(votes_df: pd.DataFrame, vote_count_cols: List[str]) -> List[dict]:
    dicts = votes_df.to_dict('records')

    result = []
    for dic in dicts:
        for key, value in dic.items():
            # This long conditional basically fixes all the types so that they are correct and compatible with SQL.
            # It's gross but it basically amounts to a way to write down a series of rules that ensure the data will be
            # correct. The rest of the code is supposed to be clean, simple transformations, and all the ugliness and
            # special cases packed into here.
            if key in ('election_id', 'state', 'year', 'date', 'election', 'special', 'election'):
                # We injected these columns so we don't need to worry about the type
                pass
            elif key in ('candidate', 'precinct', 'county', 'party'):
                if value is None or pd.isna(value):
                    dic[key] = DEFAULT_PK_VALUE
            elif key == 'election_id':
                pass
            elif key == 'polling':
                if pd.isna(value):
                    dic[key] = None
            elif key in vote_count_cols:
                if type(value) == str:
                    if value in ('X', '-', '', 'S'):
                        dic[key] = None
                    else:
                        dic[key] = int(value.replace(',', '').replace('*', ''))
                elif pd.isna(value):
                    dic[key] = None
                elif type(value) == int:
                    pass
                elif type(value) == float:
                    dic[key] = int(value)
                else:
                    raise ValueError('Value {} is not valid or vote ("{}") column'.format(value, key))
            elif key == 'polling':
                pass
            else:
                pass
                # raise ValueError('Encountered unknown column {}'.format(key))

        result.append(dic)

    return result


def deduplicate_elections(all_county_elections: List[dict], all_precinct_elections: List[dict]) -> List[dict]:
    result = []
    seen = set()

    def inner(dict_list: List[dict]):
        for el in dict_list:
            if el['hash_id'] not in seen:
                seen.add(el['hash_id'])
                result.append(el)

    inner(all_county_elections)
    inner(all_precinct_elections)

    return result


def deduplicate_votes(votes: List[dict], primary_keys: List[str]):
    result = []
    seen = set()

    for el in votes:
        hashable = hash(tuple(el[key] for key in primary_keys))
        if hashable not in seen:
            seen.add(hashable)
            result.append(el)

    logger.info('Found {} unique records from {} records'.format(len(seen), len(votes)))
    return result


def build_state_metadata(state: str, base_dir: str, state_module: str = None):
    if state_module:
        try:
            retrieved_module = importlib.import_module(state_module)

            if hasattr(retrieved_module, 'metadata'):
                state_metadata = getattr(retrieved_module, 'metadata')
                state_metadata.set_source_dir(base_dir)
                return state_metadata
        except ModuleNotFoundError as _:
            raise ModuleNotFoundError('Missing module {}, provide module with required attributes'.format(state_module))

    else:
        return StateMetadata(base_dir, state)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--state', type=str, help='State to load data for', required=True)
    parser.add_argument('--base-dir', type=str, help='Git repo containing raw data files', required=True)
    parser.add_argument('--dolt-dir', type=str, help='Dolt repo directory')
    parser.add_argument('--state-module', type=str, help='Module containing information for state')
    parser.add_argument('--load-elections', help='Load elections data', action='store_true')
    parser.add_argument('--load-precinct-votes', help='Load elections data', action='store_true')
    parser.add_argument('--load-county-votes', help='Load elections data', action='store_true')
    parser.add_argument('--show-columns', help='', action='store_true')
    args = parser.parse_args()
    state_metadata = build_state_metadata(args.state, args.base_dir, args.state_module)

    logger.setLevel('INFO')
    logger.addHandler(logging.StreamHandler(sys.stdout))

    if args.show_columns:
        valid = not any([args.load_precinct_votes, args.load_elections, args.load_county_votes])
        assert valid, '--load-* arguments not valid when printing schema'
        logger.info('Printing schema of county and precinct data files for state {} in directory {}'.format(
            state_metadata.state,
            state_metadata.source_dir
        ))
        print_columns(state_metadata)
    else:
        assert args.state_module and args.dolt_dir, 'When loading to Dolt --state-module and --dolt-dir required'
        logger.info('''Loading the following data to Dolt repo in {} from Open Elections data in {}:
            - elections         : {}
            - county votes      : {}
            - precinct votes    : {}   
        '''.format(args.dolt_dir,
                   state_metadata.source_dir,
                   args.load_elections,
                   args.load_precinct_votes,
                   args.load_county_votes))
        load_to_dolt(args.dolt_dir,
                     state_metadata,
                     args.load_elections,
                     args.load_precinct_votes,
                     args.load_county_votes)


if __name__ == '__main__':
    main()
