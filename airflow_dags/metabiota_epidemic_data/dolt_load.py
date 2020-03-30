from doltpy.etl import insert_unique_key, dolt_loader
import pandas as pd
import os
from doltpy.core import Dolt
from subprocess import Popen, PIPE, STDOUT
from typing import List
import itertools
# Attempt 1: import tables as is
#
# Initial table create
#   - dolt schema import --create --dry-run dictionary --pks Variable ../metabiota-epidemic-data/incidence/dictionary.csv
# Have to prepare data by injecting a hash_id
#   - data dir: $DATA_DIR, run Python to inject hash
#       - prepare_data($DATA_DIR/incidence/ebola, "ebola", "prepared")
#       - prepare_data($DATA_DIR/incidence/ncov", "data_ncov2019.csv", "prepared")
#   - run schema import to get schema, we need to change some types
#       - dolt schema import --dry-run --create --pks hash_id ebola $DATA_DIR/incidence/ebola/prepared_data_drc2018.csv
#       - dolt schema import --dry-run --create --pks hash_id ncov $DATA_DIR/incidence/ncov/prepared_data_ncov2019.csv
#   - switch date cols to DATETIME
#       - switch types in SQL produced by schema import commands
#       - execute schema import commands
#   - when fails (with "missing value")
#       - compare schema to and find columns that have been removed
#       - drop columns
#   - run script
#
# Issues
#   - PK deletes don't work (fatal)
#   - clunky, context switching between shell, Python, and SQL
def prepare_data(directory: str, name: str, prefix: str) -> str:
    data = pd.read_csv(os.path.join(directory, name))
    with_pk = insert_unique_key(data)
    new_path = os.path.join(directory, '{}_{}'.format(prefix, name))
    with_pk.to_csv(new_path, index=False)
    return new_path


def update_data_table(repo: Dolt, file: str, table: str):
    data = pd.read_csv(file)
    with_pk = insert_unique_key(data)
    with_dates = with_pk.assign(DATE_LOW=pd.to_datetime(data['DATE_LOW']),
                             DATE_HIGH=pd.to_datetime(data['DATE_HIGH']),
                             DATE_REPORT=pd.to_datetime(data['DATE_REPORT']),
                             CUMULATIVE_FLAG=data['CUMULATIVE_FLAG'].astype(int)
                             )
    repo.import_df(table, with_dates, ['hash_id'], import_mode='update')


# def update_for_commmit(commit_id: str, dolt_dir: str, git_dir: str):
#     base_dir = '/Users/oscarbatori/Documents/metabiota-epidemic-data/incidence'
#     try:
#         print('Checking out at commit {}'.format(commit_id))
#         proc = Popen(args=['git', 'checkout', commit_id], cwd=git_dir, stdout=PIPE, stderr=PIPE)
#         out, err = proc.communicate()
#         exitcode = proc.returncode
#     except:
#         print(err)
#
#     repo = Dolt(dolt_dir)
#     print('Checked out, updating dictionary')
#     dictionary = pd.read_csv(os.path.join(base_dir, 'dictionary.csv'))
#     repo.import_df('dictionary', dictionary, ['Variable'], 'update')
#     print('updating dictionary sources')
#     dictionary_sources = pd.read_csv(os.path.join(base_dir, 'dictionary_source.csv'))
#     repo.import_df('dictionary_sources', dictionary_sources, ['EVENT_NAME'], 'update')
#     print('Updating NCOV')
#     update_data_table(repo, '{}/ncov/data_ncov2019.csv'.format(base_dir), 'ncov')
#     print('Updating Ebola')
#     update_data_table(repo, '{}/ebola/data_drc2018.csv'.format(base_dir), 'ebola')
#
#     new, changes = repo.get_dirty_tables()
#     for changed, staged in changes.items():
#         if not staged:
#             print('Staging {}'.format(changed))
#             repo.add_table_to_next_commit(changed)
#         print("Committing")
#         repo.commit('Updated data for commit ID {}'.format(commit_id))
#     else:
#         print("Nothing to commit")

# Attempt 2: create more logical DB structure
#
# Schema design
#   - run schema import dry run to get schema to modify
#       - manually tweak SQL to get schema right
#       - execute
#   - two tables for ncov (same for ebola)
#       - ncov_events (has hash_id, count)
#       - ncov_events_detail (has FK to ncov_events ncov_events_hash_id, hash_id, count)
#   - update procedure for new data
#       - drop any hash_id not in ncov_events
#       - drop any ncov_event_detail with ncov_events_hash_id droped value
#   - implement Python code to do this
#
# Initial import
#   - import dictionary
#       - dolt schema import --create dictionary --pks Variable ../metabiota-epidemic-data/incidence/dictionary.csv
#       - dolt table import --update-table dictionary ../metabiota-epidemic-data/incidence/dictionary.csv
#   - prepare files for schema import
#       - directory: , event table: , detail table:
#       - pk cols:
#       - prepare_initial_load(')
#       - repeat for ebola
#   - run schema import on files to get SQL (needs to be manually edited)
#       - dolt schema import --dry-run --create ncov_events --pks hash_id ../metabiota-epidemic-data/incidence/ncov/ncov_events.csv
#       - dolt schema import --dry-run --create ncov_event_detail --pks hash_id ../metabiota-epidemic-data/incidence/ncov/ncov_event_detail.csv
#       - repeat for ebola
#           - dolt schema import --dry-run --create eobla_events --pks hash_id ../metabiota-epidemic-data/incidence/ebola/ebola_events.csv
#           - dolt schema import --dry-run --create eobla_event_detail --pks hash_id ../metabiota-epidemic-data/incidence/ebola/ebola_event_detail.csv
#   - update schema to capture date types (edit SQL, change date cols to DATETIME)
#   - execute SQL in shell
#   - run imports
#       - dolt table import --update-table ncov_events ../metabiota-epidemic-data/incidence/ncov/ncov_events.csv
#       - dolt table import --update-table ncov_event_detail ../metabiota-epidemic-data/incidence/ncov/ncov_event_detail.csv
#       - repeat for ebola
#           - dolt table import --update-table ebola_events ../metabiota-epidemic-data/incidence/ebola/ebola_events.csv
#           - dolt table import --update-table ebola_event_detail ../metabiota-epidemic-data/incidence/ebola/ebola_event_detail.csv
# Things that went wrong
#   - I needed Tim's help to model data properly, counldn't just import CSV
#   - constant switching between Python and shell, SQL shell, but couldn't do without either
#   - a priori knowledge of shortcomings of schema inference required and have to know how to correct
#   - had to write code to implement relatively complicated update semantics
#   - mistake in initial schema required literally tearing the repo down and starting again, changing column name painful

# Update
#   - schema changes will break import
#       - open data file and see what changed
#       - open SQL shell and adjust schema
#       - retry
#   - underlying data file can change schema
#       - run update data
#   - adding tables requires modifying code, at they add a table called dictionary sources
#       - dolt schema import --create dictionary_sources --pks EVENT_NAME,SOURCE ../metabiota-epidemic-data/incidence/dictionary_sources.csv
#       -


# Need to be updated for schema changes
DATA_FILE_PK_COLS = ['EVENT_NAME',
                     'SOURCE',
                     'DATE_LOW',
                     'DATE_HIGH',
                     'DATE_REPORT',
                     'DATE_TYPE',
                     'SPATIAL_RESOLUTION',
                     'AL0_CODE',
                     'AL0_NAME',
                     'AL1_CODE',
                     'AL1_NAME',
                     'AL2_NAME',
                     'AL3_NAME',
                     'LOCALITY_NAME',
                     'LOCATION_TYPE']


# Prepare initial data
def prepare_initial_load(directory: 'str', data_file: str, pk_cols: List[str], events_table: str, event_detail_table):

    def get_path(table: str):
        return os.path.join(directory, '{}.csv'.format(table))

    data = read_data_file(os.path.join(directory, data_file))
    events, detail = extract_detail(data, pk_cols, events_table)
    events.to_csv(get_path(events_table), index=False)
    detail.to_csv(get_path(event_detail_table), index=False)
    return get_path(events_table), get_path(event_detail_table)


def update_for_commmit(commit_id: str, dolt_dir: str, git_dir: str):
    base_dir = '/Users/oscarbatori/Documents/metabiota-epidemic-data/incidence'
    try:
        print('Checking out at commit {}'.format(commit_id))
        proc = Popen(args=['git', 'checkout', commit_id], cwd=git_dir, stdout=PIPE, stderr=PIPE)
        out, err = proc.communicate()
        exitcode = proc.returncode
        print(out)
    except:
        print(err)
        raise

    repo = Dolt(dolt_dir)

    print('Checked out, updating dictionary')
    dictionary = pd.read_csv(os.path.join(base_dir, 'dictionary.csv'))
    repo.import_df('dictionary', dictionary, ['Variable'], 'update')
    print('updating dictionary sources')
    dictionary_sources = pd.read_csv(os.path.join(base_dir, 'dictionary_source.csv'))
    repo.import_df('dictionary_sources', dictionary_sources, ['EVENT_NAME'], 'update')

    update_helper(repo,
                  '/Users/oscarbatori/Documents/metabiota-epidemic-data/incidence/ncov',
                  'data_ncov2019.csv',
                  'ncov_events',
                  'ncov_event_detail')

    # update_helper(repo,
    #               '/Users/oscarbatori/Documents/metabiota-epidemic-data/incidence/ebola',
    #               'data_drc2018.csv',
    #               'ebola_events',
    #               'ebola_event_detail')

    new, changes = repo.get_dirty_tables()
    for changed, staged in changes.items():
        if not staged:
            print('Staging {}'.format(changed))
            repo.add_table_to_next_commit(changed)

    if changes:
        print("Committing")
        repo.commit('Updated data for commit ID {}'.format(commit_id))
    else:
        print("Nothing to commit")


def update_helper(repo: Dolt, directory: str, data_file: str, events_table: str, event_detail_table: str):
    print('Updating {} and {}'.format(events_table, event_detail_table))
    data = read_data_file(os.path.join(directory, data_file))
    events, event_detail = extract_detail(data, DATA_FILE_PK_COLS, events_table)
    update_table(repo, events_table, 'hash_id', events)
    update_table(repo, event_detail_table, 'hash_id', event_detail)


# This function ensures that for a table with a hash_id PK, we drop deleted records, replace updated ones, and append
# new ones.
def update_table(repo: Dolt, table: str, pk: str, data: pd.DataFrame):
    # Get dropped pks
    if table not in repo.get_existing_tables():
        raise ValueError('Missing table')

    existing = repo.read_table(table)
    existing_pks = existing[pk].to_list()

    # Get proposed pks
    proposed_pks = data[pk].to_list()
    to_drop = [existing for existing in existing_pks if existing not in proposed_pks]

    print('''Table {}
        - existing      : {}
        - proposed      : {}
        - to drop       : {}
    '''.format(table, len(existing_pks), len(proposed_pks), len(to_drop)))

    # Generate and execute SQL drop statement
    if to_drop:
        iterator = iter(to_drop)
        while iterator:
            batch = list(itertools.islice(iterator, 3000))
            if len(batch) == 0:
                break

            print('Dropping batch of {} IDs from {}'.format(len(batch), table))
            drop_statement = '''
            DELETE FROM {table} WHERE {pk} in ("{pks_to_drop}")
            '''.format(table=table, pk=pk, pks_to_drop='","'.join(batch))
            repo.execute_sql_stmt(drop_statement)

    # Import the rest of the data
    new_data = data[~(data[pk].isin(existing_pks))]
    if not new_data.empty:
        print('Importing {} records'.format(len(new_data)))
        repo.import_df(table, new_data, [pk], 'update')
    else:
        print("No new data to import")


def extract_detail(data: pd.DataFrame, pk_cols: List[str], pk_table_name: str):
    # Create PK table
    pk_data = data[pk_cols].drop_duplicates()
    pks_with_hash = insert_unique_key(pk_data)

    # Insert hash into data for details table
    pk_indexed_data = data.set_index(pk_cols)
    pk_indexed_pks_with_hash = pks_with_hash.set_index(pk_cols)
    pk_indexed_data.loc[pk_indexed_pks_with_hash.index,
                        '{}_hash_id'.format(pk_table_name)] = pk_indexed_pks_with_hash['hash_id']

    # Extract details table with FK, and insert unique hash
    detail_with_fk = pk_indexed_data.reset_index().drop(pk_cols, axis=1)
    detail_with_hash_index = insert_unique_key(detail_with_fk)

    return pks_with_hash.reset_index(drop=True), detail_with_hash_index


def read_data_file(path: str):
    data = pd.read_csv(path)
    clean = data.assign(DATE_LOW=pd.to_datetime(data['DATE_LOW']),
                        DATE_HIGH=pd.to_datetime(data['DATE_HIGH']),
                        DATE_REPORT=pd.to_datetime(data['DATE_REPORT']),
                        CUMULATIVE_FLAG=data['CUMULATIVE_FLAG'].astype(int))

    return clean
