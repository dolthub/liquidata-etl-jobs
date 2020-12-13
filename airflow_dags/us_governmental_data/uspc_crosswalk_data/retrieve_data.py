# Some tools for importing USPS Crosswalk data
# Remove null pks
#   df.dropna(subset=['zip', 'cd'])
# drop weird empty string column
# force values to acutal ints where necessary
#   df = df.assign(zip=df['zip'].map(lambda v: int(v)), county=df['cbsa'].map(lambda v: int(v)))

import requests
import os
import pandas as pd
from doltpy.core import Dolt
from doltpy.core.write import import_df
from doltpy.core.system_helpers import get_logger

logger = get_logger(__name__)

crosswalks = [
    "ZIP_TRACT_",
    "ZIP_COUNTY_",
    "ZIP_COUNTY_SUB_",
    "ZIP_CBSA_",
    "ZIP_CBSA_DIV_",
    "ZIP_CD_",
    "TRACT_ZIP_",
    "COUNTY_ZIP_",
    "COUNTY_SUB_ZIP_",
    "CBSA_ZIP_",
    "CBSA_DIV_ZIP_",
    "CD_ZIP_",
]

crosswalk_table_to_pk = {
    "zip_tract": ['zip', 'tract'],
    "zip_county": ['zip', 'county'],
    "zip_county_sub": ['zip', 'county_sub'],
    "zip_cbsa": ['zip', 'cbsa'],
    "zip_cbsa_div":['zip', 'cbsa_div'] ,
    "zip_cd": ['zip', 'cd'],
    "tract_zip": ['zip', 'tract'],
    "county_zip": ['county', 'zip'],
    "county_sub_zip": ['county_sub', 'zip'],
    "cbsa_zip": ['cbsa', 'zip'],
    "cbsa_div_zip": ['cbsa_div', 'zip'],
    "cd_zip": ['cd', 'zip'],
}

quarters = ["032020",
            "122019",
            "092019",
            "062019",
            "032019",
            "122018",
            "092018",
            "062018",
            "032018",
            "122017",
            "092017",
            "062017",
            "032017",
            "122016",
            "092016",
            "062016",
            "032016",
            "122015",
            "092015",
            "062015",
            "032015",
            "122014",
            "092014",
            "062014",
            "032014",
            "122013",
            "092013",
            "062013",
            "032013",
            "122012",
            "092012",
            "062012",
            "032012",
            "122011",
            "092011",
            "062011",
            "032011",
            "122010",
            "092010",
            "062010",
            "032010"]

DOWNLOAD_DIR = '/Users/oscarbatori/usps_crosswalk_data'
BASE_URL = 'https://www.huduser.gov/portal/datasets/usps'
DOLT_REPO = '/Users/oscarbatori/Documents/irs-analysis/dolt-dbs/usps-crosswalk-data'


def download_history():
    for quarter in quarters:
        for crosswalk in crosswalks:
            download_file(quarter, crosswalk)


def get_filename(quarter, crosswalk):
    year = int(quarter[2:])
    if year < 2015 and (crosswalk == 'ZIP_CD_' or crosswalk == 'CD_ZIP_'):
        ext = "xls"
    else:
        ext = "xlsx"

    return '{}{}.{}'.format(crosswalk, quarter, ext)


def download_file(quarter, crosswalk):
    print('Fetching file for {}/{}'.format(quarter, crosswalk))

    filename = get_filename(quarter, crosswalk)
    response = requests.get('{}/{}'.format(BASE_URL, filename))

    if response.status_code == 200:
        filepath = os.path.join(DOWNLOAD_DIR, filename)
        print('Writing data to {}'.format(filepath))
        with open(filepath, 'wb') as f:
            f.write(response.content)


def get_crosswalk_df(crosswalk):
    table_name = crosswalk.lower().rstrip('_')
    result = []
    for quarter in quarters:
        month, year = int(quarter[:2].lstrip('0')), int(quarter[2:])
        filepath = os.path.join(DOWNLOAD_DIR, get_filename(quarter, crosswalk))
        if os.path.exists(filepath):
            df = pd.read_excel(filepath).assign(month=month, year=year)
            df = df.rename(columns={col: col.lower() for col in df.columns})
            df = df.dropna(subset=crosswalk_table_to_pk[table_name])
            df = df.assign(county=df['county'].apply(lambda v: str(int(v)).zfill(5)))
            df = df.assign(zip=df['zip'].apply(lambda v: str(int(v)).zfill(5)))
            if ' ' in df.columns:
                df = df.drop(columns=[' '])

            result.append(df)

    return pd.concat(result)


def load_to_dolt(df, table):
    repo = Dolt(DOLT_REPO)
    import_df(repo, table, df, ['year', 'month'] + crosswalk_table_to_pk[table], import_mode='update')


# from doltpy.core.write import import_df
# from doltpy.core import Dolt
# from airflow_dags.uspc_crosswalk_data.retrieve_data import *
# df = get_crosswalk_df('ZIP_COUNTY_')
# repo = Dolt('/Users/oscarbatori/Documents/irs-analysis/dolt-dbs/usps-crosswalk-data')
# import_df(repo, 'zip_county', df, ['year', 'month'] + ['zip', 'county'], import_mode='update')