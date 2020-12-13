#!/usr/bin/python3

from doltpy.etl import get_df_table_writer, get_dolt_loader, load_to_dolthub
import requests
from bs4 import BeautifulSoup
import pandas as pd
import re

REPO_PATH = 'wikipedia-mirrors/county-fips-codes'
URL = 'https://en.wikipedia.org/wiki/List_of_United_States_FIPS_codes_by_county'


def get_data():
    """ Parse a section of a page, fetch its table data and save it to a CSV file
    """
    text = requests.get(URL).text
    soup = BeautifulSoup(text, 'lxml')
    rows = soup.find_all('tr')
    current = 2
    result = []
    while current <= len(rows):
        state_row = rows[current]
        state_row_cells = list(state_row.find_all('td'))
        if len(state_row_cells) == 0:
            break
        state = state_row_cells[2].text.strip()
        current_section_len = int(state_row_cells[2].get('rowspan', 1))
        for i in range(current, current + current_section_len):
            current_row = rows[i]
            cells = list(current_row.find_all('td'))
            fips = cells[0].text.strip()
            # Remove non UTF-8 characters
            county = cells[1].text.strip().replace('–', '-')
            clean_county = re.sub(r'\[.*\]', '', county)
            result.append(dict(fips=fips, county_or_equivalent=clean_county, state_or_equivalent=state))

        current = current + current_section_len

    return pd.DataFrame(result)


def main():
    table_writer = get_df_table_writer('county_by_fips', get_data, ['fips'], 'update')
    loaders = get_dolt_loader(table_writer, True, 'Update county_by_fips')
    load_to_dolthub(loaders, clone=True, push=True, remote_name='origin', remote_url=REPO_PATH)


if __name__ == '__main__':
    main()
