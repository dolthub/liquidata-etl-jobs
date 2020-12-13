from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator


def get_default_args_helper(start_date: datetime):
    return {'owner': 'liquidata-etl',
            'depends_on_past': False,
            'start_date': start_date,
            'email': ['airflow@liquidata.co'],
            'email_on_failure': False,
            'email_on_retry': False,
            'catchup': False,
            'retry_delay': timedelta(minutes=5)}


# FX rates DAG
fx_rates_dag = DAG('fx_rates',
                   default_args=get_default_args_helper(datetime(2019, 10, 9)),
                   schedule_interval=timedelta(hours=1))


fx_rates_raw_data = BashOperator(
    task_id='fx_rates_raw',
    bash_command='{{conf.get("core", "dags_folder")}}/fx_rates_example/dolt_load.py --raw ',
    dag=fx_rates_dag
)


fx_rates_averages = BashOperator(
    task_id='fx_rates_averages',
    bash_command='{{conf.get("core", "dags_folder")}}/fx_rates_example/dolt_load.py --averages ',
    dag=fx_rates_dag
)


fx_rates_averages.set_upstream(fx_rates_raw_data)


# MTA data DAG
raw_mta_data_dag = DAG(
    'mta_data',
    default_args=get_default_args_helper(datetime(2019, 10, 8)),
    schedule_interval=timedelta(days=1)
)

raw_mta_data = BashOperator(
    task_id='raw_mta_data',
    bash_command='{{conf.get("core", "dags_folder")}}/mta/dolt_load.py ',
    dag=raw_mta_data_dag
)

# IP to country mappings
ip_to_country_dag = DAG(
    'ip_to_country',
    default_args=get_default_args_helper(datetime(2019, 10, 8)),
    schedule_interval=timedelta(days=1)
)

raw_ip_to_country = BashOperator(
    task_id='ip_to_country',
    bash_command='{{conf.get("core", "dags_folder")}}/ip_to_country/dolt_load.py ',
    dag=ip_to_country_dag
)

# WordNet database
word_net_dag = DAG(
    'word_net',
    default_args=get_default_args_helper(datetime(2019, 10, 22)),
    schedule_interval=timedelta(days=7)
)

raw_word_net = BashOperator(
    task_id='import-data',
    bash_command='{{conf.get("core", "dags_folder")}}/word_net/import_from_source.pl ',
    dag=word_net_dag
)

# GitHub repos
github_repos_dag = DAG(
    'github_repos',
    default_args=get_default_args_helper(datetime(2020, 3, 5)),
    schedule_interval=timedelta(days=7)
)

raw_github_repos = BashOperator(
    task_id='import-data',
    bash_command='{{conf.get("core", "dags_folder")}}/github-repos/import-data.pl -b -p ',
    dag=github_repos_dag
)

# Code Search Net database
code_search_net_dag = DAG(
    'code_search_net',
    default_args=get_default_args_helper(datetime(2019, 10, 23)),
    schedule_interval=timedelta(days=7)
)

code_search_net = BashOperator(
    task_id='import-data',
    bash_command='{{conf.get("core", "dags_folder")}}/code_search_net/import_from_source.pl ',
    dag=code_search_net_dag
)

# USDA All Foods database
usda_all_foods_dag = DAG(
    'usda_all_foods',
    default_args=get_default_args_helper(datetime(2019, 10, 24)),
    schedule_interval=timedelta(days=7)
)

raw_usda_all_foods = BashOperator(
    task_id='import-data',
    bash_command='{{conf.get("core", "dags_folder")}}/usda_all_foods/import_from_source.pl ',
    dag=usda_all_foods_dag
)

# Tatoeba sentence translations
tatoeba_sentence_translations_dag = DAG(
    'tatoeba_sentence_translations',
    default_args=get_default_args_helper(datetime(2019, 10, 21)),
    schedule_interval=timedelta(days=7)
)

raw_tatoeba_sentence_translations = BashOperator(
    task_id='import-data',
    bash_command='{{conf.get("core", "dags_folder")}}/tatoeba_sentence_translations/import-from-source.pl ',
    dag=tatoeba_sentence_translations_dag
)

# Facebook Neural Code Search Evaluation
neural_code_search_eval_dag = DAG(
    'neural_code_search_eval',
    default_args=get_default_args_helper(datetime(2019, 10, 25)),
    schedule_interval=timedelta(days=7)
)

raw_neural_code_search_eval = BashOperator(
    task_id='import-data',
    bash_command='{{conf.get("core", "dags_folder")}}/neural_code_search_eval/import_from_source.pl ',
    dag=neural_code_search_eval_dag
)

# PPDB - This just checks to make sure PPDB has not changed
# I accidentally deleted the import code. If PPDB changes, I will rewrite :-(
ppdb_dag = DAG(
    'ppdb',
    default_args=get_default_args_helper(datetime(2019, 12, 11)),
    schedule_interval=timedelta(days=7)
)

raw_ppdb = BashOperator(
    task_id='is-changed',
    bash_command='{{conf.get("core", "dags_folder")}}/ppdb/is-changed.pl ',
    dag=ppdb_dag
)

# Stanford Questions and Answers Database (SQuAD)
squad_dag = DAG(
    'squad',
    default_args=get_default_args_helper(datetime(2019, 12, 13)),
    schedule_interval=timedelta(days=7)
)

raw_squad = BashOperator(
    task_id='import-data',
    bash_command='{{conf.get("core", "dags_folder")}}/squad/import-data.pl ',
    dag=squad_dag
)

# Stanford Natural Language Inference (SNLI)
snli_dag = DAG(
    'snli',
    default_args=get_default_args_helper(datetime(2020, 1, 3)),
    schedule_interval=timedelta(days=7)
)

raw_snli = BashOperator(
    task_id='import-data',
    bash_command='{{conf.get("core", "dags_folder")}}/snli/import-data.pl ',
    dag=snli_dag
)

# Multi Natural Language Inference (Multi NLI)
multinli_dag = DAG(
    'multinli',
    default_args=get_default_args_helper(datetime(2020, 1, 3)),
    schedule_interval=timedelta(days=7)
)

raw_multinli = BashOperator(
    task_id='import-data',
    bash_command='{{conf.get("core", "dags_folder")}}/multi-nli/import-data.pl ',
    dag=multinli_dag
)

# Google Landmarks
google_landmarks_dag = DAG(
    'google_landmarks',
    default_args=get_default_args_helper(datetime(2019, 12, 18)),
    schedule_interval=timedelta(days=7)
)

raw_google_lanadmarks = BashOperator(
    task_id='import-data',
    bash_command='{{conf.get("core", "dags_folder")}}/google-landmarks/import-data.pl ',
    dag=google_landmarks_dag
)

# Baseball Databank
baseball_databank_dag = DAG(
    'baseball_databank',
    default_args=get_default_args_helper(datetime(2020,1, 6)),
    schedule_interval=timedelta(days=7)
)

raw_baseball_databank = BashOperator(
    task_id='import-data',
    bash_command='{{conf.get("core", "dags_folder")}}/baseball-databank/import-data.pl ',
    dag=baseball_databank_dag
)

# US Baby Names
us_baby_names_dag = DAG(
    'us_baby_names',
    default_args=get_default_args_helper(datetime(2020,1,15)),
    schedule_interval=timedelta(days=7)
)

raw_us_baby_names = BashOperator(
    task_id='import-data',
    bash_command='{{conf.get("core", "dags_folder")}}/us_baby_names/import-data.pl ',
    dag=us_baby_names_dag
)

# Corona Virus
corona_virus_dag = DAG(
    'corona_virus',
    default_args=get_default_args_helper(datetime(2020,2,5)),
    schedule_interval=timedelta(hours=12)
)

raw_corona_virus = BashOperator(
    task_id='import-data',
    bash_command='{{conf.get("core", "dags_folder")}}/corona-virus/import-data.pl ',
    dag=corona_virus_dag,
    execution_timeout=timedelta(hours=4)
)

corona_virus_details_dag = DAG(
    'corona_virus_details',
    default_args=get_default_args_helper(datetime(2020,3,3)),
    schedule_interval=timedelta(days=1)
)

singapore_details = BashOperator(
    task_id='singapore-details',
    bash_command='{{conf.get("core", "dags_folder")}}/corona-virus/import-case-details-singapore.pl ',
    dag=corona_virus_details_dag
)

hongkong_details = BashOperator(
    task_id='hongkong-details',
    bash_command='{{conf.get("core", "dags_folder")}}/corona-virus/import-case-details-hongkong.pl ',
    dag=corona_virus_details_dag
)

southkorea_details = BashOperator(
    task_id='southkorea-details',
    bash_command='{{conf.get("core", "dags_folder")}}/corona-virus/import-case-details-southkorea.pl ',
    dag=corona_virus_details_dag
)

philippines_details = BashOperator(
    task_id='philippines-details',
    bash_command='{{conf.get("core", "dags_folder")}}/corona-virus/import-case-details-philippines.pl ',
    dag=corona_virus_details_dag
)

viro_dot_org_details = BashOperator(
    task_id='viro-dot-org-details',
    bash_command='{{conf.get("core", "dags_folder")}}/corona-virus/import-case-details-viro-dot-org.pl ',
    dag=corona_virus_details_dag
)

# Wikipedia word frequency
# Wikipedia dump variables
DUMP_DATE = datetime.now() - timedelta(days=4)
FORMATTED_DATE = DUMP_DATE.strftime("%Y%m%d")
# XML dumps released on the 1st and 20th of every month. These jobs should run 4 days after.
CRON_FORMAT = '0 8 5,24 * *'

# Wikipedia word frequency
wikipedia_word_frequencies_dag = DAG(
    'wikipedia-word-frequency',
    default_args=get_default_args_helper(datetime(2019, 10, 18)),
    schedule_interval=CRON_FORMAT
)

wikipedia_word_frequencies = BashOperator(
    task_id='import-data',
    bash_command='{{conf.get("core", "dags_folder")}}/word_frequency/dolt_load.py --branch {}'.format(FORMATTED_DATE),
    dag=wikipedia_word_frequencies_dag
)


# Wikipedia ngrams
wikipedia_ngrams_dag = DAG(
    'wikipedia-ngrams',
    default_args=get_default_args_helper(datetime(2019, 11, 5)),
    schedule_interval=CRON_FORMAT
)

wikipedia_ngrams = BashOperator(
    task_id='import-data',
    bash_command='{{conf.get("core", "dags_folder")}}/ngrams/dolt_load.py --date-string  {} --dump-target {} '.format(
        FORMATTED_DATE,
        'latest'
    ),
    dag=wikipedia_ngrams_dag
)

# Backfill Wikipedia ngrams
wikipedia_ngrams_backfill_dag = DAG(
    'wikipedia-ngrams-backfill',
    default_args=get_default_args_helper(datetime(2019, 12, 2)),
    schedule_interval='@once',
)


def create_ngrams_backfill_tasks(dump_dates):
    tasks_list = []
    for i, dump_date in enumerate(dump_dates):
        task = BashOperator(
            task_id='backfill-data-{}'.format(dump_date),
            bash_command='{{conf.get("core", "dags_folder")}}/ngrams/dolt_load.py --date-string  {} --dump-target {} '.format(
                dump_date,
                dump_date
            ),
            dag=wikipedia_ngrams_backfill_dag
        )
        tasks_list.append(task)

        if i != 0:
            tasks_list[i-1] >> tasks_list[i]


dump_dates = ['20190901', '20190920', '20191001', '20191020', '20191101', '20191120', '20191201', '20191220']
create_ngrams_backfill_tasks(dump_dates)

five_thirty_eight_polls_dag = DAG(
    'five_thirty_eight_polls',
    default_args=get_default_args_helper(datetime(2019, 12, 3)),
    schedule_interval=timedelta(hours=1)
)

five_thirty_eight_polls = BashOperator(
    task_id='five_thirty_eight_polls',
    bash_command='{{conf.get("core", "dags_folder")}}/five_thirty_eight/polls.py' ,
    dag=five_thirty_eight_polls_dag
)

five_thirty_eight_soccer_spi_dag = DAG(
    'five_thirty_eight_soccer_spi',
    default_args=get_default_args_helper(datetime(2019, 12, 3)),
    schedule_interval=timedelta(hours=1)
)

five_thirty_eight_soccer_spi = BashOperator(
    task_id='five_thirty_eight_polls',
    bash_command='{{conf.get("core", "dags_folder")}}/five_thirty_eight/soccer_spi.py ',
    dag=five_thirty_eight_soccer_spi_dag
)

five_thirty_eight_nba_forecasts_dag = DAG(
    'five_thirty_eight_nba_forecasts',
    default_args=get_default_args_helper(datetime(2019, 12, 3)),
    schedule_interval=timedelta(hours=1)
)

five_thirty_eight_nba_forecasts = BashOperator(
    task_id='five_thirty_eight_nba_forecasts',
    bash_command='{{conf.get("core", "dags_folder")}}/five_thirty_eight/nba_forecasts.py',
    dag=five_thirty_eight_nba_forecasts_dag
)

five_thirty_eight_nfl_forecasts_dag = DAG(
    'five_thirty_eight_nfl_forecasts',
    default_args=get_default_args_helper(datetime(2019, 12, 3)),
    schedule_interval=timedelta(hours=1)
)

five_thirty_eight_nfl_forecasts = BashOperator(
    task_id='five_thirty_eight_polls',
    bash_command='{{conf.get("core", "dags_folder")}}/five_thirty_eight/nfl_forecasts.py',
    dag=five_thirty_eight_nfl_forecasts_dag
)


# coin metrics
coin_metrics_eod_dag = DAG(
    'coin_metrics_eod',
    default_args=get_default_args_helper(datetime(2020, 3, 30)),
    schedule_interval=timedelta(days=1)
)

coin_metrics_eod = BashOperator(
    task_id='coin_metrics_eod',
    bash_command='{{conf.get("core", "dags_folder")}}/coin_metrics/dolt_load.py',
    dag=coin_metrics_eod_dag
)

# Common Crawl Index Summary
ccis_dag = DAG(
    'common_crawl_index_summary',
    default_args=get_default_args_helper(datetime(2020, 2, 6)),
    schedule_interval=timedelta(days=28)
)

ccis = BashOperator(
    task_id='common_crawl_index_summary',
    bash_command='{{conf.get("core", "dags_folder")}}/common_crawl_index_summary/run.sh ',
    dag=ccis_dag
)

# Bad Words
bad_words_dag = DAG(
    'bad-words',
    default_args=get_default_args_helper(datetime(2020, 4, 21)),
    schedule_interval=timedelta(days=7)
)

bad_words = BashOperator(
    task_id='check_new_commits',
    bash_command='{{conf.get("core", "dags_folder")}}/bad-words/check_new_commits.sh ',
    dag=bad_words_dag
)

# Open Flights 
open_flights_dag = DAG(
    'open_flights',
    default_args=get_default_args_helper(datetime(2020,3,19)),
    schedule_interval=timedelta(days=1)
)

raw_open_flights = BashOperator(
    task_id='import-data',
    bash_command='{{conf.get("core", "dags_folder")}}/open_flights/import-data.pl ',
    dag=open_flights_dag
)

# Stock Tickers
stock_tickers_dag = DAG(
    'stock_tickers',
    default_args=get_default_args_helper(datetime(2020,3,26)),
    schedule_interval=timedelta(days=1)
)

raw_stock_tickers = BashOperator(
    task_id='import-data',
    bash_command='{{conf.get("core", "dags_folder")}}/stock_tickers/import-ticker-data.pl ',
    dag=stock_tickers_dag
)

# National Vulnerability Database
nvd_dag = DAG(
    'national_vulnerability_database',
    default_args=get_default_args_helper(datetime(2020, 4, 22)),
    schedule_interval=timedelta(hours=1)
)

nvd = BashOperator(
    task_id='national_vulnerability_database',
    bash_command='{{conf.get("core", "dags_folder")}}/nvd/run.sh ',
    dag=nvd_dag
)

# COVID Stimulus Watch
covid_stimulus_watch_dag = DAG(
    'covid_stimulus_watch',
    default_args=get_default_args_helper(datetime(2020, 5, 13)),
    schedule_interval=timedelta(days=1)
)

covid_stimulus_watch = BashOperator(
    task_id='import-csv',
    bash_command='{{conf.get("core", "dags_folder")}}/covid_stimulus_watch/import-csv.py ',
    dag=covid_stimulus_watch_dag
)

# Terms and Privacy Policies for interesting online services
online_services_dag = DAG(
    'online_services',
    default_args=get_default_args_helper(datetime(2020, 5, 28)),
    schedule_interval=timedelta(days=7)
)

online_services_task = BashOperator(
    task_id='scrape-docs',
    bash_command='{{conf.get("core", "dags_folder")}}/online_services/scrape-documents.py ',
    dag=online_services_dag
)

# US Census Response Rates
us_census_response_rates_dag = DAG(
    'us_census_response_rates',
    default_args=get_default_args_helper(datetime(2020, 5, 13)),
    schedule_interval=timedelta(days=1)
)

us_census_response_rates = BashOperator(
    task_id='daily-import',
    bash_command='{{conf.get("core", "dags_folder")}}/us_census_response_rates/daily-csv-import.py ',
    dag=us_census_response_rates_dag
)
