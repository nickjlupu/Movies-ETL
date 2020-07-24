# import dependencies
import json
import pandas as pd
import numpy as np
import re
from sqlalchemy import create_engine
from config import db_password
import psycopg2
import time


# =============
# Pull in data
# =============

# set file path
file_dir = "C:/Users/nickl/Documents/DataViz/MyRepo/02-Assignments/Movies-ETL/"

# obtain raw datasets from wikipedia & kaggle
# assumption 1:  wiki will be a json file provided (no api will be called), kaggle metadata & ratings will be csv's in same directory
# assumption 2:  all data is located in the same folder as noted in variable file_dir
# could put try - except block here to return an error message if files are not in correct location or format
try:
    kaggle_metadata = pd.read_csv(f'{file_dir}movies_metadata.csv', low_memory=False)
    ratings = pd.read_csv(f'{file_dir}ratings.csv')
    with open(f'{file_dir}/wikipedia.movies.json', mode='r') as file:
        wiki_movies_raw = json.load(file)
except:
    print('=====================================================')  
    print('files not found.  please check filepath and filenames')  
    print('=====================================================')  


# create function that takes in 3 arguments 
def automate_etl (wiki_movies_raw,kaggle_metadata,ratings):

    
    # =====================
    # Clean Wikipedia Data
    # =====================
    
    # put wiki data into dataframe (kaggle metadata & ratings already in df's)
    wiki_movies_df = pd.DataFrame(wiki_movies_raw)
    
    # keep rows with a director and an imdb link
    # filter out tv shows by only keeping rows without "No. of episodes"
    wiki_movies = [movie for movie in wiki_movies_raw
               if ('Director' in movie or 'Directed by' in movie)
                   and 'imdb_link' in movie
                   and 'No. of episodes' not in movie]
    wiki_movies_df = pd.DataFrame(wiki_movies)

    
    # function to clean movies
    def clean_movie(movie):
        movie = dict(movie) #create a non-destructive copy
        alt_titles = {}
        # combine alternate titles into one list
        for key in ['Also known as','Arabic','Cantonese','Chinese','French',
                    'Hangul','Hebrew','Hepburn','Japanese','Literally',
                    'Mandarin','McCune-Reischauer','Original title','Polish',
                    'Revised Romanization','Romanized','Russian',
                    'Simplified','Traditional','Yiddish']:
            if key in movie:
                alt_titles[key] = movie[key]
                movie.pop(key)
        if len(alt_titles) > 0:
            movie['alt_titles'] = alt_titles
        
        # merge column names
        def change_column_name(old_name, new_name):
            if old_name in movie:
                movie[new_name] = movie.pop(old_name)
        
        change_column_name('Adaptation by', 'Writer(s)')
        change_column_name('Country of origin', 'Country')
        change_column_name('Directed by', 'Director')
        change_column_name('Distributed by', 'Distributor')
        change_column_name('Edited by', 'Editor(s)')
        change_column_name('Length', 'Running time')
        change_column_name('Original release', 'Release date')
        change_column_name('Music by', 'Composer(s)')
        change_column_name('Produced by', 'Producer(s)')
        change_column_name('Producer', 'Producer(s)')
        change_column_name('Productioncompanies ', 'Production company(s)')
        change_column_name('Productioncompany ', 'Production company(s)')
        change_column_name('Released', 'Release Date')
        change_column_name('Release Date', 'Release date')
        change_column_name('Screen story by', 'Writer(s)')
        change_column_name('Screenplay by', 'Writer(s)')
        change_column_name('Story by', 'Writer(s)')
        change_column_name('Theme music composer', 'Composer(s)')
        change_column_name('Written by', 'Writer(s)')
        
        return movie

    # call clean_movies function with list comprehension to then put into df
    clean_movies = [clean_movie(movie) for movie in wiki_movies]
    wiki_movies_df = pd.DataFrame(clean_movies)

    # extract imdb id and remove duplicate rows
    wiki_movies_df['imdb_id'] = wiki_movies_df['imdb_link'].str.extract(r'(tt\d{7})')
    wiki_movies_df.drop_duplicates(subset='imdb_id', inplace=True)

    # remove mostly null columns
    wiki_columns_to_keep = [column for column in wiki_movies_df.columns if wiki_movies_df[column].isnull().sum() < len(wiki_movies_df) * 0.9]
    wiki_movies_df = wiki_movies_df[wiki_columns_to_keep]

    # drop rows with missing data, convert lists to a concatonated string
    box_office = wiki_movies_df['Box office'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)
    budget = wiki_movies_df['Budget'].dropna().map(lambda x: ' '.join(x) if type(x) == list else x)
    release_date = wiki_movies_df['Release date'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)
    running_time = wiki_movies_df['Running time'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)   

    # initialize form variables
    form_one = r'\$\s*\d+\.?\d*\s*[mb]illi?on'
    form_two = r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)'

    # remove lower end of ranges if box office & budget data given in ranges
    box_office = box_office.str.replace(r'\$.*[-—–](?![a-z])', '$', regex=True)
    budget = budget.str.replace(r'\$.*[-—–](?![a-z])', '$', regex=True)
    
    # remove citation in budget data
    budget = budget.str.replace(r'\[\d+\]\s*', '')

    # convert box office strings to numeric data (floating point) 
    def parse_dollars(s):
        # if s is not a string, return NaN
        if type(s) != str:
            return np.nan

        # if input is of the form $###.# million
        if re.match(r'\$\s*\d+\.?\d*\s*milli?on', s, flags=re.IGNORECASE):

            # remove dollar sign and " million"
            s = re.sub(r'\$|\s|[a-zA-Z]','', s)

            # convert to float and multiply by a million
            value = float(s) * 10**6

            # return value
            return value

        # if input is of the form $###.# billion
        elif re.match(r'\$\s*\d+\.?\d*\s*billi?on', s, flags=re.IGNORECASE):

            # remove dollar sign and " billion"
            s = re.sub(r'\$|\s|[a-zA-Z]','', s)

            # convert to float and multiply by a billion
            value = float(s) * 10**9

            # return value
            return value

        # if input is of the form $###,###,###
        elif re.match(r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)', s, flags=re.IGNORECASE):

            # remove dollar sign and commas
            s = re.sub(r'\$|,','', s)

            # convert to float
            value = float(s)

            # return value
            return value

        # otherwise, return NaN
        else:
            return np.nan

    # extract strings & call parse_dollars function
    wiki_movies_df['box_office'] = box_office.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars)
    wiki_movies_df['budget'] = budget.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars)

    # set up date forms
    date_form_one = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s[123]\d,\s\d{4}'
    date_form_two = r'\d{4}.[01]\d.[123]\d'
    date_form_three = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s\d{4}'
    date_form_four = r'\d{4}'

    # extract dates
    release_date.str.extract(f'({date_form_one}|{date_form_two}|{date_form_three}|{date_form_four})', flags=re.IGNORECASE)

    # convert to datetime
    wiki_movies_df['release_date'] = pd.to_datetime(release_date.str.extract(f'({date_form_one}|{date_form_two}|{date_form_three}|{date_form_four})')[0], infer_datetime_format=True)

    # extract running time, convert to numeric, change Nans to 0's
    running_time_extract = running_time.str.extract(r'(\d+)\s*ho?u?r?s?\s*(\d*)|(\d+)\s*m')
    running_time_extract = running_time_extract.apply(lambda col: pd.to_numeric(col, errors='coerce')).fillna(0)
    # convert to minutes
    wiki_movies_df['running_time'] = running_time_extract.apply(lambda row: row[0]*60 + row[1] if row[2] == 0 else row[2], axis=1)

    # drop unncessary columns
    wiki_movies_df.drop(['Box office','Budget','Running time'],axis=1, inplace=True)
    
    # =================
    # Clean Kaggle Data
    # =================

    # keep rows where the adult column is False, and then drop the adult & video columns
    kaggle_metadata = kaggle_metadata[kaggle_metadata['adult'] == 'False'].drop(['adult','video'],axis='columns')

    # convert to numeric data type
    kaggle_metadata['budget'] = kaggle_metadata['budget'].astype(int)
    kaggle_metadata['id'] = pd.to_numeric(kaggle_metadata['id'], errors='raise')
    kaggle_metadata['popularity'] = pd.to_numeric(kaggle_metadata['popularity'], errors='raise')
 
    # convert to datetime
    kaggle_metadata['release_date'] = pd.to_datetime(kaggle_metadata['release_date'])
    ratings['timestamp'] = pd.to_datetime(ratings['timestamp'], unit='s')

    # =================
    # MERGE DATASETS
    # =================

    # merge
    movies_df = pd.merge(wiki_movies_df, kaggle_metadata, on='imdb_id', suffixes=['_wiki','_kaggle'])

    # Competing data:
    # Wiki                     Movielens                Resolution
    #--------------------------------------------------------------------------
    # title_wiki               title_kaggle             Drop Wikipedia
    # running_time             runtime                  Keep Kaggle; fill in zeros with Wikipedia data.
    # budget_wiki              budget_kaggle            Keep Kaggle; fill in zeros with Wikipedia data.
    # box_office               revenue                  Keep Kaggle; fill in zeros with Wikipedia data.
    # release_date_wiki        release_date_kaggle      Drop Wikipedia.
    # Language                 original_language        Drop Wikipedia.
    # Production company(s)    production_companies     Drop Wikipedia.
    # ASSUMPTION:  This pattern will continue on future datasets

    # drop the title_wiki, release_date_wiki, Language, and Production company(s) columns
    movies_df.drop(columns=['title_wiki','release_date_wiki','Language','Production company(s)'], inplace=True)

    # make a function that fills in missing data for a column pair and then drops the redundant column
    def fill_missing_kaggle_data(df, kaggle_column, wiki_column):
        df[kaggle_column] = df.apply(
            lambda row: row[wiki_column] if row[kaggle_column] == 0 else row[kaggle_column]
            , axis=1)
        df.drop(columns=wiki_column, inplace=True)

    # call function to fill in missing data
    fill_missing_kaggle_data(movies_df, 'runtime', 'running_time')
    fill_missing_kaggle_data(movies_df, 'budget_kaggle', 'budget_wiki')
    fill_missing_kaggle_data(movies_df, 'revenue', 'box_office')

    # reorder columns with .loc
    movies_df = movies_df.loc[:, ['imdb_id','id','title_kaggle','original_title','tagline','belongs_to_collection','url','imdb_link',
                       'runtime','budget_kaggle','revenue','release_date_kaggle','popularity','vote_average','vote_count',
                       'genres','original_language','overview','spoken_languages','Country',
                       'production_companies','production_countries','Distributor',
                       'Producer(s)','Director','Starring','Cinematography','Editor(s)','Writer(s)','Composer(s)','Based on'
                      ]]

    # rename columns
    movies_df.rename({'id':'kaggle_id',
                  'title_kaggle':'title',
                  'url':'wikipedia_url',
                  'budget_kaggle':'budget',
                  'release_date_kaggle':'release_date',
                  'Country':'country',
                  'Distributor':'distributor',
                  'Producer(s)':'producers',
                  'Director':'director',
                  'Starring':'starring',
                  'Cinematography':'cinematography',
                  'Editor(s)':'editors',
                  'Writer(s)':'writers',
                  'Composer(s)':'composers',
                  'Based on':'based_on'
                 }, axis='columns', inplace=True)


    # ====================
    # RATINGS DATA
    # =====================

    # group ratings by movieID & rating, pivot data so that movieID is index and rating values columns
    rating_counts = ratings.groupby(['movieId','rating'], as_index=False).count() \
                .rename({'userId':'count'}, axis=1) \
                .pivot(index='movieId',columns='rating', values='count')

    # rename columns
    rating_counts.columns = ['rating_' + str(col) for col in rating_counts.columns]

    # merge rating counts into movies_df
    movies_with_ratings_df = pd.merge(movies_df, rating_counts, left_on='kaggle_id', right_index=True, how='left')

    # fill missing values with 0's
    movies_with_ratings_df[rating_counts.columns] = movies_with_ratings_df[rating_counts.columns].fillna(0)


    # ===============
    # LOAD DATABASE
    # ===============

    # create database engine
    db_string = f"postgres://postgres:{db_password}@127.0.0.1:5432/movie_data"
    engine = create_engine(db_string)
    
    # clear old data in database tables
    query1 = "DELETE FROM movies"
    query2 = "DELETE FROM ratings"
    engine.execute(query1,query2)

    # load movies df to sql table
    try:
        movies_df.to_sql(name='movies', con=engine, if_exists='replace')
    except:
        print('Loading movies df to SQL table failed, investigate')
    
    # # load ratings data to sql table
    # rows_imported = 0
    # # get the start_time from time.time()
    # try:
    #     start_time = time.time()    
    #     for data in pd.read_csv(f'{file_dir}ratings.csv', chunksize=1000000):
    #         print(f'importing rows {rows_imported} to {rows_imported + len(data)}...', end='')
    #         data.to_sql(name='ratings', con=engine, if_exists='append')
    #         rows_imported += len(data)

    #         # add elapsed time to final print out
    #         print(f'Done. {time.time() - start_time} total seconds elapsed')
    # except:
    #     print('Loading ratings to SQL table failed, consider reducing chunk size')

# ========================
# CALL AUTOMATED FUNCTION
# ========================
automate_etl(wiki_movies_raw, kaggle_metadata, ratings)
