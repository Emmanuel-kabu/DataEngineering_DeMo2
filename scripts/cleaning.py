import json 

import ast
import os
import logging
import numpy as np
import pandas as pd

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class MovieDataCleaner:

    def __init__(self, df):
        self.df = df.copy()

        # Configuration (easy to change later)
        self.columns_to_drop = ['adult', 'imdb_id', 'original_title', 'video', 'homepage']
        self.json_columns = ['genres', 'spoken_languages', 'production_companies',
                             'production_countries', 'belongs_to_collection']
        self.numeric_columns = ['budget', 'id', 'popularity', 'revenue', 'runtime',
                                'vote_average', 'vote_count']
        self.replace_zero_columns = ["budget", "revenue", "runtime"]
        self.text_columns = ["overview", "tagline"]
        self.text_placeholders = ["No Data", "No overview available.", "None", "", "nan", "N/A"]
        self.columns_to_drop_duplicates = ['id','title']

    # 1. DROP UNNECESSARY COLUMNS
    def drop_columns(self):
        existing = [c for c in self.columns_to_drop if c in self.df.columns]
        self.df.drop(columns=existing, inplace=True)
        logger.info(f"Dropped columns: {existing}")

    
    # 2. PARSE JSON-LIKE FIELDS
    def parse_json(self, value):
        if value is None or value == "" or value == "null":
            return np.nan

        try:
            if isinstance(value, str):
                value = ast.literal_eval(value)

            if isinstance(value, dict):
                return value.get("name", np.nan)

            if isinstance(value, list):
                names = [item.get("name") for item in value if isinstance(item, dict)]
                return "|".join(n for n in names if n) if names else np.nan

        except Exception:
            return np.nan

        return np.nan

    def process_json_columns(self):
        for col in self.json_columns:
            if col in self.df.columns:
                self.df[col] = self.df[col].apply(self.parse_json)
                logger.info(f"Processed JSON column: {col}")
            else:
                logger.info(f"Skipped missing JSON column: {col}")

    # 3. CONVERT DATATYPES

    def convert_dtypes(self):
        for col in self.numeric_columns:
            if col in self.df.columns:
                self.df[col] = pd.to_numeric(self.df[col], errors="coerce")
        if "release_date" in self.df.columns:
            self.df["release_date"] = pd.to_datetime(self.df["release_date"], errors="coerce")
        logger.info("Numeric + datetime conversions done.")

    # 4. HANDLE ZERO VALUES, BUDGET/REVENUE IN MILLIONS, VOTE FIXES
    def clean_numeric_fields(self):
        # Replace zeros with NaN
        for col in self.replace_zero_columns:
            if col in self.df.columns:
                self.df[col] = self.df[col].replace(0, np.nan)

        # Convert budget & revenue to millions
        for col in ["budget", "revenue"]:
            if col in self.df.columns:
                self.df[col] = self.df[col] / 1_000_000

        # Rename columns to indicate millions USD
        self.df.rename(columns={"budget": "budget_musd", "revenue": "revenue_musd"}, inplace=True)
        logger.info("Budget and Revenue converted to millions USD.")

        # Vote count = 0 means vote_average unreliable
        if "vote_count" in self.df.columns and "vote_average" in self.df.columns:
            self.df.loc[self.df["vote_count"] == 0, "vote_average"] = np.nan

        print("Numeric fields cleaned.")

    # 5. CLEAN TEXT FIELDS

    def clean_text_fields(self):
        for col in self.text_columns:
            if col in self.df.columns:
                self.df[col] = (
                    self.df[col]
                    .replace(self.text_placeholders, np.nan)
                    .astype("string")
                    .str.strip()
                    .replace("", np.nan)
                )
        print("Text fields cleaned.")

    def drop_duplicate_rows(self):
        for col in self.columns_to_drop_duplicates:
            if col not in self.df.columns:
                raise ValueError(f"Column '{col}' not found in DataFrame for duplicate removal.")
    
        self.df.drop_duplicates(inplace=True)
        final_count = len(self.df)
        print(f"Duplicate rows dropped. Final row count: {final_count}")
    # drop columns with more than 10 NaN values
    def drop_high_nan_columns(self, threshold=10):
        cols_to_drop = [col for col in self.df.columns if self.df[col].isna().sum() > threshold]
        self.df.drop(columns=cols_to_drop, inplace=True)
        print(f"Dropped columns with more than {threshold} NaN values: {cols_to_drop}")

    # create crew and cast columns from credits json
    def create_crew_and_cast_columns(self):
        if 'credits' in self.df.columns:
            def parse_credits(credits):
                try:
                    return ast.literal_eval(credits) if isinstance(credits, str) else credits
                except Exception:
                    return None

            def extract_cast(credits):
                parsed = parse_credits(credits)
                if isinstance(parsed, dict) and 'cast' in parsed:
                    names = [m.get('name') for m in parsed.get('cast', []) if isinstance(m, dict) and m.get('name')]
                    return "|".join(names) if names else np.nan
                return np.nan

            def extract_crew(credits):
                parsed = parse_credits(credits)
                if isinstance(parsed, dict) and 'crew' in parsed:
                    names = [m.get('name') for m in parsed.get('crew', []) if isinstance(m, dict) and m.get('name')]
                    return "|".join(names) if names else np.nan
                return np.nan

            self.df['cast'] = self.df['credits'].apply(extract_cast)
            self.df['crew'] = self.df['credits'].apply(extract_crew)
            print("Created 'cast' and 'crew' columns from 'credits'.")
        else:
            print("Skipped creating 'cast' and 'crew' columns as 'credits' column is missing.")

    # create director columns from credits columns
    def create_director_column(self):
        if 'credits' in self.df.columns:
            def extract_directors(credits):
                try:
                    parsed = ast.literal_eval(credits) if isinstance(credits, str) else credits
                    if isinstance(parsed, dict):
                        director_names = [m.get('name') for m in parsed.get('crew', []) if isinstance(m, dict) and m.get('job') == 'Director' and m.get('name')]
                        return "|".join(director_names) if director_names else np.nan
                except Exception:
                    return np.nan
                return np.nan

            self.df['directors'] = self.df['credits'].apply(extract_directors)
            print("Created 'directors' column from 'credits'.")
        else:
            print("Skipped creating 'directors' column as 'credits' column is missing.")

    # create crew_size and cast_size columns from credits
    def create_crew_and_cast_size_columns(self):
        if 'credits' in self.df.columns:
            def crew_size(credits):
                try:
                    parsed = ast.literal_eval(credits) if isinstance(credits, str) else credits
                    crew_list = parsed.get('crew', []) if isinstance(parsed, dict) else []
                    return len(crew_list)
                except Exception:
                    return np.nan

            def cast_size(credits):
                try:
                    parsed = ast.literal_eval(credits) if isinstance(credits, str) else credits
                    cast_list = parsed.get('cast', []) if isinstance(parsed, dict) else []
                    return len(cast_list)
                except Exception:
                    return np.nan

            self.df['crew_size'] = self.df['credits'].apply(crew_size)
            self.df['cast_size'] = self.df['credits'].apply(cast_size)
            print("Created 'crew_size' and 'cast_size' columns from 'credits'.")
        else:
            print("Skipped creating 'crew_size' and 'cast_size' columns as 'credits' column is missing.")  

    # drop only credits column once derived fields are created
    def drop_crew_and_credits_columns(self):
        cols_to_drop = []
        if 'credits' in self.df.columns:
            cols_to_drop.append('credits')
        if cols_to_drop:
            self.df.drop(columns=cols_to_drop, inplace=True)
            print(f"Dropped columns: {cols_to_drop}")
        else:
            print("No 'credits' column to drop.")  


    # reorder columns to have columns in a logical order
    def reorder_columns(self):
        desired_order = ['id', 'title', 'tagline', 'release_date', 'genres', 'belongs_to_collection',
                          'original_language', 'budget_musd', 'revenue_musd', 'production_companies', 
                          'production_countries', 'vote_count', 'vote_average', 'popularity', 'runtime',
                          'overview', 'spoken_languages', 'poster_path', 'cast', 'cast_size','crew_size']
        
        # Only include columns that exist in the dataframe
        existing_columns = [col for col in desired_order if col in self.df.columns]
        # Add any remaining columns not in desired_order
        remaining_columns = [col for col in self.df.columns if col not in existing_columns]
        new_order = existing_columns + remaining_columns
        
        self.df = self.df[new_order]
        print("Reordered columns.")                                
    # resetto index
    def reset_index(self):
        self.df.reset_index(drop=True, inplace=True)
        print("Index reset.")
    # Aggregated cleaning methods
    def clean(self):
        print("Starting data cleaning...\n")

        self.drop_columns()
        self.process_json_columns()
        self.convert_dtypes()
        self.clean_numeric_fields()
        self.clean_text_fields()
        self.drop_duplicate_rows()
        self.drop_high_nan_columns()
        self.create_crew_and_cast_columns()
        self.create_director_column()
        self.create_crew_and_cast_size_columns()
        self.drop_crew_and_credits_columns()
        self.reorder_columns()
        #self.reset_index()

        print("\nCleaning complete.")
        print("Unwanted columns dropped, JSON fields processed, data types converted, numeric fields cleaned, text fields cleaned, duplicate rows dropped, crew and cast columns created, directors column created, columns reordered.")
        return self.df
    

if __name__ == "__main__":
    """
    Standalone execution for testing and development.
    
    This section demonstrates proper error handling and logging
    when running the cleaner independently.
    """
    try:
        # Configuration
        script_dir = os.path.dirname(os.path.abspath(__file__))
        project_dir = os.path.dirname(script_dir)
        data_dir = os.path.join(project_dir, 'data')
        
        input_path = os.path.join(data_dir, 'rawextracted_tmdb_movies.csv')
        output_path = os.path.join(data_dir, 'cleaned_tmdb_movies.csv')
        
        # Validate input file exists
        if not os.path.exists(input_path):
            logger.error(f"Input file not found: {input_path}")
            logger.info("Please run extraction.py first to generate raw data")
            exit(1)
        
        # Load and validate data
        logger.info(f"Loading data from: {input_path}")
        df = pd.read_csv(input_path)
        
        if df.empty:
            raise ValueError("Input file contains no data")
        
        logger.info(f"Loaded {len(df)} rows, {len(df.columns)} columns")
        
        # Clean data
        cleaner = MovieDataCleaner(df)
        cleaned_df = cleaner.clean()
        
        # Save results
        os.makedirs(data_dir, exist_ok=True)
        cleaned_df.to_csv(output_path, index=False)
        logger.info(f"Cleaned data saved to: {output_path}")
        
        # Summary statistics
        logger.info(f"Final dataset: {len(cleaned_df)} rows, {len(cleaned_df.columns)} columns")
        null_summary = (cleaned_df.notna().sum() / len(cleaned_df) * 100).round(1)
        logger.info(f"Data completeness: {null_summary.to_dict()}")
        
    except Exception as e:
        logger.error(f"Data cleaning process failed: {e}")
        raise   