import os
import requests
import pandas as pd
import json
from dotenv import load_dotenv

load_dotenv()
class TMDBExtractor:
    def __init__(self, api_key):
        self.api_key = api_key
        self.base_url = 'https://api.themoviedb.org/3/movie/'

    def extract_tmdb_data(self, movie_ids):
        movies_data = []

        for movie_id in movie_ids:
            # Construct the URL correctly for the specific movie_id
            url = f"{self.base_url}{movie_id}?api_key={self.api_key}&language=en-US&append_to_response=credits"

            try:
                response = requests.get(url)

                # Check for a successful status code
                if response.status_code == 200:
                    movie_data = response.json()
                    movies_data.append(movie_data)
                else:
                    # Print specific error details (status code and ID)
                    print(f"Failed to fetch movie with ID {movie_id}. Status Code: {response.status_code}")

            except requests.exceptions.RequestException as e:
                # Handle network-related errors
                print(f"An error occurred while fetching ID {movie_id}: {e}")

        return movies_data
    

if __name__ == "__main__":
    # Load API key from environment variable
    api_key = os.getenv('TMDB_API_KEY')

    if not api_key:
        raise ValueError("TMDB_API_KEY not found in environment variables.")

    # IDs of movies to extract data for
    movie_ids = [0, 299534, 19995, 140607, 299536, 597,
                 135397, 420818, 24428, 168259, 99861, 284054,
                 12445, 181808, 330457, 351286, 109445, 321612,
                 260513] 

    extractor = TMDBExtractor(api_key)
    movies_data = extractor.extract_tmdb_data(movie_ids)

    # Convert the list of movie data to a DataFrame for easier handling
    movies_df = pd.DataFrame(movies_data)

    # Get the TMDB_movies_project directory (one level up from scripts/)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_dir = os.path.dirname(script_dir)
    data_dir = os.path.join(project_dir, 'data')

    # Create data directory if it doesn't exist
    os.makedirs(data_dir, exist_ok=True)

    # Save CSV to data directory
    csv_path = os.path.join(data_dir, 'rawextracted_tmdb_movies.csv')
    movies_df.to_csv(csv_path, index=False)
    print(f"Data extraction complete. Saved to '{csv_path}'.")