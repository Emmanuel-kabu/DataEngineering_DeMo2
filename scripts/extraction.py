import os
import requests
import pandas as pd
import json
import logging
import time
from typing import List, Dict, Optional
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dotenv import load_dotenv

load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)
class TMDBExtractor:
    """Enhanced TMDB extractor with retry logic, timeout handling, and comprehensive error management."""
    
    def __init__(self, api_key: str, max_retries: int = 3, timeout: int = 30, delay_between_requests: float = 0.5):
        """
        Initialize TMDB extractor with robust configuration.
        
        Args:
            api_key: TMDB API key
            max_retries: Maximum number of retry attempts for failed requests
            timeout: Request timeout in seconds
            delay_between_requests: Delay between API requests to respect rate limits
            
        Raises:
            ValueError: If API key is invalid or missing
        """
        if not api_key or not isinstance(api_key, str) or len(api_key.strip()) == 0:
            raise ValueError("Valid TMDB API key is required")
            
        self.api_key = api_key.strip()
        self.base_url = 'https://api.themoviedb.org/3/movie/'
        self.timeout = timeout
        self.delay_between_requests = delay_between_requests
        
        # Configure session with retry strategy
        self.session = requests.Session()
        retry_strategy = Retry(
            total=max_retries,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        
        logger.info(f"TMDBExtractor initialized with {max_retries} max retries and {timeout}s timeout")
    
    def _validate_movie_id(self, movie_id) -> bool:
        """Validate that movie_id is a positive integer."""
        try:
            movie_id_int = int(movie_id)
            return movie_id_int > 0
        except (ValueError, TypeError):
            return False
    
    def _extract_single_movie(self, movie_id: int) -> Optional[Dict]:
        """
        Extract data for a single movie with comprehensive error handling.
        
        Args:
            movie_id: TMDB movie ID
            
        Returns:
            Movie data dictionary or None if extraction fails
        """
        if not self._validate_movie_id(movie_id):
            logger.error(f"Invalid movie ID: {movie_id}. Must be a positive integer.")
            return None
            
        url = f"{self.base_url}{movie_id}?api_key={self.api_key}&language=en-US&append_to_response=credits"
        
        try:
            logger.debug(f"Fetching data for movie ID: {movie_id}")
            response = self.session.get(url, timeout=self.timeout)
            
            if response.status_code == 200:
                movie_data = response.json()
                
                # Validate response contains required fields
                if not self._validate_movie_data(movie_data):
                    logger.warning(f"Movie ID {movie_id} returned invalid data structure")
                    return None
                    
                logger.info(f"Successfully fetched data for movie ID: {movie_id}")
                return movie_data
                
            elif response.status_code == 404:
                logger.warning(f"Movie ID {movie_id} not found (404)")
                return None
                
            elif response.status_code == 401:
                logger.error(f"Unauthorized access (401). Check your API key.")
                raise ValueError("Invalid API key or unauthorized access")
                
            elif response.status_code == 429:
                logger.error(f"Rate limit exceeded (429) for movie ID {movie_id}")
                return None
                
            else:
                logger.error(f"Failed to fetch movie ID {movie_id}. Status: {response.status_code}, Response: {response.text[:200]}")
                return None
                
        except requests.exceptions.Timeout:
            logger.error(f"Timeout occurred while fetching movie ID {movie_id}")
            return None
            
        except requests.exceptions.ConnectionError:
            logger.error(f"Connection error while fetching movie ID {movie_id}")
            return None
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Request exception for movie ID {movie_id}: {e}")
            return None
            
        except ValueError as e:
            logger.error(f"JSON decode error for movie ID {movie_id}: {e}")
            return None
    
    def _validate_movie_data(self, movie_data: Dict) -> bool:
        """
        Validate that movie data contains essential fields.
        
        Args:
            movie_data: Movie data dictionary from API
            
        Returns:
            True if data is valid, False otherwise
        """
        required_fields = ['id', 'title']
        return all(field in movie_data for field in required_fields)
    
    def extract_tmdb_data(self, movie_ids: List[int]) -> List[Dict]:
        """
        Extract data for multiple movies with comprehensive error handling and progress tracking.
        
        Args:
            movie_ids: List of TMDB movie IDs
            
        Returns:
            List of successfully extracted movie data dictionaries
            
        Raises:
            ValueError: If movie_ids is empty or contains invalid values
        """
        if not movie_ids or len(movie_ids) == 0:
            raise ValueError("movie_ids cannot be empty")
            
        if not isinstance(movie_ids, (list, tuple)):
            raise ValueError("movie_ids must be a list or tuple")
            
        logger.info(f"Starting extraction for {len(movie_ids)} movies")
        
        movies_data = []
        failed_ids = []
        
        for i, movie_id in enumerate(movie_ids):
            logger.info(f"Processing movie {i+1}/{len(movie_ids)}: ID {movie_id}")
            
            movie_data = self._extract_single_movie(movie_id)
            
            if movie_data:
                movies_data.append(movie_data)
            else:
                failed_ids.append(movie_id)
            
            # Rate limiting: delay between requests
            if i < len(movie_ids) - 1:  # Don't delay after last request
                time.sleep(self.delay_between_requests)
        
        logger.info(f"Extraction complete. Successfully extracted: {len(movies_data)}, Failed: {len(failed_ids)}")
        
        if failed_ids:
            logger.warning(f"Failed to extract data for movie IDs: {failed_ids}")
        
        if len(movies_data) == 0:
            logger.error("No movie data was successfully extracted")
            raise RuntimeError("Failed to extract any movie data")
            
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