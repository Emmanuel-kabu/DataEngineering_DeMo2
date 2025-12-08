import pandas as pd

class MovieAnalyzer:
    def __init__(self, df: pd.DataFrame):
        """Initialize with a movie DataFrame."""
        self.df = df.copy()
        self._prepare_movie_types()

    
    # INTERNAL METHOD: Adds 'movies_type' column
    def _prepare_movie_types(self):
        self.df['movies_type'] = self.df['belongs_to_collection'].apply(
            lambda x: 'Franchise' if pd.notna(x) else 'Standalone'
        )


    # SCIENCE FICTION ANALYSIS
    def best_scifi_movies_starring_actor(self, actor_name, top_n=5):
        """Return the top N best-rated Science Fiction movies starring a specific actor sorted by vote_count."""
        # Filter for Science Fiction
        scifi_df = self.df[self.df['genres'].str.contains("Science Fiction", na=False, case=False)]
        if scifi_df.empty:
            return pd.DataFrame(columns=['title', 'genres', 'vote_count', 'vote_average'])
        
        # Filter for actor in cast (handle NaN cast values)
        scifi_actor_df = scifi_df[
            scifi_df['cast'].fillna('').str.contains(actor_name, na=False, case=False)
        ]
        
        if scifi_actor_df.empty:
            print(f"⚠ No Science Fiction movies found starring '{actor_name}'")
            return pd.DataFrame(columns=['title', 'genres', 'vote_count', 'vote_average'])
        
        return scifi_actor_df.sort_values(by='vote_count', ascending=False)[
            ['title', 'genres', 'vote_count', 'vote_average']
        ].head(top_n)
    
    def movies_starring_actor_by_director(self, actor_name, director_name):
        """Return movies starring a specific actor and directed by a specific director."""
        # Filter for actor in cast (handle NaN cast values)
        actor_movies = self.df[
            self.df['cast'].fillna('').str.contains(actor_name, na=False, case=False)
        ]
        if actor_movies.empty:
            print(f"No movies found starring '{actor_name}'")
            return pd.DataFrame(columns=['title', 'directors', 'release_date'])
        
        # Filter for director (handle NaN directors values)
        director_actor_movies = actor_movies[
            actor_movies['directors'].fillna('').str.contains(director_name, na=False, case=False)
        ].sort_values(by='runtime', ascending=False)
        
        if director_actor_movies.empty:
            print(f"⚠ No movies found starring '{actor_name}' directed by '{director_name}'")
            return pd.DataFrame(columns=['title', 'directors', 'release_date'])
        
        return director_actor_movies[['title', 'directors', 'release_date']]  


    # BUDGET, REVENUE, ROI ANALYSIS

    def compute_mean_revenue(self):
        """Return mean revenue for Franchise vs Standalone."""
        return (
            self.df.groupby('movies_type')['revenue_musd']
            .mean()
            .rename("mean_revenue")
        )

    def compute_budget_stats(self):
        """Return total & mean budget for franchise and standalone."""
        return self.df.groupby('movies_type')['budget_musd'].agg(['sum', 'mean'])

    def compute_median_roi(self):
        """Return median ROI for franchise vs standalone."""
        return (
            self.df.groupby('movies_type')['roi']
            .median()
            .rename("median_roi")
        )

    def compute_mean_popularity(self):
        """Return mean popularity for franchise vs standalone."""
        return (
            self.df.groupby('movies_type')['popularity']
            .mean()
            .rename("mean_popularity")
        )

    def compute_mean_ratings(self):
        """Return mean vote_count for franchise vs standalone."""
        return (
            self.df.groupby('movies_type')['vote_count']
            .mean()
            .rename("mean_rating")
        )


    # FRANCHISE SUCCESS ANALYSIS
    def top_franchise_by_movie_count(self):
        """Return the franchise with the highest number of movies."""
        return (
            self.df['belongs_to_collection'].value_counts()
            .rename("movie_count")
        )

    def top_franchise_by_budget(self):
        """Return franchises sorted by total & mean budget."""
        return self.df.groupby('belongs_to_collection')['budget_musd'].agg(['sum', 'mean']).sort_values(
            by='sum', ascending=False
        )

    def top_franchise_by_revenue(self):
        """Return franchises sorted by total & mean revenue."""
        return self.df.groupby('belongs_to_collection')['revenue_musd'].agg(['sum', 'mean']).sort_values(
            by='sum', ascending=False
        )

    def top_franchise_by_rating(self):
        """Return franchises sorted by average vote rating."""
        return self.df.groupby('belongs_to_collection')['vote_average'].mean().sort_values(
            ascending=False
        )
    
    # successful director
    def top_director_by_no_of_movies(self):
        """Return directors sorted by number of movies directed."""
        return self.df.explode('directors').groupby('directors').size().sort_values(ascending=False).rename("movie_count")
    
    def top_director_by_revenue(self):
        """Return directors sorted by total revenue."""
        return self.df.explode('directors').groupby('directors')['revenue_musd'].sum().sort_values(ascending=False).rename("total_revenue")
    
    def top_director_by_rating(self):
        """Return directors sorted by average rating."""
        return self.df.explode('directors').groupby('directors')['vote_average'].mean().sort_values(ascending=False).rename("average_rating")
    
    # Full Report
    def full_report(self):
        """Return a dictionary of all computed analytics, excluding empty results."""
        report = {}
        
        # Actor-based queries (skip if empty)
        scifi_movies = self.best_scifi_movies_starring_actor('Harrison Ford')
        if not scifi_movies.empty:
            report["Best Science Fiction Movies"] = scifi_movies
        
        actor_director_movies = self.movies_starring_actor_by_director("Uma Thurman", "Quentin Tarantino")
        if not actor_director_movies.empty:
            report["Movies Starring Actor by Director"] = actor_director_movies
        
        # Always include aggregate stats
        report["Mean Revenue"] = self.compute_mean_revenue()
        report["Budget Stats"] = self.compute_budget_stats()
        report["Median ROI"] = self.compute_median_roi()
        report["Mean Popularity"] = self.compute_mean_popularity()
        report["Mean Rating"] = self.compute_mean_ratings()
        report["Top Franchise by Movie Count"] = self.top_franchise_by_movie_count()
        report["Top Franchise by Budget"] = self.top_franchise_by_budget()
        report["Top Franchise by Revenue"] = self.top_franchise_by_revenue()
        report["Top Franchise by Rating"] = self.top_franchise_by_rating()
        report["Top Director by Number of Movies"] = self.top_director_by_no_of_movies()
        report["Top Director by Revenue"] = self.top_director_by_revenue()
        report["Top Director by Rating"] = self.top_director_by_rating()
        
        return report
    

if __name__ == "__main__":
    # Example usage
    import os

    # Get the TMDB_movies_project directory (one level up from scripts/)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_dir = os.path.dirname(script_dir)
    data_dir = os.path.join(project_dir, 'data')

    # Load cleaned data
    cleaned_data_path = os.path.join(data_dir, 'movies_with_kpi.csv')
    movies_df = pd.read_csv(cleaned_data_path)

    analyzer = MovieAnalyzer(movies_df)

    #

    # Print full report
    report = analyzer.full_report()
    for key, value in report.items():
        print(f"\n{key}:\n{value}")
