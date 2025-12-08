import numpy as np
import pandas as pd

class MovieRanker:

    def __init__(self, df):
        """
        Initialize the ranker with a movie DataFrame.
        A copy is always made to avoid modifying original data.
        """
        self.df = df.copy()
        self.prepare_data()


    # HELPER METHODS
    def prepare_data(self):
        """Compute profit and ROI safely and create helper subsets."""

        # Profit
        self.df["profit"] = self.df["revenue_musd"] - self.df["budget_musd"]

        # ROI (protect against division by zero)
        self.df["roi"] = self.df["revenue_musd"] / self.df["budget_musd"].replace(0, np.nan)

        # ROI filter → Only movies with budget >= $10M
        self.roi_df = self.df[self.df["budget_musd"] >= 10]

    def get_title(self, row):
        """Extract a movie title safely."""
        if row is None:
            return "<Unknown Movie>"
        return row["title"] if ("title" in row and pd.notna(row["title"])) else "<Unknown Title>"

    def get_row_with_max(self, column):
        return self.df.loc[self.df[column].idxmax()]

    def get_row_with_min(self, column):
        return self.df.loc[self.df[column].idxmin()]

    # RANKINGS (EACH AS ITS OWN METHOD)
    def highest_revenue_movie(self):
        row = self.get_row_with_max("revenue_musd")
        return f"The highest revenue movie is '{self.get_title(row)}' with ${row['revenue_musd']:,} million."

    def highest_budget_movie(self):
        row = self.get_row_with_max("budget_musd")
        return f"The highest budgeted movie is '{self.get_title(row)}' with a budget of ${row['budget_musd']:,} million."
    def highest_profit_movie(self):
        row = self.get_row_with_max("profit")
        return f"The most profitable movie is '{self.get_title(row)}' with a profit of ${row['profit']:,}."

    def lowest_profit_movie(self):
        row = self.get_row_with_min("profit")
        return f"The least profitable movie is '{self.get_title(row)}' with a profit of ${row['profit']:,}."
    def highest_roi_movie(self):
        if self.roi_df.empty:
            return "No movie qualifies for ROI ranking (budget < $10M for all movies)."

        row = self.roi_df.loc[self.roi_df["roi"].idxmax()]
        return f"The movie with the highest ROI is '{self.get_title(row)}' with an ROI of {row['roi']:.2f}x."

    def lowest_roi_movie(self):
        if self.roi_df.empty:
            return "No movie qualifies for ROI ranking (budget < $10M for all movies)."

        row = self.roi_df.loc[self.roi_df["roi"].idxmin()]
        return f"The movie with the lowest ROI is '{self.get_title(row)}' with an ROI of {row['roi']:.2f}x."

    def most_voted_movie(self):
        row = self.get_row_with_max("vote_count")
        return f"The most voted movie is '{self.get_title(row)}' with {row['vote_count']:,} votes."
    def highest_rated_movie(self):
        row = self.get_row_with_max("vote_average")
        return f"The highest rated movie is '{self.get_title(row)}' with a rating of {row['vote_average']}."

    def lowest_rated_movie(self):
        row = self.get_row_with_min("vote_average")
        return f"The lowest rated movie is '{self.get_title(row)}' with a rating of {row['vote_average']}."
    def most_popular_movie(self):
        row = self.get_row_with_max("popularity")
        return f"The most popular movie is '{self.get_title(row)}' with a popularity score of {row['popularity']}."

    # RETURNS A NICE STRUCTURED DICTIONARY
    def get_summary(self):
        return {
            "highest_revenue_movie": self.highest_revenue_movie(),
            "highest_budget_movie": self.highest_budget_movie(),
            "highest_profit_movie": self.highest_profit_movie(),
            "lowest_profit_movie": self.lowest_profit_movie(),
            "highest_roi_movie": self.highest_roi_movie(),
            "lowest_roi_movie": self.lowest_roi_movie(),
            "most_voted_movie": self.most_voted_movie(),
            "highest_rated_movie": self.highest_rated_movie(),
            "lowest_rated_movie": self.lowest_rated_movie(),
            "most_popular_movie": self.most_popular_movie(),
        }
if __name__ == "__main__":
    import os
    
    # Get the TMDB_movies_project directory (one level up from scripts/)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_dir = os.path.dirname(script_dir)
    data_dir = os.path.join(project_dir, 'data')
    
    csv_path = os.path.join(data_dir, 'cleaned_tmdb_movies.csv')
    data = pd.read_csv(csv_path)
    ranker = MovieRanker(data)
    summary = ranker.get_summary()

    for key, value in summary.items():
        print(f"{key.replace('_', ' ').title()}: {value}")
    
    # Save the DataFrame with roi and profit columns
    output_csv_path = os.path.join(data_dir, 'movies_with_kpi.csv')
    ranker.df.to_csv(output_csv_path, index=False)
    print(f"\nDataFrame with KPI metrics saved to '{output_csv_path}'.")