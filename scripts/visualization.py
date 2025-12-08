import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

class MovieVisualizer:
    def __init__(self, df):
        """Initialize with a movie DataFrame."""
        self.df = df

    # 1. Revenue vs Budget Trend
    def plot_revenue_vs_budget(self):
        plt.figure(figsize=(12, 6))
        plt.scatter(self.df['budget'], self.df['revenue'], alpha=0.5)
        plt.title('Revenue vs Budget Trend')
        plt.xlabel('Budget')
        plt.ylabel('Revenue')
        plt.grid(False)
        plt.show()


    # 2. ROI Distribution by Genres
    def plot_roi_distribution(self):
        plt.figure(figsize=(12, 6))
        sns.histplot(
            x=self.df['roi'], 
            hue=self.df['genres'], 
            kde=True
        )
        plt.title('Distribution of ROI by Genres')
        plt.xlabel('ROI')
        plt.ylabel('Density / Count')
        plt.grid(False)
        plt.show()


    # 3. Popularity vs Rating
    def plot_popularity_vs_rating(self):
        plt.figure(figsize=(12, 6))
        sns.scatterplot(
            x=self.df['popularity'], 
            y=self.df['vote_average'], 
            alpha=0.5
        )
        plt.title('Popularity vs Rating')
        plt.xlabel('Popularity')
        plt.ylabel('Rating')
        plt.show()

    # -----------------------------------------------------------
    # 4. Yearly Trends in Box Office Performance
    # -----------------------------------------------------------
    def plot_yearly_revenue_trend(self):
        # Ensure release_date is datetime
        if not pd.api.types.is_datetime64_any_dtype(self.df['release_date']):
            self.df['release_date'] = pd.to_datetime(self.df['release_date'], errors='coerce')

        plt.figure(figsize=(12, 6))
        sns.lineplot(
            x=self.df['release_date'].dt.year, 
            y=self.df['revenue'], 
            marker='o'
        )
        plt.title('Yearly Trends in Box Office Performance')
        plt.xlabel('Year')
        plt.ylabel('Revenue')
        plt.grid(False)
        plt.show()

    # 5. Franchise vs Standalone Success
    def plot_franchise_vs_standalone(self):
        plt.figure(figsize=(12, 6))
        sns.barplot(
            x=self.df['movies_type'].value_counts().index, 
            y=self.df['movies_type'].value_counts().values
        )
        plt.title('Comparison of Franchise vs Standalone Success')
        plt.xlabel('Movie Type')
        plt.ylabel('Count')
        plt.show()

    #RUN ALL VISUALIZATIONS
    def run_all(self):
        self.plot_revenue_vs_budget()
        self.plot_roi_distribution()
        self.plot_popularity_vs_rating()
        self.plot_yearly_revenue_trend()
        self.plot_franchise_vs_standalone()
