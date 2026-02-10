import numpy as np
import pandas as pd
import logging
from typing import Dict, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class MovieRanker:
    """
    Enhanced movie ranking and KPI calculation with comprehensive validation.
    
    Design choices and rationale:
    
    ROI Filtering Threshold ($10M):
    - Movies with budgets under $10M often have unreliable budget reporting
    - Independent films may not report actual marketing costs
    - Studio accounting practices differ significantly for low-budget productions
    - $10M threshold captures most mainstream theatrical releases with reliable data
    
    Profit Calculation:
    - Simple formula: Revenue - Budget
    - Does not account for marketing costs (typically 50-100% of production budget)
    - Represents gross profit, not net profit
    
    ROI Calculation:
    - Formula: Revenue / Budget 
    - 1.0 = break-even, >1.0 = profitable, <1.0 = loss
    - Protects against division by zero with NaN replacement
    """

    def __init__(self, df: pd.DataFrame):
        """
        Initialize the ranker with comprehensive input validation.
        
        Args:
            df: Movie DataFrame with required columns
            
        Raises:
            ValueError: If input data is invalid or missing required columns
        """
        if df is None or df.empty:
            raise ValueError("Input DataFrame cannot be None or empty")
        
        # Validate required columns for KPI calculation
        required_columns = ['revenue_musd', 'budget_musd']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns for KPI calculation: {missing_columns}")
        
        logger.info(f"Initializing MovieRanker with {len(df)} movies")
        
        self.df = df.copy()
        self.roi_threshold_musd = 10  # $10M threshold for ROI analysis
        self._prepare_data()
        
        logger.info(f"KPI preparation complete. ROI-eligible movies: {len(self.roi_df)}")

    def _prepare_data(self):
        """
        Compute profit and ROI safely with comprehensive validation and logging.
        
        Raises:
            RuntimeError: If KPI computation fails
        """
        try:
            initial_count = len(self.df)
            
            # Validate numeric columns
            if not pd.api.types.is_numeric_dtype(self.df["revenue_musd"]):
                logger.warning("Revenue column is not numeric, attempting conversion")
                self.df["revenue_musd"] = pd.to_numeric(self.df["revenue_musd"], errors="coerce")
            
            if not pd.api.types.is_numeric_dtype(self.df["budget_musd"]):
                logger.warning("Budget column is not numeric, attempting conversion")
                self.df["budget_musd"] = pd.to_numeric(self.df["budget_musd"], errors="coerce")
            
            # Profit calculation (simple: Revenue - Budget)
            self.df["profit"] = self.df["revenue_musd"] - self.df["budget_musd"]
            profit_count = self.df["profit"].notna().sum()
            logger.info(f"Calculated profit for {profit_count}/{initial_count} movies")

            # ROI calculation with zero-division protection
            # Rationale: Replace zero budgets with NaN to avoid infinite ROI values
            budget_safe = self.df["budget_musd"].replace(0, np.nan)
            self.df["roi"] = self.df["revenue_musd"] / budget_safe
            
            roi_count = self.df["roi"].notna().sum()
            zero_budget_count = (self.df["budget_musd"] == 0).sum()
            
            if zero_budget_count > 0:
                logger.warning(f"Found {zero_budget_count} movies with zero budget - ROI set to NaN")
            
            logger.info(f"Calculated ROI for {roi_count}/{initial_count} movies")

            # ROI filter for reliable analysis
            # Rationale: $10M threshold filters out movies with potentially unreliable budget data
            self.roi_df = self.df[self.df["budget_musd"] >= self.roi_threshold_musd].copy()
            
            filtered_out = initial_count - len(self.roi_df)
            if filtered_out > 0:
                logger.info(f"Filtered out {filtered_out} movies with budget < ${self.roi_threshold_musd}M for ROI analysis")
            
            # Validate results
            self._validate_kpi_results()
            
        except Exception as e:
            logger.error(f"KPI preparation failed: {e}")
            raise RuntimeError(f"Failed to prepare KPI data: {e}") from e
    
    def _validate_kpi_results(self):
        """Validate KPI calculation results."""
        # Check for extreme outliers that might indicate data quality issues
        if "roi" in self.df.columns and self.df["roi"].notna().any():
            roi_stats = self.df["roi"].describe()
            if roi_stats["max"] > 100:  # ROI > 100x might indicate data issues
                logger.warning(f"Extremely high ROI detected: {roi_stats['max']:.2f}x - verify data quality")
            
            if roi_stats["min"] < 0:  # Negative ROI indicates revenue < budget
                negative_roi_count = (self.df["roi"] < 0).sum()
                logger.info(f"Found {negative_roi_count} movies with negative ROI (revenue < budget)")
        
        # Log summary statistics
        if "profit" in self.df.columns:
            profit_positive = (self.df["profit"] > 0).sum()
            profit_total = self.df["profit"].notna().sum()
            if profit_total > 0:
                profit_rate = profit_positive / profit_total * 100
                logger.info(f"Profitability rate: {profit_rate:.1f}% ({profit_positive}/{profit_total})")

    def get_title(self, row: Optional[pd.Series]) -> str:
        """Extract a movie title safely with validation."""
        if row is None or row.empty:
            return "<Unknown Movie>"
        return row.get("title", "<Unknown Title>") if pd.notna(row.get("title")) else "<Unknown Title>"

    def _get_row_with_max_safe(self, column: str) -> Optional[pd.Series]:
        """Safely get row with maximum value, handling edge cases."""
        if column not in self.df.columns:
            logger.error(f"Column '{column}' not found in DataFrame")
            return None
        
        col_data = self.df[column]
        if col_data.isna().all():
            logger.warning(f"All values in column '{column}' are NaN")
            return None
        
        try:
            max_idx = col_data.idxmax()
            return self.df.loc[max_idx]
        except Exception as e:
            logger.error(f"Error finding maximum in column '{column}': {e}")
            return None
    
    def _get_row_with_min_safe(self, column: str) -> Optional[pd.Series]:
        """Safely get row with minimum value, handling edge cases."""
        if column not in self.df.columns:
            logger.error(f"Column '{column}' not found in DataFrame")
            return None
        
        col_data = self.df[column]
        if col_data.isna().all():
            logger.warning(f"All values in column '{column}' are NaN")
            return None
        
        try:
            min_idx = col_data.idxmin()
            return self.df.loc[min_idx]
        except Exception as e:
            logger.error(f"Error finding minimum in column '{column}': {e}")
            return None

    # Enhanced ranking methods with better error handling
    def highest_revenue_movie(self) -> str:
        """Get the movie with highest revenue."""
        row = self._get_row_with_max_safe("revenue_musd")
        if row is None:
            return "No revenue data available"
        return f"The highest revenue movie is '{self.get_title(row)}' with ${row['revenue_musd']:.2f} million."

    def highest_budget_movie(self) -> str:
        """Get the movie with highest budget."""
        row = self._get_row_with_max_safe("budget_musd")
        if row is None:
            return "No budget data available"
        return f"The highest budgeted movie is '{self.get_title(row)}' with a budget of ${row['budget_musd']:.2f} million."
    
    def highest_profit_movie(self) -> str:
        """Get the most profitable movie."""
        row = self._get_row_with_max_safe("profit")
        if row is None:
            return "No profit data available"
        return f"The most profitable movie is '{self.get_title(row)}' with a profit of ${row['profit']:.2f} million."

    def lowest_profit_movie(self) -> str:
        """Get the least profitable movie.""" 
        row = self._get_row_with_min_safe("profit")
        if row is None:
            return "No profit data available"
        return f"The least profitable movie is '{self.get_title(row)}' with a profit of ${row['profit']:.2f} million."
    
    def highest_roi_movie(self) -> str:
        """
        Get the movie with highest ROI from the filtered dataset.
        
        Note: Uses movies with budget >= $10M for reliable ROI analysis.
        """
        if self.roi_df.empty:
            return f"No movie qualifies for ROI ranking (budget < ${self.roi_threshold_musd}M for all movies)."

        roi_col = self.roi_df["roi"]
        if roi_col.isna().all():
            return "No valid ROI data available for qualified movies"
        
        try:
            max_idx = roi_col.idxmax()
            row = self.roi_df.loc[max_idx]
            return f"The movie with the highest ROI is '{self.get_title(row)}' with an ROI of {row['roi']:.2f}x (budget >= ${self.roi_threshold_musd}M)."
        except Exception as e:
            logger.error(f"Error calculating highest ROI: {e}")
            return "Error calculating highest ROI"

    def lowest_roi_movie(self) -> str:
        """
        Get the movie with lowest ROI from the filtered dataset.
        
        Note: Uses movies with budget >= $10M for reliable ROI analysis.
        """
        if self.roi_df.empty:
            return f"No movie qualifies for ROI ranking (budget < ${self.roi_threshold_musd}M for all movies)."

        roi_col = self.roi_df["roi"]
        if roi_col.isna().all():
            return "No valid ROI data available for qualified movies"
        
        try:
            min_idx = roi_col.idxmin()
            row = self.roi_df.loc[min_idx]
            return f"The movie with the lowest ROI is '{self.get_title(row)}' with an ROI of {row['roi']:.2f}x (budget >= ${self.roi_threshold_musd}M)."
        except Exception as e:
            logger.error(f"Error calculating lowest ROI: {e}")
            return "Error calculating lowest ROI"

    def most_voted_movie(self) -> str:
        """Get the most voted movie."""
        row = self._get_row_with_max_safe("vote_count")
        if row is None:
            return "No vote count data available"
        return f"The most voted movie is '{self.get_title(row)}' with {row['vote_count']:,} votes."
        
    def highest_rated_movie(self) -> str:
        """Get the highest rated movie."""
        row = self._get_row_with_max_safe("vote_average")
        if row is None:
            return "No rating data available"
        return f"The highest rated movie is '{self.get_title(row)}' with a rating of {row['vote_average']:.1f}."

    def lowest_rated_movie(self) -> str:
        """Get the lowest rated movie."""
        row = self._get_row_with_min_safe("vote_average")
        if row is None:
            return "No rating data available"
        return f"The lowest rated movie is '{self.get_title(row)}' with a rating of {row['vote_average']:.1f}."
        
    def most_popular_movie(self) -> str:
        """Get the most popular movie by TMDB popularity score."""
        row = self._get_row_with_max_safe("popularity")
        if row is None:
            return "No popularity data available"
        return f"The most popular movie is '{self.get_title(row)}' with a popularity score of {row['popularity']:.2f}."

    def get_summary(self) -> Dict[str, str]:
        """
        Get comprehensive movie ranking summary with error handling.
        
        Returns:
            Dictionary with ranking results for all KPI categories
        """
        logger.info("Generating comprehensive movie ranking summary")
        
        try:
            summary = {
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
            
            logger.info("Movie ranking summary generated successfully")
            return summary
            
        except Exception as e:
            logger.error(f"Error generating summary: {e}")
            return {"error": f"Failed to generate summary: {e}"}
if __name__ == "__main__":
    """
    Standalone execution for testing and development.
    
    Demonstrates proper error handling when running KPI calculation independently.
    """
    try:
        import os
        
        # Setup paths
        script_dir = os.path.dirname(os.path.abspath(__file__))
        project_dir = os.path.dirname(script_dir)
        data_dir = os.path.join(project_dir, 'data')
        
        input_path = os.path.join(data_dir, 'cleaned_tmdb_movies.csv')
        output_path = os.path.join(data_dir, 'movies_with_kpi.csv')
        
        # Validate input file exists
        if not os.path.exists(input_path):
            logger.error(f"Input file not found: {input_path}")
            logger.info("Please run cleaning.py first to generate cleaned data")
            exit(1)
        
        # Load and validate data
        logger.info(f"Loading cleaned data from: {input_path}")
        data = pd.read_csv(input_path)
        
        if data.empty:
            raise ValueError("Input file contains no data")
        
        logger.info(f"Loaded {len(data)} movies for KPI calculation")
        
        # Calculate KPIs
        ranker = MovieRanker(data)
        summary = ranker.get_summary()

        # Display results
        print("\n" + "="*50)
        print("MOVIE KPI ANALYSIS SUMMARY")
        print("="*50)
        
        for key, value in summary.items():
            formatted_key = key.replace('_', ' ').title()
            print(f"\n{formatted_key}:")
            print(f"  {value}")
        
        # Save results
        os.makedirs(data_dir, exist_ok=True)
        ranker.df.to_csv(output_path, index=False)
        logger.info(f"KPI data saved to: {output_path}")
        
        # Summary statistics
        kpi_stats = {
            'total_movies': len(ranker.df),
            'movies_with_profit_data': ranker.df['profit'].notna().sum(),
            'movies_with_roi_data': ranker.df['roi'].notna().sum(),
            'roi_eligible_movies': len(ranker.roi_df)
        }
        
        print("\n" + "="*30)
        print("KPI CALCULATION STATISTICS")
        print("="*30)
        for key, value in kpi_stats.items():
            formatted_key = key.replace('_', ' ').title()
            print(f"{formatted_key}: {value}")
        
        print(f"\nKPI-enhanced dataset saved to: {output_path}")
        
    except Exception as e:
        logger.error(f"KPI calculation process failed: {e}")
        print(f"\n❌ Error: {e}")
        print("Check the log for detailed error information")
        raise