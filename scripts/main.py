import os
import pandas as pd
import logging
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from dotenv import load_dotenv
from extraction import TMDBExtractor
from cleaning import MovieDataCleaner
from KPI import MovieRanker
from filtering import MovieAnalyzer

# Load environment variables from .env file (optional)
load_dotenv()

# Configure comprehensive logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('tmdb_pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configuration constants
DEFAULT_MOVIE_IDS = [299534, 19995, 140607, 299536, 597, 135397, 420818, 24428, 168259, 99861,
                     284054, 12445, 181808, 330457, 351286, 109445, 321612, 260513]
REQUIRED_COLUMNS_POST_CLEANING = ['id', 'title', 'budget_musd', 'revenue_musd']


class PipelineConfig:
    """Configuration management for the TMDB pipeline."""
    
    def __init__(self, api_key: Optional[str] = None, movie_ids: Optional[List[int]] = None):
        """Initialize pipeline configuration with validation."""
        self.api_key = self.validate_api_key(api_key)
        self.movie_ids = self.validate_movie_ids(movie_ids or DEFAULT_MOVIE_IDS)
        self.base_paths = self.setup_paths()
        
        logger.info(f"Pipeline configured with {len(self.movie_ids)} movies")
    
    def validate_api_key(self, api_key: Optional[str]) -> str:
        """Validate and retrieve API key."""
        final_key = api_key or os.getenv("TMDB_API_KEY")
        if not final_key:
            raise ValueError("TMDB API key is required. Set USER_API_KEY or TMDB_API_KEY environment variable.")
        
        if len(final_key.strip()) < 10:  # Basic validation
            raise ValueError("API key appears to be invalid (too short)")
            
        return final_key.strip()
    
    def validate_movie_ids(self, movie_ids: List[int]) -> List[int]:
        """Validate movie IDs list."""
        if not movie_ids or len(movie_ids) == 0:
            raise ValueError("At least one movie ID is required")
            
        # Validate each ID
        valid_ids = []
        for movie_id in movie_ids:
            try:
                id_int = int(movie_id)
                if id_int > 0:
                    valid_ids.append(id_int)
                else:
                    logger.warning(f"Skipping invalid movie ID: {movie_id} (must be positive)")
            except (ValueError, TypeError):
                logger.warning(f"Skipping invalid movie ID: {movie_id} (not a number)")
        
        if not valid_ids:
            raise ValueError("No valid movie IDs provided")
            
        return valid_ids
    
    def setup_paths(self) -> Dict[str, Path]:
        """Setup and validate file paths."""
        script_dir = Path(os.path.dirname(os.path.abspath(__file__)))
        project_dir = script_dir.parent
        data_dir = project_dir / "data"
        
        # Ensure data directory exists
        data_dir.mkdir(exist_ok=True)
        
        return {
            'project': project_dir,
            'data': data_dir,
            'raw_csv': data_dir / "rawextracted_tmdb_movies.csv",
            'cleaned_csv': data_dir / "cleaned_tmdb_movies.csv",
            'kpi_csv': data_dir / "movies_with_kpi.csv",
            'report': data_dir / "analysis_report.json"
        }


class TMDBMoviePipeline:
    """
    Enhanced TMDB movie data pipeline with comprehensive error handling,
    logging, and recovery mechanisms.
    
    Design choices:
    - Modular design allows independent execution of pipeline stages
    - Comprehensive error handling with detailed logging
    - Data validation at each stage prevents cascading failures
    - Configuration validation ensures reliable execution
    - Recovery mechanisms allow restarting from intermediate stages
    """
    
    def __init__(self, config: PipelineConfig):
        """Initialize pipeline with validated configuration."""
        self.config = config
        self.extraction_stats = {}
        self.cleaning_stats = {}
        self.kpi_stats = {}
        
        logger.info("TMDB Movie Pipeline initialized")
    
    def extract_movies(self, skip_existing: bool = True) -> Tuple[List[dict], bool]:
        """
        Extract movie data with error recovery and progress tracking.
        
        Args:
            skip_existing: Skip extraction if raw data file exists
            
        Returns:
            Tuple of (movies_data, extraction_performed)
            
        Raises:
            RuntimeError: If extraction fails completely
        """
        raw_path = self.config.base_paths['raw_csv']
        
        # Check if we should skip extraction
        if skip_existing and raw_path.exists():
            logger.info(f"Loading existing raw data from {raw_path}")
            try:
                existing_df = pd.read_csv(raw_path)
                if not existing_df.empty:
                    movies_data = existing_df.to_dict('records')
                    logger.info(f"Loaded {len(movies_data)} movies from existing file")
                    return movies_data, False
            except Exception as e:
                logger.warning(f"Failed to load existing raw data: {e}. Proceeding with fresh extraction.")
        
        logger.info(f"Starting extraction for {len(self.config.movie_ids)} movies")
        
        try:
            extractor = TMDBExtractor(
                self.config.api_key,
                max_retries=3,
                timeout=30,
                delay_between_requests=0.5
            )
            
            movies_data = extractor.extract_tmdb_data(self.config.movie_ids)
            
            if not movies_data:
                raise RuntimeError("No movie data extracted")
            
            # Save raw data immediately
            raw_df = pd.DataFrame(movies_data)
            raw_df.to_csv(raw_path, index=False)
            
            self.extraction_stats = {
                'total_requested': len(self.config.movie_ids),
                'successfully_extracted': len(movies_data),
                'extraction_rate': len(movies_data) / len(self.config.movie_ids) * 100,
                'timestamp': datetime.now().isoformat()
            }
            
            logger.info(f"Extraction completed. Success rate: {self.extraction_stats['extraction_rate']:.1f}%")
            logger.info(f"Raw data saved to: {raw_path}")
            
            return movies_data, True
            
        except Exception as e:
            logger.error(f"Movie extraction failed: {e}")
            raise RuntimeError(f"Data extraction failed: {e}") from e
    
    def clean_and_save_data(self, movies_data: List[dict], skip_existing: bool = True) -> pd.DataFrame:
        """
        Clean movie data with comprehensive validation and error handling.
        
        Args:
            movies_data: Raw movie data from extraction
            skip_existing: Skip cleaning if cleaned data file exists
            
        Returns:
            Cleaned DataFrame
            
        Raises:
            RuntimeError: If cleaning fails
        """
        cleaned_path = self.config.base_paths['cleaned_csv']
        
        # Check if we should skip cleaning
        if skip_existing and cleaned_path.exists():
            logger.info(f"Loading existing cleaned data from {cleaned_path}")
            try:
                existing_df = pd.read_csv(cleaned_path)
                if not existing_df.empty and self._validate_cleaned_data(existing_df):
                    logger.info(f"Loaded {len(existing_df)} cleaned movies from existing file")
                    return existing_df
            except Exception as e:
                logger.warning(f"Failed to load existing cleaned data: {e}. Proceeding with fresh cleaning.")
        
        logger.info(f"Starting data cleaning for {len(movies_data)} movies")
        
        try:
            # Convert to DataFrame and validate
            df = pd.DataFrame(movies_data)
            if df.empty:
                raise ValueError("Input data is empty")
            
            initial_shape = df.shape
            
            # Clean data
            cleaner = MovieDataCleaner(df)
            cleaned_df = cleaner.clean()
            
            # Validate cleaning results
            if not self.validate_cleaned_data(cleaned_df):
                raise RuntimeError("Data validation failed after cleaning")
            
            # Save cleaned data
            cleaned_df.to_csv(cleaned_path, index=False)
            
            self.cleaning_stats = {
                'initial_shape': initial_shape,
                'final_shape': cleaned_df.shape,
                'data_quality_score': self.calculate_data_quality_score(cleaned_df),
                'timestamp': datetime.now().isoformat()
            }
            
            logger.info(f"Cleaning completed. Shape: {initial_shape} -> {cleaned_df.shape}")
            logger.info(f"Cleaned data saved to: {cleaned_path}")
            
            return cleaned_df
            
        except Exception as e:
            logger.error(f"Data cleaning failed: {e}")
            raise RuntimeError(f"Data cleaning failed: {e}") from e
    
    def validate_cleaned_data(self, df: pd.DataFrame) -> bool:
        """Validate that cleaned data meets quality requirements."""
        if df.empty:
            logger.error("Cleaned DataFrame is empty")
            return False
        
        # Check for required columns
        missing_required = [col for col in REQUIRED_COLUMNS_POST_CLEANING if col not in df.columns]
        if missing_required:
            logger.error(f"Missing required columns after cleaning: {missing_required}")
            return False
        
        # Check data quality
        if df['id'].isna().any():
            logger.error("Found NaN values in ID column")
            return False
        
        if df['title'].isna().all():
            logger.error("All movie titles are missing")
            return False
        
        return True
    
    def calculate_data_quality_score(self, df: pd.DataFrame) -> float:
        """Calculate overall data quality score (0-100)."""
        if df.empty:
            return 0.0
        
        # Calculate percentage of non-null values across all columns
        total_cells = df.shape[0] * df.shape[1]
        non_null_cells = df.notna().sum().sum()
        
        return (non_null_cells / total_cells * 100) if total_cells > 0 else 0.0
    
    def compute_kpis_and_save(self, cleaned_df: pd.DataFrame, skip_existing: bool = True) -> pd.DataFrame:
        """
        Compute KPIs with validation and error handling.
        
        Args:
            cleaned_df: Cleaned movie DataFrame
            skip_existing: Skip KPI computation if file exists
            
        Returns:
            DataFrame with KPI metrics
            
        Raises:
            RuntimeError: If KPI computation fails
        """
        kpi_path = self.config.base_paths['kpi_csv']
        
        # Check if we should skip KPI computation
        if skip_existing and kpi_path.exists():
            logger.info(f"Loading existing KPI data from {kpi_path}")
            try:
                existing_df = pd.read_csv(kpi_path)
                if not existing_df.empty and self.validate_kpi_data(existing_df):
                    logger.info(f"Loaded {len(existing_df)} movies with KPI data from existing file")
                    return existing_df
            except Exception as e:
                logger.warning(f"Failed to load existing KPI data: {e}. Proceeding with fresh computation.")
        
        logger.info("Starting KPI computation")
        
        try:
            # Validate input data
            if not self.validate_cleaned_data(cleaned_df):
                raise ValueError("Input data validation failed")
            
            ranker = MovieRanker(cleaned_df)
            kpi_df = ranker.df
            
            # Validate KPI results
            if not self.validate_kpi_data(kpi_df):
                raise RuntimeError("KPI validation failed")
            
            # Save KPI data
            kpi_df.to_csv(kpi_path, index=False)
            
            self.kpi_stats = {
                'total_movies': len(kpi_df),
                'movies_with_profit_data': kpi_df['profit'].notna().sum(),
                'movies_with_roi_data': kpi_df['roi'].notna().sum(),
                'avg_roi': kpi_df['roi'].mean() if 'roi' in kpi_df.columns else None,
                'timestamp': datetime.now().isoformat()
            }
            
            logger.info(f"KPI computation completed for {len(kpi_df)} movies")
            logger.info(f"KPI data saved to: {kpi_path}")
            
            return kpi_df
            
        except Exception as e:
            logger.error(f"KPI computation failed: {e}")
            raise RuntimeError(f"KPI computation failed: {e}") from e
    
    def validate_kpi_data(self, df: pd.DataFrame) -> bool:
        """Validate KPI data quality."""
        if df.empty:
            logger.error("KPI DataFrame is empty")
            return False
        
        required_kpi_cols = ['profit', 'roi']
        missing_kpi = [col for col in required_kpi_cols if col not in df.columns]
        if missing_kpi:
            logger.error(f"Missing KPI columns: {missing_kpi}")
            return False
        
        return True
    
    def perform_analysis(self, kpi_df: pd.DataFrame) -> Dict:
        """
        Perform comprehensive analysis with error handling.
        
        Args:
            kpi_df: DataFrame with KPI data
            
        Returns:
            Analysis results dictionary
            
        Raises:
            RuntimeError: If analysis fails
        """
        logger.info("Starting comprehensive analysis")
        
        try:
            if not self.validate_kpi_data(kpi_df):
                raise ValueError("KPI data validation failed for analysis")
            
            analyzer = MovieAnalyzer(kpi_df)
            analysis_report = analyzer.full_report()
            
            # Save analysis report
            report_path = self.config.base_paths['report']
            with open(report_path, 'w', encoding='utf-8') as f:
                json.dump(analysis_report, f, indent=2, default=str)
            
            logger.info(f"Analysis completed and saved to: {report_path}")
            
            return analysis_report
            
        except Exception as e:
            logger.error(f"Analysis failed: {e}")
            raise RuntimeError(f"Analysis failed: {e}") from e
    
    def visualize_data(self, kpi_df: pd.DataFrame) -> bool:
        """
        Create visualizations with error handling.
        
        Args:
            kpi_df: DataFrame with KPI data
            
        Returns:
            True if visualization succeeded, False otherwise
        """
        logger.info("Starting data visualization")
        
        try:
            from visualization import MovieVisualizer
            
            if not self.validate_kpi_data(kpi_df):
                logger.warning("KPI data validation failed for visualization")
                return False
            
            visualizer = MovieVisualizer(kpi_df)
            
            # Execute visualizations with individual error handling
            viz_results = {}
            viz_methods = [
                'plot_revenue_vs_budget',
                'plot_roi_distribution', 
                'plot_popularity_vs_rating',
                'plot_yearly_revenue_trend'
            ]
            
            for method_name in viz_methods:
                try:
                    method = getattr(visualizer, method_name)
                    method()
                    viz_results[method_name] = True
                    logger.info(f"Successfully created {method_name}")
                except Exception as e:
                    viz_results[method_name] = False
                    logger.warning(f"Failed to create {method_name}: {e}")
            
            successful_viz = sum(viz_results.values())
            logger.info(f"Visualization completed: {successful_viz}/{len(viz_methods)} plots created")
            
            return successful_viz > 0
            
        except ImportError as e:
            logger.warning(f"Visualization module not available: {e}")
            return False
        except Exception as e:
            logger.error(f"Visualization failed: {e}")
            return False
    
    def run_full_pipeline(self, skip_existing_steps: bool = True) -> Dict:
        """
        Execute the complete pipeline with comprehensive error handling and recovery.
        
        Args:
            skip_existing_steps: Skip steps if intermediate files exist
            
        Returns:
            Pipeline execution summary
            
        Raises:
            RuntimeError: If critical pipeline steps fail
        """
        pipeline_start = datetime.now()
        logger.info("="*50)
        logger.info("STARTING TMDB MOVIE ANALYSIS PIPELINE")
        logger.info("="*50)
        
        execution_summary = {
            'start_time': pipeline_start.isoformat(),
            'config': {
                'movie_count': len(self.config.movie_ids),
                'skip_existing': skip_existing_steps
            },
            'steps': {},
            'errors': []
        }
        
        try:
            # Step 1: Extract data
            logger.info("STEP 1: Data Extraction")
            movies_data, extraction_performed = self.extract_movies(skip_existing_steps)
            execution_summary['steps']['extraction'] = {
                'performed': extraction_performed,
                'movies_extracted': len(movies_data),
                'stats': self.extraction_stats
            }
            
            # Step 2: Clean data  
            logger.info("STEP 2: Data Cleaning")
            cleaned_df = self.clean_and_save_data(movies_data, skip_existing_steps)
            execution_summary['steps']['cleaning'] = {
                'final_movie_count': len(cleaned_df),
                'stats': self.cleaning_stats
            }
            
            # Step 3: Compute KPIs
            logger.info("STEP 3: KPI Computation")
            kpi_df = self.compute_kpis_and_save(cleaned_df, skip_existing_steps)
            execution_summary['steps']['kpi'] = {
                'kpi_movie_count': len(kpi_df),
                'stats': self.kpi_stats
            }
            
            # Step 4: Analysis
            logger.info("STEP 4: Data Analysis")
            analysis_report = self.perform_analysis(kpi_df)
            execution_summary['steps']['analysis'] = {
                'report_sections': list(analysis_report.keys()) if analysis_report else []
            }
            
            # Step 5: Visualization (non-critical)
            logger.info("STEP 5: Data Visualization")
            viz_success = self.visualize_data(kpi_df)
            execution_summary['steps']['visualization'] = {
                'success': viz_success
            }
            
            # Pipeline completion
            pipeline_end = datetime.now()
            execution_duration = pipeline_end - pipeline_start
            
            execution_summary.update({
                'end_time': pipeline_end.isoformat(),
                'duration_seconds': execution_duration.total_seconds(),
                'success': True
            })
            
            logger.info("="*50)
            logger.info(f"PIPELINE COMPLETED SUCCESSFULLY in {execution_duration}")
            logger.info(f"Final dataset: {len(kpi_df)} movies with complete analysis")
            logger.info("="*50)
            
            # Print key results
            self._print_pipeline_summary(execution_summary, analysis_report)
            
            return execution_summary
            
        except Exception as e:
            execution_summary['success'] = False
            execution_summary['error'] = str(e)
            execution_summary['end_time'] = datetime.now().isoformat()
            
            logger.error("="*50)
            logger.error(f"PIPELINE FAILED: {e}")
            logger.error("="*50)
            
            raise RuntimeError(f"Pipeline execution failed: {e}") from e
    
    def _print_pipeline_summary(self, execution_summary: Dict, analysis_report: Dict):
        """Print a human-readable pipeline summary."""
        print("\n" + "="*50)
        print("TMDB MOVIE PIPELINE - EXECUTION SUMMARY")
        print("="*50)
        
        # Basic stats
        if 'extraction' in execution_summary['steps']:
            extraction_stats = execution_summary['steps']['extraction']
            print(f"Movies Extracted: {extraction_stats['movies_extracted']}")
        
        if 'cleaning' in execution_summary['steps']:
            cleaning_stats = execution_summary['steps']['cleaning']
            print(f"Movies After Cleaning: {cleaning_stats['final_movie_count']}")
            
            if 'stats' in cleaning_stats and 'data_quality_score' in cleaning_stats['stats']:
                quality_score = cleaning_stats['stats']['data_quality_score']
                print(f"Data Quality Score: {quality_score:.1f}%")
        
        if 'kpi' in execution_summary['steps']:
            kpi_stats = execution_summary['steps']['kpi']
            print(f"Movies with KPI Data: {kpi_stats['kpi_movie_count']}")
        
        # Analysis highlights
        if analysis_report:
            print("\nANALYSIS HIGHLIGHTS:")
            for section, content in analysis_report.items():
                if isinstance(content, str) and len(content) < 200:
                    print(f"   • {section}: {content}")
        
        print(f"\n⏱Total Execution Time: {execution_summary.get('duration_seconds', 0):.2f} seconds")
        print("="*50)


def main():
    """
    Main function with comprehensive error handling and user-friendly interface.
    
    This function demonstrates best practices for:
    - Configuration validation
    - Error recovery
    - User feedback
    - Resource cleanup
    """
    try:
        # Configuration with user-friendly error messages
        logger.info("Initializing TMDB Movie Analysis Pipeline")
        
        # Allow user overrides via environment or direct assignment
        api_key = os.getenv("USER_API_KEY") or os.getenv("TMDB_API_KEY")
        
        # Parse movie IDs from environment if provided
        env_movie_ids = os.getenv("USER_MOVIE_IDS")
        if env_movie_ids:
            try:
                movie_ids = [int(x.strip()) for x in env_movie_ids.split(",")]
            except ValueError:
                logger.warning("Invalid USER_MOVIE_IDS format, using defaults")
                movie_ids = DEFAULT_MOVIE_IDS
        else:
            movie_ids = DEFAULT_MOVIE_IDS
        
        # Initialize configuration
        config = PipelineConfig(api_key=api_key, movie_ids=movie_ids)
        
        # Initialize and run pipeline
        pipeline = TMDBMoviePipeline(config)
        
        # Get user preference for skipping existing files
        skip_existing = os.getenv("SKIP_EXISTING", "true").lower() in ("true", "1", "yes")
        
        # Execute pipeline
        execution_summary = pipeline.run_full_pipeline(skip_existing_steps=skip_existing)
        
        # Success message
        print("\nPipeline completed successfully!")
        print(f"Results saved to: {config.base_paths['data']}")
        
        return execution_summary
        
    except ValueError as e:
        logger.error(f"Configuration error: {e}")
        print(f"\nConfiguration Error: {e}")
        print("\nPlease check your:")
        print("- TMDB API key (set TMDB_API_KEY environment variable)")
        print("- Movie IDs (should be positive integers)")
        return None
        
    except RuntimeError as e:
        logger.error(f"Pipeline execution failed: {e}")
        print(f"\nPipeline Failed: {e}")
        print("\nCheck the log file 'tmdb_pipeline.log' for detailed error information")
        return None
        
    except KeyboardInterrupt:
        logger.info("Pipeline interrupted by user")
        print("\nPipeline interrupted by user")
        return None
        
    except Exception as e:
        logger.exception(f"Unexpected error: {e}")
        print(f"\nUnexpected Error: {e}")
        print("Check the log file 'tmdb_pipeline.log' for detailed error information")
        return None


if __name__ == "__main__":
    """
    Entry point with environment setup and graceful error handling.
    
    Environment Variables:
    - TMDB_API_KEY: Your TMDB API key (required)
    - USER_MOVIE_IDS: Comma-separated movie IDs (optional)
    - SKIP_EXISTING: Skip processing if files exist (default: true)
    """
    import sys
    
    print("TMDB Movie Analysis Pipeline")
    print("=" * 50)
    
    try:
        result = main()
        
        if result is None:
            print("\nPipeline did not complete successfully")
            sys.exit(1)
        else:
            print(f"\nPipeline completed in {result.get('duration_seconds', 0):.2f} seconds")
            sys.exit(0)
            
    except KeyboardInterrupt:
        print("\n\nGoodbye!")
        sys.exit(0)
