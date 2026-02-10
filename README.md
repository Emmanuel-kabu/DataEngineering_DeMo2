# TMDB Movies Analysis Project

A comprehensive Python data pipeline for extracting, cleaning, analyzing, and visualizing The Movie Database (TMDB) movie data.

## Project Overview

This project extracts movie data from TMDB API, performs data cleaning and transformation, calculates KPIs (profit, ROI), and provides analytical insights on movies, franchises, directors, and actors.

### Key Features
- **Data Extraction** — Fetch movie data from TMDB API
- **Data Cleaning** — Parse JSON fields, handle missing values, normalize data types
- **KPI Calculation** — Compute profit, ROI, and other financial metrics
- **Advanced Analytics** — Franchise analysis, director rankings, actor filtering
- **Data Visualization** — Generate visual reports and insights

## Project Structure

```
TMDB_movies_project/
├── scripts/
│   ├── extraction.py          # Extract data from TMDB API
│   ├── cleaning.py            # Data cleaning and transformation
│   ├── KPI.py                 # KPI calculations (profit, ROI)
│   ├── filtering.py           # Advanced filtering and analytics
│   ├── visualization.py       # Data visualization (charts, plots)
│   ├── array.py               # Array operations (if applicable)
│   └── __init__.py
├── data/
│   ├── rawextracted_tmdb_movies.csv     # Raw API output
│   ├── cleaned_tmdb_movies.csv          # Cleaned data
│   └── movies_with_kpi.csv              # Data with KPI metrics
├── notebooks/                 # Jupyter notebooks for exploration
├── requirements.txt           # Python dependencies
├── .gitignore                 # Git ignore rules
└── README.md                  # This file
```

## Getting Started

### Prerequisites
- Python 3.8+
- pip (Python package manager)
- TMDB API key (get one at https://www.themoviedb.org/settings/api)

### Installation

1. **Clone the repository**
```bash
git clone <repository-url>
cd TMDB_movies_project
```

2. **Create and activate virtual environment**
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. **Install dependencies**
```bash
pip install -r requirements.txt
```

4. **Set up environment variables**
Create a `.env` file in the project root:
```
TMDB_API_KEY=your_api_key_here
```

### Usage

Run the scripts in order:

1. **Extract data from TMDB API**
```bash
python scripts/extraction.py
```
Output: `data/rawextracted_tmdb_movies.csv`

2. **Clean and transform data**
```bash
python scripts/cleaning.py
```
Output: `data/cleaned_tmdb_movies.csv`

3. **Calculate KPIs**
```bash
python scripts/KPI.py
```
Output: `data/movies_with_kpi.csv`

4. **Run analytics**
```bash
python scripts/filtering.py
```

5. **Generate visualizations** (if applicable)
```bash
python scripts/visualization.py
```

## Data Pipeline

### Extraction (`extraction.py`)
- Fetches movie data from TMDB API
- Extracts details: title, budget, revenue, ratings, genres, cast, crew, etc.
- Saves raw data to CSV

### Cleaning (`cleaning.py`)
- Parses JSON fields (genres, production companies, etc.)
- Handles missing values and duplicates
- Converts data types (numeric, datetime)
- Renames columns (`budget_musd`, `revenue_musd`)
- Creates derived fields: `cast`, `crew`, `directors`, `cast_size`, `crew_size`
- Outputs clean, structured data

### KPI Calculation (`KPI.py`)
- Computes **Profit** = Revenue - Budget
- Computes **ROI** (Return on Investment) = Revenue / Budget
- Ranks movies by various metrics
- Exports results with KPI columns

### Analytics (`filtering.py`)
- Filters movies by genre, actors, directors
- Analyzes franchise performance
- Ranks directors by movies, revenue, ratings
- Computes statistics: mean revenue, budget stats, median ROI, etc.

### Visualization (`visualization.py`)
- Creates charts and plots
- Generates visual reports

##  Main Classes

### `TMDBExtractor` (extraction.py)
Fetches movie data from TMDB API.
```python
extractor = TMDBExtractor(api_key)
movies_data = extractor.extract_tmdb_data(movie_ids)
```

### `MovieDataCleaner` (cleaning.py)
Cleans and transforms raw data.
```python
cleaner = MovieDataCleaner(raw_df)
cleaned_df = cleaner.clean()
```

### `MovieRanker` (KPI.py)
Calculates KPIs and rankings.
```python
ranker = MovieRanker(cleaned_df)
summary = ranker.get_summary()
```

### `MovieAnalyzer` (filtering.py)
Performs advanced analytics and filtering.
```python
analyzer = MovieAnalyzer(kpi_df)
report = analyzer.full_report()
```

## Dependencies

See `requirements.txt` for full list. Key packages:
- **pandas** — Data manipulation
- **requests** — HTTP requests to TMDB API
- **numpy** — Numerical operations
- **python-dotenv** — Environment variable management

## Security & Best Practices

### Before Pushing to Production

**Secrets & API Keys**
- Never commit `.env` files or API keys
- Use environment variables for sensitive data
- Rotate API keys regularly

**Data Privacy**
- Exclude raw data files (`*.csv`, `*.json`)
- Be mindful of personal data in cast/crew fields

**Code Quality**
- Run tests before committing
- Follow PEP 8 style guidelines
- Use type hints where appropriate

**Dependencies**
- Keep `requirements.txt` updated
- Regularly update dependencies for security patches

### .gitignore Highlights

The `.gitignore` file excludes:
- Virtual environments (`venv/`, `demo_2_env/`)
- Data files (`data/`, `*.csv`)
- Environment files (`.env`)
- Python cache (`__pycache__/`, `*.pyc`)
- IDE settings (`.vscode/`, `.idea/`)
- API keys and credentials
- Temporary files and logs

## Example Output

### KPI Summary
```
Highest Revenue Movie: Avatar with $2,923.71 million.
Highest Profit Movie: Avatar with $2,686.71 million.
Highest ROI Movie: Avatar with 12.33x return.
```

### Analytics Report
```
Top Franchise by Movie Count:
  Avatar Collection         2
  The Avengers Collection   4

Top Director by Revenue:
  James Cameron         2931.37M USD
  Anthony and Joe Russo 3457.65M USD
```

## Contributing

1. Create a feature branch: `git checkout -b feature/your-feature`
2. Commit changes: `git commit -am 'Add feature'`
3. Push to branch: `git push origin feature/your-feature`
4. Submit a Pull Request

## Important Notes

- **API Rate Limits** — TMDB API has rate limits. Implement delays between requests for large datasets.
- **Data Freshness** — Movie data changes regularly. Re-run extraction periodically.
- **Movie IDs** — Update `movie_ids` list in `extraction.py` based on your analysis needs.


## Contact
kabuemmanuel7@icloud.com

For questions or issues, please open a GitHub issue or contact the project maintainer.

---

**Last Updated:** December 9, 2025
