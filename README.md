# Tennis Data

Daily updated tennis player rankings and match data for ATP and WTA tours.

## Data Contents

- **Players**: Top 500 ATP and WTA players with current rankings
- **Matches**: Last 6 months of match results
- **Surface Stats**: Win rates by surface for all players

## Files

- `tennis_data.db.gz` - Compressed SQLite database (download this)
- `scrape_data.py` - Scraper script
- `.github/workflows/daily-refresh.yml` - Automated daily refresh

## Usage

Download the compressed database:
```
https://github.com/Anners92/tennisdata/raw/main/tennis_data.db.gz
```

## Update Schedule

Data is refreshed automatically at 6:00 AM UTC daily via GitHub Actions.

## Database Schema

### players
| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER | Player ID |
| name | TEXT | Full name |
| country | TEXT | Country code |
| ranking | INTEGER | Current ranking |
| tour | TEXT | ATP or WTA |

### matches
| Column | Type | Description |
|--------|------|-------------|
| id | TEXT | Match ID |
| date | TEXT | Match date |
| tournament | TEXT | Tournament name |
| surface | TEXT | Hard/Clay/Grass |
| winner_id | INTEGER | Winner player ID |
| loser_id | INTEGER | Loser player ID |

### player_surface_stats
| Column | Type | Description |
|--------|------|-------------|
| player_id | INTEGER | Player ID |
| surface | TEXT | Surface type |
| wins | INTEGER | Total wins |
| losses | INTEGER | Total losses |
| win_rate | REAL | Win percentage |
