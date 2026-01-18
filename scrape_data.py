"""
Tennis Data Scraper - Daily refresh script for GitHub Actions
Scrapes Tennis Explorer for current player rankings and individual player match histories
"""

import requests
from bs4 import BeautifulSoup
import sqlite3
import re
import time
import gzip
import shutil
import sys
from datetime import datetime, timedelta
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import json

def log(message):
    """Print with immediate flush for GitHub Actions visibility."""
    print(message, flush=True)

class TennisDataScraper:
    """Scraper for Tennis Explorer data."""

    BASE_URL = "https://www.tennisexplorer.com"

    def __init__(self, db_path="tennis_data.db"):
        self.db_path = db_path
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Connection': 'keep-alive',
        })
        self._init_database()
        self.player_slugs = {}  # Map player ID to slug for fetching matches

    def _init_database(self):
        """Initialize the SQLite database."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Players table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS players (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                country TEXT,
                ranking INTEGER,
                tour TEXT,
                slug TEXT,
                updated_at TEXT
            )
        """)

        # Matches table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS matches (
                id TEXT PRIMARY KEY,
                date TEXT,
                tournament TEXT,
                surface TEXT,
                round TEXT,
                winner_id INTEGER,
                winner_name TEXT,
                loser_id INTEGER,
                loser_name TEXT,
                score TEXT,
                tour TEXT
            )
        """)

        # Player surface stats
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS player_surface_stats (
                player_id INTEGER,
                surface TEXT,
                matches_played INTEGER,
                wins INTEGER,
                losses INTEGER,
                win_rate REAL,
                PRIMARY KEY (player_id, surface)
            )
        """)

        # Metadata
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS metadata (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        """)

        conn.commit()
        conn.close()

    def fetch_rankings(self, tour: str = "atp", max_players: int = 500) -> list:
        """Fetch current rankings from Tennis Explorer."""
        players = []
        page = 1

        while len(players) < max_players:
            if tour == "atp":
                url = f"{self.BASE_URL}/ranking/atp-men/?page={page}"
            else:
                url = f"{self.BASE_URL}/ranking/wta-women/?page={page}"

            try:
                response = self.session.get(url, timeout=30)
                if response.status_code != 200:
                    log(f"    Failed to fetch page {page}: status {response.status_code}")
                    break

                soup = BeautifulSoup(response.text, 'html.parser')

                # Find player rows
                rows = soup.select('table.result tbody tr')
                if not rows:
                    break

                found_on_page = 0
                for row in rows:
                    try:
                        # Get ranking
                        rank_cell = row.select_one('td.rank')
                        if not rank_cell:
                            continue
                        rank_text = rank_cell.get_text(strip=True).rstrip('.')
                        if not rank_text.isdigit():
                            continue
                        ranking = int(rank_text)

                        # Get player name and link
                        name_cell = row.select_one('td.t-name a')
                        if not name_cell:
                            continue

                        name = name_cell.get_text(strip=True)
                        href = name_cell.get('href', '')

                        # Extract player slug from URL
                        match = re.search(r'/player/([^/]+)', href)
                        player_slug = match.group(1) if match else None

                        if not player_slug:
                            continue

                        # Get country
                        country_img = row.select_one('td.t-name img')
                        country = country_img.get('title', '') if country_img else ''

                        # Generate a numeric ID from the slug
                        player_id = hash(player_slug) % (10**9)
                        if tour == 'wta':
                            player_id = -abs(player_id)  # Negative for WTA

                        players.append({
                            'id': player_id,
                            'name': name,
                            'country': country,
                            'ranking': ranking,
                            'tour': tour.upper(),
                            'slug': player_slug
                        })

                        # Store slug for later match fetching
                        self.player_slugs[player_id] = player_slug
                        found_on_page += 1

                        if len(players) >= max_players:
                            break

                    except Exception as e:
                        continue

                if found_on_page == 0:
                    break

                page += 1
                time.sleep(0.3)  # Rate limiting

            except Exception as e:
                log(f"Error fetching rankings page {page}: {e}")
                break

            # Progress update every 10 pages
            if page % 10 == 0:
                log(f"    ... fetched {len(players)} players so far (page {page})")

        return players

    def fetch_player_matches(self, player_id: int, player_slug: str, tour: str,
                            max_matches: int = 50, cutoff_date: str = None) -> list:
        """Fetch match history for a specific player."""
        matches = []

        # Tennis Explorer player matches URL
        url = f"{self.BASE_URL}/player/{player_slug}/?annual=all"

        try:
            response = self.session.get(url, timeout=30)
            if response.status_code != 200:
                return matches

            soup = BeautifulSoup(response.text, 'html.parser')

            # Find match tables
            tables = soup.select('table.result')

            current_tournament = ""
            current_surface = "Hard"
            current_year = datetime.now().year

            for table in tables:
                rows = table.select('tr')

                for row in rows:
                    try:
                        # Check for tournament header
                        header = row.select_one('td.t-name a, th.t-name a')
                        if header and not row.select_one('td.result, td.score'):
                            tournament_text = header.get_text(strip=True)
                            # Extract tournament name and surface
                            current_tournament = tournament_text
                            current_surface = self._guess_surface(tournament_text)

                            # Try to get year from tournament
                            year_match = re.search(r'20\d{2}', tournament_text)
                            if year_match:
                                current_year = int(year_match.group())
                            continue

                        # Check for date in first cell
                        cells = row.select('td')
                        if not cells:
                            continue

                        first_cell = cells[0].get_text(strip=True)
                        date_match = re.match(r'^(\d{1,2})\.(\d{1,2})\.$', first_cell)
                        if date_match:
                            day, month = date_match.groups()
                            match_date = f"{current_year}-{month.zfill(2)}-{day.zfill(2)}"

                            # Skip if before cutoff
                            if cutoff_date and match_date < cutoff_date:
                                continue
                        else:
                            continue

                        # Get opponent
                        opponent_link = row.select_one('td.t-name a[href*="/player/"]')
                        if not opponent_link:
                            continue

                        opponent_name = opponent_link.get_text(strip=True)
                        opponent_href = opponent_link.get('href', '')

                        # Skip doubles
                        if '/' in opponent_name:
                            continue

                        # Get opponent slug and ID
                        opp_match = re.search(r'/player/([^/]+)', opponent_href)
                        opponent_slug = opp_match.group(1) if opp_match else None
                        opponent_id = hash(opponent_slug) % (10**9) if opponent_slug else hash(opponent_name) % (10**9)
                        if tour.upper() == 'WTA':
                            opponent_id = -abs(opponent_id)

                        # Get result (win/loss)
                        result_cell = row.select_one('td.result, td.score')
                        if not result_cell:
                            continue

                        result_text = result_cell.get_text(strip=True).lower()

                        # Determine winner/loser
                        # Look for win indicator (often bold, or specific text)
                        is_win = False

                        # Check for score cells - winner usually has higher set count
                        score_cells = row.select('td.score, td[class*="s"], td.result')
                        score_text = ""
                        for sc in score_cells:
                            txt = sc.get_text(strip=True)
                            if re.match(r'^\d', txt):
                                score_text += txt + " "

                        # Check if player won by looking at class or content
                        winner_class = row.get('class', [])
                        if 'win' in str(winner_class).lower():
                            is_win = True
                        elif row.select_one('td.win, .winner, b'):
                            is_win = True
                        else:
                            # Try to infer from score - first number is usually player's sets
                            sets = re.findall(r'(\d+)', score_text[:10])
                            if len(sets) >= 2:
                                is_win = int(sets[0]) > int(sets[1])

                        # Get player name from the page
                        player_name_elem = soup.select_one('h3.plDetail a, h1.player-name, .player-info h1')
                        player_name = player_name_elem.get_text(strip=True) if player_name_elem else f"Player_{player_id}"

                        if is_win:
                            winner_id, winner_name = player_id, player_name
                            loser_id, loser_name = opponent_id, opponent_name
                        else:
                            winner_id, winner_name = opponent_id, opponent_name
                            loser_id, loser_name = player_id, player_name

                        # Get round
                        round_text = ""
                        round_cell = row.select_one('td.round')
                        if round_cell:
                            round_text = round_cell.get_text(strip=True)

                        match_id = f"TE_{match_date}_{winner_id}_{loser_id}"

                        matches.append({
                            'id': match_id,
                            'date': match_date,
                            'tournament': current_tournament,
                            'surface': current_surface,
                            'round': round_text,
                            'winner_id': winner_id,
                            'winner_name': winner_name,
                            'loser_id': loser_id,
                            'loser_name': loser_name,
                            'score': score_text.strip(),
                            'tour': tour.upper()
                        })

                        if len(matches) >= max_matches:
                            return matches

                    except Exception as e:
                        continue

        except Exception as e:
            log(f"Error fetching matches for {player_slug}: {e}")

        return matches

    def _guess_surface(self, tournament_name: str) -> str:
        """Guess surface from tournament name."""
        name = tournament_name.lower()

        clay_keywords = ['roland garros', 'french open', 'rome', 'madrid', 'barcelona',
                        'monte carlo', 'buenos aires', 'rio', 'hamburg', 'clay',
                        'estoril', 'geneva', 'lyon', 'kitzbuhel', 'gstaad', 'bastad',
                        'umag', 'marrakech', 'houston', 'bucharest', 'palermo']
        grass_keywords = ['wimbledon', 'queens', "queen's", 'halle', 'eastbourne',
                         'grass', 's-hertogenbosch', 'stuttgart', 'mallorca', 'newport',
                         'berlin']

        for keyword in clay_keywords:
            if keyword in name:
                return 'Clay'
        for keyword in grass_keywords:
            if keyword in name:
                return 'Grass'

        return 'Hard'

    def save_players(self, players: list):
        """Save players to database."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        for player in players:
            cursor.execute("""
                INSERT OR REPLACE INTO players (id, name, country, ranking, tour, slug, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (player['id'], player['name'], player['country'],
                  player['ranking'], player['tour'], player.get('slug', ''),
                  datetime.now().isoformat()))

        conn.commit()
        conn.close()

    def save_matches(self, matches: list):
        """Save matches to database."""
        if not matches:
            return

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        for match in matches:
            cursor.execute("""
                INSERT OR IGNORE INTO matches
                (id, date, tournament, surface, round, winner_id, winner_name, loser_id, loser_name, score, tour)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (match['id'], match['date'], match['tournament'], match['surface'],
                  match.get('round', ''), match['winner_id'], match['winner_name'],
                  match['loser_id'], match['loser_name'], match.get('score', ''), match['tour']))

        conn.commit()
        conn.close()

    def compute_surface_stats(self):
        """Compute surface statistics for all players."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute("""
            INSERT OR REPLACE INTO player_surface_stats
            (player_id, surface, matches_played, wins, losses, win_rate)
            SELECT
                player_id,
                surface,
                COUNT(*) as matches_played,
                SUM(won) as wins,
                SUM(1 - won) as losses,
                CAST(SUM(won) AS REAL) / COUNT(*) as win_rate
            FROM (
                SELECT winner_id as player_id, surface, 1 as won FROM matches
                UNION ALL
                SELECT loser_id as player_id, surface, 0 as won FROM matches
            )
            WHERE surface IS NOT NULL
            GROUP BY player_id, surface
        """)

        conn.commit()
        conn.close()

    def update_metadata(self):
        """Update metadata with last refresh time."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute("""
            INSERT OR REPLACE INTO metadata (key, value)
            VALUES ('last_updated', ?)
        """, (datetime.now().isoformat(),))

        cursor.execute("""
            INSERT OR REPLACE INTO metadata (key, value)
            VALUES ('version', '2.0')
        """)

        conn.commit()
        conn.close()

    def compress_database(self):
        """Compress the database file."""
        # Vacuum the database first
        conn = sqlite3.connect(self.db_path)
        conn.execute("VACUUM")
        conn.close()

        # Compress with gzip
        with open(self.db_path, 'rb') as f_in:
            with gzip.open(f"{self.db_path}.gz", 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)

        log(f"Compressed database: {self.db_path}.gz")

    def run_full_refresh(self):
        """Run a full data refresh."""
        log(f"Starting full refresh at {datetime.now()}")

        # Calculate cutoff date (12 months ago)
        cutoff_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
        log(f"Fetching matches since {cutoff_date}")

        # Fetch ATP rankings (top 500 for reasonable scraping time)
        log("Fetching ATP rankings...")
        atp_players = self.fetch_rankings('atp', max_players=500)
        log(f"  Found {len(atp_players)} ATP players")
        self.save_players(atp_players)

        # Fetch WTA rankings (top 500)
        log("Fetching WTA rankings...")
        wta_players = self.fetch_rankings('wta', max_players=500)
        log(f"  Found {len(wta_players)} WTA players")
        self.save_players(wta_players)

        all_players = atp_players + wta_players
        total_matches = 0

        # Fetch matches for each player
        log(f"\nFetching match history for {len(all_players)} players...")

        for i, player in enumerate(all_players, 1):
            player_id = player['id']
            player_slug = player.get('slug')
            player_name = player['name']
            tour = player['tour']

            if not player_slug:
                continue

            # Progress update
            if i % 50 == 0 or i == len(all_players):
                log(f"  Processing player {i}/{len(all_players)} ({player_name})... Total matches: {total_matches}")

            # Fetch player's matches
            matches = self.fetch_player_matches(
                player_id, player_slug, tour,
                max_matches=30,  # Last 30 matches per player
                cutoff_date=cutoff_date
            )

            if matches:
                self.save_matches(matches)
                total_matches += len(matches)

            # Rate limiting - be respectful to the server
            time.sleep(0.5)

        log(f"\nTotal matches collected: {total_matches}")

        # Compute stats
        log("Computing surface statistics...")
        self.compute_surface_stats()

        # Update metadata
        self.update_metadata()

        # Compress
        log("Compressing database...")
        self.compress_database()

        # Final stats
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM players")
        player_count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM matches")
        match_count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(DISTINCT winner_id) + COUNT(DISTINCT loser_id) FROM matches")
        players_with_matches = cursor.fetchone()[0]
        conn.close()

        log(f"\nRefresh complete at {datetime.now()}")
        log(f"Final stats: {player_count} players, {match_count} matches")
        log(f"Players with match data: ~{players_with_matches}")


if __name__ == "__main__":
    scraper = TennisDataScraper()
    scraper.run_full_refresh()
