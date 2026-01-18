"""
Tennis Data Scraper - Daily refresh script for GitHub Actions
Scrapes Tennis Explorer for current player rankings and recent match results
"""

import requests
from bs4 import BeautifulSoup
import sqlite3
import re
import time
import gzip
import shutil
from datetime import datetime, timedelta
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import json

class TennisDataScraper:
    """Scraper for Tennis Explorer data."""

    BASE_URL = "https://www.tennisexplorer.com"

    def __init__(self, db_path="tennis_data.db"):
        self.db_path = db_path
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
        })
        self._init_database()

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
                    break

                soup = BeautifulSoup(response.text, 'html.parser')

                # Find player rows
                rows = soup.select('table.result tbody tr')
                if not rows:
                    break

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

                        # Extract player ID from URL
                        match = re.search(r'/player/([^/]+)', href)
                        player_slug = match.group(1) if match else None

                        # Get country
                        country_img = row.select_one('td.t-name img')
                        country = country_img.get('title', '') if country_img else ''

                        # Generate a numeric ID from the slug
                        player_id = hash(player_slug) % (10**9) if player_slug else hash(name) % (10**9)
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

                        if len(players) >= max_players:
                            break

                    except Exception as e:
                        continue

                page += 1
                time.sleep(0.5)  # Rate limiting

            except Exception as e:
                print(f"Error fetching rankings page {page}: {e}")
                break

        return players

    def fetch_match_results(self, year: int, month: int, tour: str = "atp") -> list:
        """Fetch match results for a specific month."""
        if tour == "atp":
            tour_type = "atp-single"
        else:
            tour_type = "wta-single"

        url = f"{self.BASE_URL}/results/?type={tour_type}&year={year}&month={month:02d}"
        matches = []

        try:
            response = self.session.get(url, timeout=30)
            if response.status_code != 200:
                return matches

            soup = BeautifulSoup(response.text, 'html.parser')

            current_tournament = ""
            current_surface = "Hard"
            current_date = f"{year}-{month:02d}-01"

            tables = soup.select('table')

            for table in tables:
                rows = table.select('tr')
                i = 0

                while i < len(rows):
                    row = rows[i]
                    row_class = row.get('class', [])

                    # Check for tournament header
                    t_name = row.select_one('td.t-name, th.t-name')
                    player_link = row.select_one('a[href*="/player/"]')
                    if t_name and not player_link:
                        text = t_name.get_text(strip=True)
                        if not re.match(r'^\d', text):
                            current_tournament = text
                            current_surface = self._guess_surface(current_tournament)
                        i += 1
                        continue

                    # Check for date
                    cells = row.select('td')
                    if cells:
                        first_cell = cells[0].get_text(strip=True)
                        date_match = re.match(r'^(\d{1,2})\.(\d{1,2})\.$', first_cell)
                        if date_match:
                            day, month_num = date_match.groups()
                            current_date = f"{year}-{month_num.zfill(2)}-{day.zfill(2)}"

                    # Match rows come in pairs
                    if 'bott' in row_class:
                        player1_link = row.select_one('a[href*="/player/"]')
                        if player1_link and i + 1 < len(rows):
                            next_row = rows[i + 1]
                            player2_link = next_row.select_one('a[href*="/player/"]')

                            if player2_link:
                                try:
                                    player1_name = re.sub(r'\(\d+\)$', '', player1_link.get_text(strip=True)).strip()
                                    player2_name = re.sub(r'\(\d+\)$', '', player2_link.get_text(strip=True)).strip()

                                    # Skip doubles
                                    if '/' in player1_name or '/' in player2_name:
                                        i += 2
                                        continue

                                    # Get scores
                                    cells1 = row.select('td')
                                    cells2 = next_row.select('td')

                                    p1_scores = []
                                    p2_scores = []

                                    for cell in cells1:
                                        text = cell.get_text(strip=True)
                                        if re.match(r'^\d{1,2}$', text):
                                            p1_scores.append(text)

                                    for cell in cells2:
                                        text = cell.get_text(strip=True)
                                        if re.match(r'^\d{1,2}$', text):
                                            p2_scores.append(text)

                                    p1_sets = int(p1_scores[0]) if p1_scores and p1_scores[0].isdigit() else 0
                                    p2_sets = int(p2_scores[0]) if p2_scores and p2_scores[0].isdigit() else 0

                                    if p1_sets > p2_sets:
                                        winner_name, loser_name = player1_name, player2_name
                                    else:
                                        winner_name, loser_name = player2_name, player1_name

                                    # Generate IDs
                                    winner_id = hash(winner_name) % (10**9)
                                    loser_id = hash(loser_name) % (10**9)
                                    if tour == 'wta':
                                        winner_id = -abs(winner_id)
                                        loser_id = -abs(loser_id)

                                    match_id = f"TE_{current_date}_{winner_id}_{loser_id}"

                                    matches.append({
                                        'id': match_id,
                                        'date': current_date,
                                        'tournament': current_tournament,
                                        'surface': current_surface,
                                        'winner_id': winner_id,
                                        'winner_name': winner_name,
                                        'loser_id': loser_id,
                                        'loser_name': loser_name,
                                        'score': '',
                                        'tour': tour.upper()
                                    })

                                    i += 2
                                    continue
                                except Exception:
                                    pass

                    i += 1

        except Exception as e:
            print(f"Error fetching results: {e}")

        return matches

    def _guess_surface(self, tournament_name: str) -> str:
        """Guess surface from tournament name."""
        name = tournament_name.lower()

        clay_keywords = ['roland garros', 'french open', 'rome', 'madrid', 'barcelona',
                        'monte carlo', 'buenos aires', 'rio', 'hamburg', 'clay']
        grass_keywords = ['wimbledon', 'queens', 'halle', 'eastbourne', 'grass']

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
                INSERT OR REPLACE INTO players (id, name, country, ranking, tour, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (player['id'], player['name'], player['country'],
                  player['ranking'], player['tour'], datetime.now().isoformat()))

        conn.commit()
        conn.close()

    def save_matches(self, matches: list):
        """Save matches to database."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        for match in matches:
            cursor.execute("""
                INSERT OR IGNORE INTO matches
                (id, date, tournament, surface, winner_id, winner_name, loser_id, loser_name, score, tour)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (match['id'], match['date'], match['tournament'], match['surface'],
                  match['winner_id'], match['winner_name'], match['loser_id'],
                  match['loser_name'], match['score'], match['tour']))

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
            VALUES ('version', '1.0')
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

        print(f"Compressed database: {self.db_path}.gz")

    def run_full_refresh(self):
        """Run a full data refresh."""
        print(f"Starting full refresh at {datetime.now()}")

        # Fetch ALL ATP rankings (up to 3000 to get all ranked players)
        print("Fetching ATP rankings (all ranked players)...")
        atp_players = self.fetch_rankings('atp', max_players=3000)
        print(f"  Found {len(atp_players)} ATP players")
        self.save_players(atp_players)

        # Fetch ALL WTA rankings (up to 2000 to get all ranked players)
        print("Fetching WTA rankings (all ranked players)...")
        wta_players = self.fetch_rankings('wta', max_players=2000)
        print(f"  Found {len(wta_players)} WTA players")
        self.save_players(wta_players)

        # Fetch recent match results (last 12 months from today's date)
        now = datetime.now()
        print(f"Fetching last 12 months of matches (from {now.strftime('%Y-%m-%d')})...")

        for months_back in range(12):
            target = now - timedelta(days=30 * months_back)
            year, month = target.year, target.month

            print(f"Fetching ATP matches for {year}-{month:02d}...")
            atp_matches = self.fetch_match_results(year, month, 'atp')
            print(f"  Found {len(atp_matches)} ATP matches")
            self.save_matches(atp_matches)
            time.sleep(1)

            print(f"Fetching WTA matches for {year}-{month:02d}...")
            wta_matches = self.fetch_match_results(year, month, 'wta')
            print(f"  Found {len(wta_matches)} WTA matches")
            self.save_matches(wta_matches)
            time.sleep(1)

        # Compute stats
        print("Computing surface statistics...")
        self.compute_surface_stats()

        # Update metadata
        self.update_metadata()

        # Compress
        print("Compressing database...")
        self.compress_database()

        print(f"Refresh complete at {datetime.now()}")


if __name__ == "__main__":
    scraper = TennisDataScraper()
    scraper.run_full_refresh()
