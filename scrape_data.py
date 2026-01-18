"""
Tennis Data Scraper - Daily refresh script for GitHub Actions
Scrapes Tennis Explorer for player match histories based on a player list
"""

import requests
from bs4 import BeautifulSoup
import sqlite3
import re
import time
import gzip
import shutil
import sys
import random
from datetime import datetime, timedelta
from pathlib import Path
import json

def log(message):
    """Print with immediate flush for GitHub Actions visibility."""
    print(message, flush=True)


class TennisDataScraper:
    """Scraper for Tennis Explorer data."""

    BASE_URL = "https://www.tennisexplorer.com"

    USER_AGENTS = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0',
    ]

    def __init__(self, db_path="tennis_data.db"):
        self.db_path = db_path
        self.session = requests.Session()
        self._rotate_user_agent()
        self._init_database()
        self.player_cache = {}  # Cache player slug lookups

    def _rotate_user_agent(self):
        """Rotate user agent to avoid detection."""
        ua = random.choice(self.USER_AGENTS)
        self.session.headers.update({
            'User-Agent': ua,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Connection': 'keep-alive',
            'Cache-Control': 'no-cache',
        })

    def _request(self, url, max_retries=3):
        """Make a request with retries and exponential backoff."""
        for attempt in range(max_retries):
            try:
                # Random delay to appear more human-like
                time.sleep(random.uniform(1.5, 3.0))

                response = self.session.get(url, timeout=30)
                if response.status_code == 200:
                    return response
                elif response.status_code == 429:  # Rate limited
                    wait_time = (attempt + 1) * 60
                    log(f"    Rate limited, waiting {wait_time}s...")
                    time.sleep(wait_time)
                    self._rotate_user_agent()
                elif response.status_code == 404:
                    return None  # Player not found
                else:
                    log(f"    HTTP {response.status_code}")
                    time.sleep(10)
            except Exception as e:
                if attempt < max_retries - 1:
                    wait_time = (attempt + 1) * 15
                    log(f"    Connection error, retrying in {wait_time}s...")
                    time.sleep(wait_time)
                    self._rotate_user_agent()
                else:
                    log(f"    Failed after {max_retries} attempts: {e}")
                    return None
        return None

    def _init_database(self):
        """Initialize the SQLite database."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

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

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS metadata (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        """)

        conn.commit()
        conn.close()

    def search_player(self, player_name: str) -> dict:
        """Search for a player on Tennis Explorer and return their info."""
        # Check cache first
        cache_key = player_name.lower()
        if cache_key in self.player_cache:
            return self.player_cache[cache_key]

        # Normalize name for search
        search_name = player_name.replace('-', ' ').replace("'", "")

        # Try different name formats
        name_variants = [
            search_name,
            ' '.join(search_name.split()[::-1]),  # Reverse first/last name
        ]

        for name in name_variants:
            # Search URL
            search_query = name.replace(' ', '+')
            url = f"{self.BASE_URL}/search/?search={search_query}"

            response = self._request(url)
            if not response:
                continue

            soup = BeautifulSoup(response.text, 'html.parser')

            # Look for player links in search results
            player_links = soup.select('a[href*="/player/"]')

            for link in player_links:
                href = link.get('href', '')
                link_text = link.get_text(strip=True).lower()

                # Check if this looks like our player
                search_parts = search_name.lower().split()
                if all(part in link_text or part in href.lower() for part in search_parts[-1:]):  # Match last name at minimum
                    # Extract slug
                    match = re.search(r'/player/([^/]+)', href)
                    if match:
                        slug = match.group(1)

                        # Determine tour (WTA if contains female indicators or negative context)
                        tour = 'WTA' if any(x in href.lower() for x in ['wta', 'women']) else 'ATP'

                        # Generate consistent ID
                        player_id = hash(slug) % (10**9)
                        if tour == 'WTA':
                            player_id = -abs(player_id)

                        result = {
                            'id': player_id,
                            'name': player_name,
                            'slug': slug,
                            'tour': tour,
                            'country': '',
                            'ranking': None
                        }

                        self.player_cache[cache_key] = result
                        return result

        # Not found
        self.player_cache[cache_key] = None
        return None

    def fetch_player_matches(self, player_id: int, player_slug: str, player_name: str,
                            tour: str, max_matches: int = 30, cutoff_date: str = None) -> list:
        """Fetch match history for a specific player."""
        matches = []

        url = f"{self.BASE_URL}/player/{player_slug}/?annual=all"

        response = self._request(url)
        if not response:
            return matches

        soup = BeautifulSoup(response.text, 'html.parser')

        # Get player's full name from page
        name_elem = soup.select_one('h3.plDetail a, h1')
        page_player_name = name_elem.get_text(strip=True) if name_elem else player_name

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
                    if header and not row.select('td.score, td.result'):
                        tournament_text = header.get_text(strip=True)
                        current_tournament = tournament_text
                        current_surface = self._guess_surface(tournament_text)

                        # Extract year
                        year_match = re.search(r'20\d{2}', tournament_text)
                        if year_match:
                            current_year = int(year_match.group())
                        continue

                    # Check for date
                    cells = row.select('td')
                    if len(cells) < 3:
                        continue

                    first_cell = cells[0].get_text(strip=True)
                    date_match = re.match(r'^(\d{1,2})\.(\d{1,2})\.$', first_cell)
                    if not date_match:
                        continue

                    day, month = date_match.groups()
                    match_date = f"{current_year}-{month.zfill(2)}-{day.zfill(2)}"

                    # Skip if before cutoff
                    if cutoff_date and match_date < cutoff_date:
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

                    # Get opponent ID
                    opp_match = re.search(r'/player/([^/]+)', opponent_href)
                    opponent_slug = opp_match.group(1) if opp_match else None
                    opponent_id = hash(opponent_slug) % (10**9) if opponent_slug else hash(opponent_name) % (10**9)
                    if tour == 'WTA':
                        opponent_id = -abs(opponent_id)

                    # Determine win/loss from score
                    score_text = ""
                    for cell in cells:
                        cell_text = cell.get_text(strip=True)
                        if re.match(r'^\d{1,2}$', cell_text):
                            score_text += cell_text + " "

                    # Check if player won (first set count > second set count typically)
                    is_win = False
                    sets = re.findall(r'(\d+)', score_text[:20])
                    if len(sets) >= 2:
                        is_win = int(sets[0]) > int(sets[1])

                    # Also check for win/loss indicators in row class or cells
                    row_classes = ' '.join(row.get('class', []))
                    if 'win' in row_classes.lower():
                        is_win = True
                    elif 'lose' in row_classes.lower() or 'lost' in row_classes.lower():
                        is_win = False

                    if is_win:
                        winner_id, winner_name = player_id, page_player_name
                        loser_id, loser_name = opponent_id, opponent_name
                    else:
                        winner_id, winner_name = opponent_id, opponent_name
                        loser_id, loser_name = player_id, page_player_name

                    # Get round
                    round_text = ""
                    round_cell = row.select_one('td.round')
                    if round_cell:
                        round_text = round_cell.get_text(strip=True)

                    match_id = f"TE_{match_date}_{abs(winner_id)}_{abs(loser_id)}"

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
                        'tour': tour
                    })

                    if len(matches) >= max_matches:
                        return matches

                except Exception:
                    continue

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

    def save_player(self, player: dict):
        """Save a single player to database."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute("""
            INSERT OR REPLACE INTO players (id, name, country, ranking, tour, slug, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (player['id'], player['name'], player.get('country', ''),
              player.get('ranking'), player['tour'], player.get('slug', ''),
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

        cursor.execute("INSERT OR REPLACE INTO metadata (key, value) VALUES ('last_updated', ?)",
                      (datetime.now().isoformat(),))
        cursor.execute("INSERT OR REPLACE INTO metadata (key, value) VALUES ('version', '3.0')")

        conn.commit()
        conn.close()

    def compress_database(self):
        """Compress the database file."""
        conn = sqlite3.connect(self.db_path)
        conn.execute("VACUUM")
        conn.close()

        with open(self.db_path, 'rb') as f_in:
            with gzip.open(f"{self.db_path}.gz", 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)

        log(f"Compressed database: {self.db_path}.gz")

    def load_player_list(self) -> list:
        """Load player list from JSON file."""
        player_file = Path(__file__).parent / "players_to_scrape.json"

        if player_file.exists():
            with open(player_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return data.get('players', [])
        else:
            log("Warning: players_to_scrape.json not found, using default top players")
            return []

    def run_full_refresh(self):
        """Run a full data refresh."""
        log(f"Starting full refresh at {datetime.now()}")

        # Calculate cutoff date (12 months ago)
        cutoff_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
        log(f"Fetching matches since {cutoff_date}")

        # Load player list
        player_names = self.load_player_list()
        log(f"Loaded {len(player_names)} players to scrape")

        if not player_names:
            log("No players to scrape!")
            return

        total_matches = 0
        players_found = 0
        players_not_found = []

        for i, player_name in enumerate(player_names, 1):
            # Progress update
            if i % 25 == 0 or i == len(player_names):
                log(f"  Processing {i}/{len(player_names)}: {player_name} | Found: {players_found} | Matches: {total_matches}")
                self._rotate_user_agent()

            # Search for player
            player_info = self.search_player(player_name)

            if not player_info:
                players_not_found.append(player_name)
                continue

            players_found += 1

            # Save player info
            self.save_player(player_info)

            # Fetch matches
            matches = self.fetch_player_matches(
                player_info['id'],
                player_info['slug'],
                player_info['name'],
                player_info['tour'],
                max_matches=30,
                cutoff_date=cutoff_date
            )

            if matches:
                self.save_matches(matches)
                total_matches += len(matches)

        log(f"\nTotal players found: {players_found}/{len(player_names)}")
        log(f"Total matches collected: {total_matches}")

        if players_not_found:
            log(f"\nPlayers not found ({len(players_not_found)}):")
            for p in players_not_found[:20]:
                log(f"  - {p}")
            if len(players_not_found) > 20:
                log(f"  ... and {len(players_not_found) - 20} more")

        # Compute stats
        log("\nComputing surface statistics...")
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
        conn.close()

        log(f"\nRefresh complete at {datetime.now()}")
        log(f"Final stats: {player_count} players, {match_count} matches")


if __name__ == "__main__":
    scraper = TennisDataScraper()
    scraper.run_full_refresh()
