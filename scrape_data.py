"""
Tennis Data Scraper - Daily refresh script for GitHub Actions
Scrapes Tennis Explorer for player match histories with priority scraping and caching
"""

import requests
from bs4 import BeautifulSoup
import sqlite3
import re
import time
import gzip
import shutil
import random
import threading
from datetime import datetime, timedelta
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import json

def log(message):
    """Print with immediate flush for GitHub Actions visibility."""
    print(message, flush=True)


class TennisDataScraper:
    """Scraper for Tennis Explorer data with parallel scraping and caching."""

    BASE_URL = "https://www.tennisexplorer.com"

    USER_AGENTS = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0',
    ]

    CACHE_TTL_DAYS = 7  # Skip non-priority players scraped within this many days

    def __init__(self, db_path="tennis_data.db"):
        self.db_path = db_path
        self.cache_path = Path(__file__).parent / "scrape_cache.json"
        self.player_cache = {}  # In-memory slug cache
        self.scrape_cache = self._load_scrape_cache()  # Persistent scrape timestamps
        self.db_lock = threading.Lock()
        self.stats_lock = threading.Lock()
        self.stats = {'found': 0, 'matches': 0, 'skipped': 0}
        self._init_database()

    def _create_session(self):
        """Create a new session with random user agent."""
        session = requests.Session()
        ua = random.choice(self.USER_AGENTS)
        session.headers.update({
            'User-Agent': ua,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Connection': 'keep-alive',
            'Cache-Control': 'no-cache',
        })
        return session

    def _load_scrape_cache(self) -> dict:
        """Load the scrape cache from disk."""
        if self.cache_path.exists():
            try:
                with open(self.cache_path, 'r') as f:
                    return json.load(f)
            except Exception:
                return {}
        return {}

    def _save_scrape_cache(self):
        """Save the scrape cache to disk."""
        try:
            with open(self.cache_path, 'w') as f:
                json.dump(self.scrape_cache, f)
        except Exception as e:
            log(f"Warning: Could not save scrape cache: {e}")

    def _should_scrape_player(self, player_name: str, is_priority: bool) -> bool:
        """Check if we should scrape this player based on cache."""
        if is_priority:
            return True  # Always scrape priority players

        cache_key = player_name.lower()
        if cache_key in self.scrape_cache:
            last_scraped = datetime.fromisoformat(self.scrape_cache[cache_key])
            age_days = (datetime.now() - last_scraped).days
            if age_days < self.CACHE_TTL_DAYS:
                return False  # Skip - recently scraped
        return True

    def _mark_player_scraped(self, player_name: str):
        """Mark a player as scraped in the cache."""
        self.scrape_cache[player_name.lower()] = datetime.now().isoformat()

    def _request(self, session, url, max_retries=3):
        """Make a request with retries and exponential backoff."""
        for attempt in range(max_retries):
            try:
                # Random delay between requests
                time.sleep(random.uniform(2.0, 4.0))

                response = session.get(url, timeout=30)
                if response.status_code == 200:
                    return response
                elif response.status_code == 429:  # Rate limited
                    wait_time = (attempt + 1) * 60
                    log(f"    Rate limited, waiting {wait_time}s...")
                    time.sleep(wait_time)
                elif response.status_code == 404:
                    return None
                else:
                    log(f"    HTTP {response.status_code}")
                    time.sleep(10)
            except Exception as e:
                if attempt < max_retries - 1:
                    wait_time = (attempt + 1) * 15
                    log(f"    Connection error, retrying in {wait_time}s...")
                    time.sleep(wait_time)
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

    def search_player(self, session, player_name: str) -> dict:
        """Search for a player on Tennis Explorer and return their info."""
        cache_key = player_name.lower()
        if cache_key in self.player_cache:
            return self.player_cache[cache_key]

        search_name = player_name.replace('-', ' ').replace("'", "")
        name_variants = [
            search_name,
            ' '.join(search_name.split()[::-1]),
        ]

        for name in name_variants:
            search_query = name.replace(' ', '+')
            url = f"{self.BASE_URL}/search/?search={search_query}"

            response = self._request(session, url)
            if not response:
                continue

            soup = BeautifulSoup(response.text, 'html.parser')
            player_links = soup.select('a[href*="/player/"]')

            for link in player_links:
                href = link.get('href', '')
                link_text = link.get_text(strip=True).lower()

                search_parts = search_name.lower().split()
                if all(part in link_text or part in href.lower() for part in search_parts[-1:]):
                    match = re.search(r'/player/([^/]+)', href)
                    if match:
                        slug = match.group(1)
                        tour = 'WTA' if any(x in href.lower() for x in ['wta', 'women']) else 'ATP'
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

        self.player_cache[cache_key] = None
        return None

    def fetch_player_matches(self, session, player_id: int, player_slug: str, player_name: str,
                            tour: str, max_matches: int = 30, cutoff_date: str = None) -> list:
        """Fetch match history for a specific player."""
        matches = []
        url = f"{self.BASE_URL}/player/{player_slug}/?annual=all"

        response = self._request(session, url)
        if not response:
            return matches

        soup = BeautifulSoup(response.text, 'html.parser')
        name_elem = soup.select_one('h3.plDetail a, h1')
        page_player_name = name_elem.get_text(strip=True) if name_elem else player_name

        tables = soup.select('table.result')
        current_tournament = ""
        current_surface = "Hard"
        current_year = datetime.now().year

        for table in tables:
            rows = table.select('tr')

            for row in rows:
                try:
                    header = row.select_one('td.t-name a, th.t-name a')
                    if header and not row.select('td.score, td.result'):
                        tournament_text = header.get_text(strip=True)
                        current_tournament = tournament_text
                        current_surface = self._guess_surface(tournament_text)
                        year_match = re.search(r'20\d{2}', tournament_text)
                        if year_match:
                            current_year = int(year_match.group())
                        continue

                    cells = row.select('td')
                    if len(cells) < 3:
                        continue

                    first_cell = cells[0].get_text(strip=True)
                    date_match = re.match(r'^(\d{1,2})\.(\d{1,2})\.$', first_cell)
                    if not date_match:
                        continue

                    day, month = date_match.groups()
                    match_date = f"{current_year}-{month.zfill(2)}-{day.zfill(2)}"

                    if cutoff_date and match_date < cutoff_date:
                        continue

                    opponent_link = row.select_one('td.t-name a[href*="/player/"]')
                    if not opponent_link:
                        continue

                    opponent_name = opponent_link.get_text(strip=True)
                    opponent_href = opponent_link.get('href', '')

                    if '/' in opponent_name:
                        continue

                    opp_match = re.search(r'/player/([^/]+)', opponent_href)
                    opponent_slug = opp_match.group(1) if opp_match else None
                    opponent_id = hash(opponent_slug) % (10**9) if opponent_slug else hash(opponent_name) % (10**9)
                    if tour == 'WTA':
                        opponent_id = -abs(opponent_id)

                    score_text = ""
                    for cell in cells:
                        cell_text = cell.get_text(strip=True)
                        if re.match(r'^\d{1,2}$', cell_text):
                            score_text += cell_text + " "

                    is_win = False
                    sets = re.findall(r'(\d+)', score_text[:20])
                    if len(sets) >= 2:
                        is_win = int(sets[0]) > int(sets[1])

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
        """Save a single player to database (thread-safe)."""
        with self.db_lock:
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
        """Save matches to database (thread-safe)."""
        if not matches:
            return

        with self.db_lock:
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
        cursor.execute("INSERT OR REPLACE INTO metadata (key, value) VALUES ('version', '4.0')")

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
        return []

    def load_priority_players(self) -> set:
        """Load priority players (those with upcoming Betfair matches)."""
        priority_file = Path(__file__).parent / "priority_players.json"

        if priority_file.exists():
            try:
                with open(priority_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    return set(data.get('players', []))
            except Exception:
                return set()
        return set()

    def _scrape_single_player(self, player_name: str, is_priority: bool, cutoff_date: str) -> dict:
        """Scrape a single player (runs in thread)."""
        result = {'name': player_name, 'found': False, 'matches': 0, 'skipped': False}

        # Check cache
        if not self._should_scrape_player(player_name, is_priority):
            result['skipped'] = True
            return result

        # Create session for this thread
        session = self._create_session()

        # Search for player
        player_info = self.search_player(session, player_name)

        if not player_info:
            return result

        result['found'] = True

        # Save player info
        self.save_player(player_info)

        # Fetch matches
        matches = self.fetch_player_matches(
            session,
            player_info['id'],
            player_info['slug'],
            player_info['name'],
            player_info['tour'],
            max_matches=30,
            cutoff_date=cutoff_date
        )

        if matches:
            self.save_matches(matches)
            result['matches'] = len(matches)

        # Mark as scraped
        self._mark_player_scraped(player_name)

        return result

    def run_full_refresh(self, max_workers: int = 3):
        """Run a full data refresh with parallel scraping."""
        log(f"Starting full refresh at {datetime.now()}")
        log(f"Using {max_workers} parallel workers")

        # Calculate cutoff date (12 months ago)
        cutoff_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
        log(f"Fetching matches since {cutoff_date}")

        # Load player lists
        all_players = self.load_player_list()
        priority_players = self.load_priority_players()

        log(f"Loaded {len(all_players)} total players")
        log(f"Loaded {len(priority_players)} priority players (Betfair upcoming)")

        if not all_players and not priority_players:
            log("No players to scrape!")
            return

        # Combine and deduplicate - priority first
        player_queue = []
        seen = set()

        # Add priority players first
        for p in priority_players:
            if p.lower() not in seen:
                player_queue.append((p, True))
                seen.add(p.lower())

        # Add remaining players
        for p in all_players:
            if p.lower() not in seen:
                player_queue.append((p, False))
                seen.add(p.lower())

        log(f"Total unique players to process: {len(player_queue)}")

        # Scrape with thread pool
        players_found = 0
        players_skipped = 0
        total_matches = 0
        players_not_found = []

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            futures = {
                executor.submit(self._scrape_single_player, name, is_priority, cutoff_date): (name, is_priority)
                for name, is_priority in player_queue
            }

            # Process results as they complete
            completed = 0
            for future in as_completed(futures):
                completed += 1
                player_name, is_priority = futures[future]

                try:
                    result = future.result()

                    if result['skipped']:
                        players_skipped += 1
                    elif result['found']:
                        players_found += 1
                        total_matches += result['matches']
                    else:
                        players_not_found.append(player_name)

                    # Progress update every 25 players
                    if completed % 25 == 0 or completed == len(futures):
                        priority_tag = " [P]" if is_priority else ""
                        log(f"  Progress: {completed}/{len(futures)} | Found: {players_found} | "
                            f"Skipped: {players_skipped} | Matches: {total_matches}{priority_tag}")

                except Exception as e:
                    log(f"    Error processing {player_name}: {e}")
                    players_not_found.append(player_name)

        # Save scrape cache
        self._save_scrape_cache()

        log(f"\nScraping complete:")
        log(f"  Players found: {players_found}")
        log(f"  Players skipped (cached): {players_skipped}")
        log(f"  Players not found: {len(players_not_found)}")
        log(f"  Total matches collected: {total_matches}")

        if players_not_found and len(players_not_found) <= 30:
            log(f"\nPlayers not found:")
            for p in players_not_found:
                log(f"  - {p}")

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
    scraper.run_full_refresh(max_workers=3)
