"""
Tennis Data Scraper - Daily refresh script for GitHub Actions
Scrapes Tennis Explorer for player match histories with priority scraping and caching

Strategy:
1. First fetch ranking pages to build player name -> slug lookup
2. Then fetch match history for each player using direct URLs
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


class PlayerNameMatcher:
    """
    Robust player name matching system that handles various name formats:
    - "LastName F." (e.g., "Grubor A.")
    - "F. LastName" (e.g., "A. Grubor")
    - "FirstName LastName" (e.g., "Ana Grubor")
    - "LastName FirstName" (e.g., "Grubor Ana")
    - Compound names (e.g., "Del Potro J.", "Juan Martin Del Potro")
    """

    def __init__(self):
        self.players = {}  # id -> canonical name
        self.by_full_name = {}  # normalized full name -> id
        self.by_last_name = {}  # last name -> [(id, full_name, first_initial)]
        self.by_name_parts = {}  # each name part -> [(id, full_name)]
        self.by_last_initial = {}  # "lastname_x" -> [(id, full_name)] where x is first initial

    def _normalize(self, name: str) -> str:
        """Normalize a name for comparison."""
        if not name:
            return ""
        name = name.lower().strip()
        name = name.replace('.', '')
        name = ' '.join(name.split())
        return name

    def _extract_components(self, name: str) -> dict:
        """Extract name components from various formats."""
        result = {
            'last_name': '',
            'first_name': '',
            'first_initial': '',
            'all_parts': []
        }

        if not name:
            return result

        normalized = self._normalize(name)
        parts = normalized.split()
        result['all_parts'] = parts

        if not parts:
            return result

        if len(parts) == 1:
            result['last_name'] = parts[0]
            return result

        first_is_initial = len(parts[0]) == 1
        last_is_initial = len(parts[-1]) == 1

        if last_is_initial:
            result['first_initial'] = parts[-1]
            result['last_name'] = parts[0]
            if len(parts) > 2:
                result['first_name'] = ' '.join(parts[1:-1])
        elif first_is_initial:
            result['first_initial'] = parts[0]
            result['last_name'] = parts[-1]
            if len(parts) > 2:
                result['first_name'] = ' '.join(parts[1:-1])
        else:
            result['first_name'] = parts[0]
            result['last_name'] = parts[-1]
            result['first_initial'] = parts[0][0] if parts[0] else ''

        return result

    def add_player(self, player_id: int, full_name: str):
        """Add a player to all indexes."""
        if not full_name:
            return

        self.players[player_id] = full_name
        normalized = self._normalize(full_name)
        components = self._extract_components(full_name)

        # Index by full normalized name
        self.by_full_name[normalized] = player_id
        self.by_full_name[normalized.replace(' ', '')] = player_id

        # Index by each name part (for compound name matching)
        for part in components['all_parts']:
            if len(part) > 1:
                if part not in self.by_name_parts:
                    self.by_name_parts[part] = []
                self.by_name_parts[part].append((player_id, full_name))

        # Index by last name
        last_name = components['last_name']
        if last_name and len(last_name) > 1:
            if last_name not in self.by_last_name:
                self.by_last_name[last_name] = []
            first_initial = components['first_initial'] or (components['first_name'][0] if components['first_name'] else '')
            self.by_last_name[last_name].append((player_id, full_name, first_initial))

            if first_initial:
                key = f"{last_name}_{first_initial}"
                if key not in self.by_last_initial:
                    self.by_last_initial[key] = []
                self.by_last_initial[key].append((player_id, full_name))

        # Index all parts as potential last names
        for part in components['all_parts']:
            if len(part) > 1:
                if part not in self.by_last_name:
                    self.by_last_name[part] = []
                initial = ''
                for p in components['all_parts']:
                    if len(p) == 1:
                        initial = p
                        break
                    elif p != part and len(p) > 1:
                        initial = p[0]
                        break
                existing = [(pid, fn) for pid, fn, fi in self.by_last_name[part]]
                if (player_id, full_name) not in existing:
                    self.by_last_name[part].append((player_id, full_name, initial))

    def find_player_id(self, name: str):
        """Find a player ID for the given name using multiple matching strategies."""
        if not name:
            return None

        normalized = self._normalize(name)
        components = self._extract_components(name)

        # Strategy 1: Exact match on normalized full name
        if normalized in self.by_full_name:
            return self.by_full_name[normalized]

        no_spaces = normalized.replace(' ', '')
        if no_spaces in self.by_full_name:
            return self.by_full_name[no_spaces]

        # Get significant parts sorted by length (longest first)
        significant_parts = sorted(
            [p for p in components['all_parts'] if len(p) > 1],
            key=len, reverse=True
        )

        initial = None
        for p in components['all_parts']:
            if len(p) == 1:
                initial = p
                break
        if not initial and len(significant_parts) >= 2:
            initial = min(significant_parts, key=len)[0]

        # Strategy 2: Match by longest name part + initial
        for part in significant_parts:
            if len(part) < 3:
                continue

            if part in self.by_last_name:
                candidates = self.by_last_name[part]

                if initial:
                    matching = [(pid, fn) for pid, fn, fi in candidates
                               if fi and fi[0] == initial]
                    if len(matching) == 1:
                        return matching[0][0]
                    if matching:
                        positive = [m for m in matching if m[0] > 0]
                        if positive:
                            return positive[0][0]
                        return matching[0][0]

                if len(candidates) == 1:
                    return candidates[0][0]

        # Strategy 3: last_name + initial combination
        last_name = components['last_name']
        first_initial = components['first_initial'] or initial

        if last_name and len(last_name) >= 3 and first_initial:
            key = f"{last_name}_{first_initial}"
            if key in self.by_last_initial:
                candidates = self.by_last_initial[key]
                if len(candidates) == 1:
                    return candidates[0][0]
                positive = [c for c in candidates if c[0] > 0]
                if len(positive) == 1:
                    return positive[0][0]
                if positive:
                    return positive[0][0]
                return candidates[0][0]

        # Strategy 4: Fuzzy match with all significant parts
        if len(significant_parts) >= 2:
            long_parts = [p for p in significant_parts if len(p) >= 3]
            if len(long_parts) >= 2:
                best_match = None
                best_score = 0

                for pid, full_name in self.players.items():
                    fn_normalized = self._normalize(full_name)
                    fn_parts = fn_normalized.split()

                    matches = sum(1 for sp in long_parts
                                 if any(sp == fp or sp in fp or fp in sp for fp in fn_parts))

                    if matches == len(long_parts):
                        score = matches * 10 + (1 if pid > 0 else 0)
                        if score > best_score:
                            best_score = score
                            best_match = pid

                if best_match:
                    return best_match

        # Strategy 5: Single part with initial
        if len(significant_parts) == 1 and initial:
            part = significant_parts[0]
            if len(part) >= 3 and part in self.by_last_name:
                candidates = self.by_last_name[part]
                matching = [(pid, fn) for pid, fn, fi in candidates
                           if fi and fi[0] == initial]
                if len(matching) == 1:
                    return matching[0][0]
                if matching:
                    positive = [m for m in matching if m[0] > 0]
                    if positive:
                        return positive[0][0]
                    return matching[0][0]

        return None

    def get_player_name(self, player_id: int):
        """Get the canonical name for a player ID."""
        return self.players.get(player_id)


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
        self.slug_cache_path = Path(__file__).parent / "player_slugs.json"
        self.player_slugs = {}  # name -> {slug, tour}
        self.scrape_cache = self._load_scrape_cache()
        self.db_lock = threading.Lock()
        self.name_matcher = PlayerNameMatcher()  # For robust name matching
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

    def _load_slug_cache(self) -> dict:
        """Load the player slug cache from disk."""
        if self.slug_cache_path.exists():
            try:
                with open(self.slug_cache_path, 'r') as f:
                    return json.load(f)
            except Exception:
                return {}
        return {}

    def _save_slug_cache(self):
        """Save the player slug cache to disk."""
        try:
            with open(self.slug_cache_path, 'w') as f:
                json.dump(self.player_slugs, f, indent=2)
        except Exception as e:
            log(f"Warning: Could not save slug cache: {e}")

    def _should_scrape_player(self, player_name: str, is_priority: bool) -> bool:
        """Check if we should scrape this player based on cache."""
        if is_priority:
            return True

        cache_key = player_name.lower()
        if cache_key in self.scrape_cache:
            last_scraped = datetime.fromisoformat(self.scrape_cache[cache_key])
            age_days = (datetime.now() - last_scraped).days
            if age_days < self.CACHE_TTL_DAYS:
                return False
        return True

    def _mark_player_scraped(self, player_name: str):
        """Mark a player as scraped in the cache."""
        self.scrape_cache[player_name.lower()] = datetime.now().isoformat()

    def _request(self, session, url, max_retries=3):
        """Make a request with retries and exponential backoff."""
        for attempt in range(max_retries):
            try:
                time.sleep(random.uniform(2.0, 4.0))
                response = session.get(url, timeout=30)
                if response.status_code == 200:
                    return response
                elif response.status_code == 429:
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

    def _load_players_into_matcher(self):
        """Load existing players from database into the name matcher."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT id, name FROM players")
        count = 0
        for row in cursor.fetchall():
            self.name_matcher.add_player(row[0], row[1])
            count += 1
        conn.close()
        if count > 0:
            log(f"  Loaded {count} existing players into name matcher")

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

    def _normalize_name(self, name: str) -> str:
        """Normalize a name for matching."""
        # Remove accents and special characters
        replacements = {
            'á': 'a', 'é': 'e', 'í': 'i', 'ó': 'o', 'ú': 'u',
            'ä': 'a', 'ë': 'e', 'ï': 'i', 'ö': 'o', 'ü': 'u',
            'ñ': 'n', 'ç': 'c', 'ş': 's', 'ğ': 'g',
            'ą': 'a', 'ę': 'e', 'ł': 'l', 'ń': 'n', 'ś': 's', 'ź': 'z', 'ż': 'z',
            'č': 'c', 'ř': 'r', 'š': 's', 'ž': 'z', 'ě': 'e', 'ů': 'u',
            'ț': 't', 'ș': 's', 'ă': 'a', 'î': 'i', 'â': 'a',
            'ø': 'o', 'å': 'a', 'æ': 'ae', 'ß': 'ss', 'ı': 'i',
            'ć': 'c', 'đ': 'd', 'ő': 'o', 'ű': 'u', 'ý': 'y',
        }
        name = name.lower().strip()
        for accented, plain in replacements.items():
            name = name.replace(accented, plain)
        return name

    def fetch_ranking_slugs(self, session, tour: str = 'ATP') -> dict:
        """Fetch player slugs from ranking pages."""
        slugs = {}

        if tour == 'ATP':
            base_url = f"{self.BASE_URL}/ranking/atp-men/"
        else:
            base_url = f"{self.BASE_URL}/ranking/wta-women/"

        # Fetch multiple pages to get more players
        for page in range(1, 16):  # Pages 1-15 (top ~750 players per tour)
            if page == 1:
                url = base_url
            else:
                url = f"{base_url}?page={page}"

            response = self._request(session, url)
            if not response:
                continue

            soup = BeautifulSoup(response.text, 'html.parser')
            player_links = soup.select('a[href*="/player/"]')

            for link in player_links:
                href = link.get('href', '')
                name = link.get_text(strip=True)

                if not name or not href:
                    continue

                # Extract slug from href
                match = re.search(r'/player/([^/]+)/?', href)
                if match:
                    slug = match.group(1)

                    # Normalize the name (Tennis Explorer uses "Lastname Firstname")
                    parts = name.split()
                    if len(parts) >= 2:
                        # Convert "Sinner Jannik" to "Jannik Sinner"
                        normalized = f"{parts[-1]} {' '.join(parts[:-1])}"
                    else:
                        normalized = name

                    key = self._normalize_name(normalized)
                    if key not in slugs:
                        slugs[key] = {'slug': slug, 'tour': tour, 'original_name': name}

            log(f"  {tour} page {page}: found {len(player_links)} player links, total unique: {len(slugs)}")

        return slugs

    def build_slug_lookup(self):
        """Build player name -> slug lookup from ranking pages."""
        log("Building player slug lookup from ranking pages...")

        # Try to load existing cache first
        self.player_slugs = self._load_slug_cache()
        if self.player_slugs:
            log(f"  Loaded {len(self.player_slugs)} slugs from cache")

        session = self._create_session()

        # Fetch ATP rankings
        log("Fetching ATP rankings...")
        atp_slugs = self.fetch_ranking_slugs(session, 'ATP')
        self.player_slugs.update(atp_slugs)

        time.sleep(5)  # Pause between tours

        # Fetch WTA rankings
        log("Fetching WTA rankings...")
        wta_slugs = self.fetch_ranking_slugs(session, 'WTA')
        self.player_slugs.update(wta_slugs)

        log(f"Total players in slug lookup: {len(self.player_slugs)}")
        self._save_slug_cache()

    def find_player_slug(self, player_name: str, session=None) -> dict:
        """Find the slug for a player name. Will try URL guessing if not in cache."""
        key = self._normalize_name(player_name)

        # Direct match
        if key in self.player_slugs:
            return self.player_slugs[key]

        # Try reversed name (Firstname Lastname -> Lastname Firstname)
        parts = player_name.split()
        if len(parts) >= 2:
            reversed_name = f"{parts[-1]} {' '.join(parts[:-1])}"
            reversed_key = self._normalize_name(reversed_name)
            if reversed_key in self.player_slugs:
                return self.player_slugs[reversed_key]

        # Try partial match on last name + first name
        last_name = self._normalize_name(parts[-1]) if parts else key
        first_name = self._normalize_name(parts[0]) if len(parts) > 1 else ""
        for cached_key, data in self.player_slugs.items():
            if last_name in cached_key and first_name and first_name in cached_key:
                return data

        # FALLBACK: Try to guess the slug and verify URL exists
        if session:
            guessed = self._guess_and_verify_slug(player_name, session)
            if guessed:
                # Cache it for future use
                self.player_slugs[key] = guessed
                return guessed

        return None

    def _guess_and_verify_slug(self, player_name: str, session) -> dict:
        """Try to guess player slug from name and verify it exists."""
        parts = player_name.split()
        if not parts:
            return None

        # Normalize name parts for URL
        def slugify(s):
            s = self._normalize_name(s)
            s = re.sub(r'[^a-z0-9]', '-', s)
            s = re.sub(r'-+', '-', s).strip('-')
            return s

        first_name = slugify(parts[0]) if parts else ""
        last_name = slugify(parts[-1]) if parts else ""

        # Common Tennis Explorer slug patterns to try
        slug_patterns = []
        if len(parts) >= 2:
            slug_patterns = [
                f"{last_name}-{first_name}",      # sinner-jannik
                f"{last_name}",                    # djokovic
                f"{first_name}-{last_name}",      # jannik-sinner
            ]
            # For names with middle parts like "De Minaur"
            if len(parts) > 2:
                middle = slugify(' '.join(parts[1:-1]))
                slug_patterns.insert(0, f"{middle}-{last_name}-{first_name}")
                slug_patterns.insert(1, f"{last_name}-{middle}-{first_name}")
        else:
            slug_patterns = [last_name]

        for slug in slug_patterns:
            url = f"{self.BASE_URL}/player/{slug}/"
            try:
                time.sleep(random.uniform(1.0, 2.0))
                response = session.get(url, timeout=15, allow_redirects=True)
                if response.status_code == 200:
                    # Verify it's a real player page by checking for player content
                    if 'plDetail' in response.text or 'player' in response.url:
                        # Determine tour from page content
                        tour = 'WTA' if 'wta' in response.text.lower()[:5000] else 'ATP'
                        return {'slug': slug, 'tour': tour, 'original_name': player_name}
            except Exception:
                continue

        return None

    def fetch_player_matches(self, session, slug: str, player_name: str,
                            tour: str, max_matches: int = 30, cutoff_date: str = None) -> list:
        """Fetch match history for a specific player."""
        matches = []
        url = f"{self.BASE_URL}/player/{slug}/?annual=all"

        response = self._request(session, url)
        if not response:
            return matches

        soup = BeautifulSoup(response.text, 'html.parser')

        # Generate player ID from slug
        player_id = hash(slug) % (10**9)
        if tour == 'WTA':
            player_id = -abs(player_id)

        # Extract player's last name for matching (e.g., "Sinner" from "Jannik Sinner")
        player_last_name = player_name.split()[-1].lower() if player_name else ""

        tables = soup.select('table.result')
        current_tournament = ""
        current_surface = "Hard"
        current_year = datetime.now().year

        for table in tables:
            rows = table.select('tr')

            for row in rows:
                try:
                    cells = row.select('td')

                    # Check for year/tournament header row (has 'year' class)
                    year_cell = row.select_one('td.year')
                    if year_cell:
                        year_link = year_cell.select_one('a')
                        if year_link:
                            href = year_link.get('href', '')
                            tournament_text = year_link.get_text(strip=True)

                            # Extract year from href like /australian-open/2025/
                            year_match = re.search(r'/(\d{4})/', href)
                            if year_match:
                                current_year = int(year_match.group(1))

                            # Extract tournament name
                            tournament_match = re.search(r'/([^/]+)/\d{4}/', href)
                            if tournament_match:
                                current_tournament = tournament_match.group(1).replace('-', ' ').title()

                            current_surface = self._guess_surface(current_tournament)
                        continue

                    # Look for match rows with date cell (class 'first time')
                    date_cell = row.select_one('td.first.time')
                    if not date_cell:
                        continue

                    date_text = date_cell.get_text(strip=True)
                    date_match = re.match(r'^(\d{1,2})\.(\d{1,2})\.$', date_text)
                    if not date_match:
                        continue

                    day, month = date_match.groups()
                    month_int = int(month)
                    day_int = int(day)

                    # Smart year detection
                    today = datetime.now()

                    # Try tournament year first
                    match_date = f"{current_year}-{month.zfill(2)}-{day.zfill(2)}"

                    try:
                        match_dt = datetime.strptime(match_date, '%Y-%m-%d')
                        days_ago = (today - match_dt).days

                        # If the date is in the future, adjust year
                        if match_dt > today:
                            match_date = f"{today.year}-{month.zfill(2)}-{day.zfill(2)}"
                            match_dt = datetime.strptime(match_date, '%Y-%m-%d')
                            if match_dt > today:
                                match_date = f"{today.year - 1}-{month.zfill(2)}-{day.zfill(2)}"

                        # Key fix: If tournament year is last year but we're early in current year,
                        # and the match month is the same as or earlier than current month,
                        # the match might be from THIS year, not last year
                        elif current_year == today.year - 1 and month_int <= today.month:
                            # Check if using current year gives a recent date (within last 30 days)
                            current_year_date = f"{today.year}-{month.zfill(2)}-{day.zfill(2)}"
                            current_year_dt = datetime.strptime(current_year_date, '%Y-%m-%d')
                            current_year_days_ago = (today - current_year_dt).days

                            # If current year date is recent (within 30 days) and old year date is ~1 year ago
                            # then use current year
                            if 0 <= current_year_days_ago <= 30 and days_ago > 300:
                                match_date = current_year_date

                        # Also handle: tournament year is 2 years old but match should be recent
                        elif days_ago > 350:
                            # Try adding a year
                            newer_date = f"{current_year + 1}-{month.zfill(2)}-{day.zfill(2)}"
                            newer_dt = datetime.strptime(newer_date, '%Y-%m-%d')
                            if newer_dt <= today:
                                match_date = newer_date

                    except ValueError:
                        pass  # Invalid date, skip this match

                    if cutoff_date and match_date < cutoff_date:
                        continue

                    # Get match name cell (class 't-name')
                    name_cell = row.select_one('td.t-name')
                    if not name_cell:
                        continue

                    match_text = name_cell.get_text(strip=True)

                    # Skip doubles matches
                    if '/' in match_text:
                        continue

                    # Parse "Player1-Player2" format
                    if '-' not in match_text:
                        continue

                    players = match_text.split('-')
                    if len(players) != 2:
                        continue

                    player1_name = players[0].strip()
                    player2_name = players[1].strip()

                    # Determine if our player won (first position = winner)
                    # Match names are in format "Winner-Loser"
                    is_win = player_last_name in player1_name.lower()

                    # Get opponent info
                    opponent_name = player2_name if is_win else player1_name
                    opponent_link = name_cell.select_one('a[href*="/player/"]')

                    # Try to find opponent in existing players using name matcher
                    opponent_id = self.name_matcher.find_player_id(opponent_name)

                    if opponent_id is None:
                        # Fallback to hash-based ID
                        if opponent_link:
                            opponent_href = opponent_link.get('href', '')
                            opp_match = re.search(r'/player/([^/]+)', opponent_href)
                            opponent_slug = opp_match.group(1) if opp_match else None
                            opponent_id = hash(opponent_slug) % (10**9) if opponent_slug else hash(opponent_name) % (10**9)
                        else:
                            opponent_id = hash(opponent_name) % (10**9)

                        if tour == 'WTA':
                            opponent_id = -abs(opponent_id)

                    # Get score
                    score_cell = row.select_one('td.tl')
                    score_text = score_cell.get_text(strip=True) if score_cell else ""

                    # Get round
                    round_cell = row.select_one('td.round')
                    round_text = round_cell.get_text(strip=True) if round_cell else ""

                    if is_win:
                        winner_id, winner_name_out = player_id, player_name
                        loser_id, loser_name = opponent_id, opponent_name
                    else:
                        winner_id, winner_name_out = opponent_id, opponent_name
                        loser_id, loser_name = player_id, player_name

                    match_id = f"TE_{match_date}_{abs(winner_id)}_{abs(loser_id)}"

                    matches.append({
                        'id': match_id,
                        'date': match_date,
                        'tournament': current_tournament,
                        'surface': current_surface,
                        'round': round_text,
                        'winner_id': winner_id,
                        'winner_name': winner_name_out,
                        'loser_id': loser_id,
                        'loser_name': loser_name,
                        'score': score_text,
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

            # Also add to name matcher for lookups
            self.name_matcher.add_player(player['id'], player['name'])

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
        cursor.execute("INSERT OR REPLACE INTO metadata (key, value) VALUES ('version', '4.1')")

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

    def fetch_upcoming_matches(self, session) -> dict:
        """Fetch upcoming matches from Tennis Explorer for today and tomorrow.

        Returns dict with 'players' set and 'matches' list.
        """
        players = set()
        matches = []

        # Fetch today and tomorrow
        today = datetime.now()
        dates_to_fetch = [
            today,
            today + timedelta(days=1),
        ]

        for date in dates_to_fetch:
            date_str = date.strftime('%Y-%m-%d')
            url = f"{self.BASE_URL}/matches/?type=all&year={date.year}&month={date.month}&day={date.day}"

            log(f"  Fetching matches for {date_str}...")
            response = self._request(session, url)
            if not response:
                continue

            soup = BeautifulSoup(response.text, 'html.parser')

            # Find all match rows
            match_rows = soup.select('tr.bott')

            for row in match_rows:
                try:
                    # Get player links
                    player_links = row.select('td.t-name a[href*="/player/"]')

                    for link in player_links:
                        name = link.get_text(strip=True)
                        href = link.get('href', '')

                        if not name or '/' in name:  # Skip doubles
                            continue

                        # Convert "Lastname F." or "Lastname Firstname" format
                        # Tennis Explorer uses "Lastname Firstname" format
                        parts = name.split()
                        if len(parts) >= 2:
                            # Convert "Sinner Jannik" to "Jannik Sinner"
                            converted = f"{parts[-1]} {' '.join(parts[:-1])}"
                            players.add(converted)
                        else:
                            players.add(name)

                        # Extract slug if available
                        slug_match = re.search(r'/player/([^/]+)', href)
                        if slug_match:
                            slug = slug_match.group(1)
                            key = self._normalize_name(name)
                            if key not in self.player_slugs:
                                # Determine tour from context
                                tour = 'WTA' if 'wta' in href.lower() else 'ATP'
                                self.player_slugs[key] = {
                                    'slug': slug,
                                    'tour': tour,
                                    'original_name': name
                                }

                    # Extract match info
                    time_cell = row.select_one('td.first.time')
                    tournament_row = row.find_previous('tr', class_='head')

                    if player_links and len(player_links) >= 2:
                        p1_name = player_links[0].get_text(strip=True)
                        p2_name = player_links[1].get_text(strip=True)

                        match_info = {
                            'date': date_str,
                            'time': time_cell.get_text(strip=True) if time_cell else '',
                            'player1': p1_name,
                            'player2': p2_name,
                            'tournament': '',
                        }

                        if tournament_row:
                            tourn_link = tournament_row.select_one('a')
                            if tourn_link:
                                match_info['tournament'] = tourn_link.get_text(strip=True)

                        matches.append(match_info)

                except Exception:
                    continue

            log(f"    Found {len(match_rows)} match rows, {len(players)} unique players so far")

        # Save upcoming matches to file for reference
        upcoming_file = Path(__file__).parent / "upcoming_matches.json"
        upcoming_data = {
            "last_updated": datetime.now().isoformat(),
            "match_count": len(matches),
            "player_count": len(players),
            "matches": matches
        }
        with open(upcoming_file, 'w', encoding='utf-8') as f:
            json.dump(upcoming_data, f, indent=2, ensure_ascii=False)
        log(f"  Saved {len(matches)} upcoming matches to upcoming_matches.json")

        return {'players': players, 'matches': matches}

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

        # Find player slug (will try URL guessing if not in rankings)
        player_data = self.find_player_slug(player_name, session=session)
        if not player_data:
            return result

        result['found'] = True
        slug = player_data['slug']
        tour = player_data['tour']

        # Generate player ID
        player_id = hash(slug) % (10**9)
        if tour == 'WTA':
            player_id = -abs(player_id)

        # Save player info
        self.save_player({
            'id': player_id,
            'name': player_name,
            'tour': tour,
            'slug': slug,
            'country': '',
            'ranking': None
        })

        # Fetch matches
        matches = self.fetch_player_matches(
            session,
            slug,
            player_name,
            tour,
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

        # Load existing players into name matcher for robust matching
        log("\nLoading existing players into name matcher...")
        self._load_players_into_matcher()

        # Build slug lookup from ranking pages
        self.build_slug_lookup()

        # Fetch upcoming matches from Tennis Explorer (today + tomorrow)
        log("\nFetching upcoming matches from Tennis Explorer...")
        session = self._create_session()
        upcoming = self.fetch_upcoming_matches(session)
        upcoming_players = upcoming['players']
        log(f"Found {len(upcoming_players)} players with upcoming matches")

        # Calculate cutoff date (12 months ago)
        cutoff_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
        log(f"Fetching matches since {cutoff_date}")

        # Load player lists
        all_players = self.load_player_list()
        priority_players = self.load_priority_players()

        # Add upcoming match players to priority (they MUST be scraped)
        priority_players = priority_players.union(upcoming_players)

        log(f"Loaded {len(all_players)} players from file")
        log(f"Total priority players (upcoming + Betfair): {len(priority_players)}")

        if not all_players and not priority_players:
            log("No players to scrape!")
            return

        # Combine and deduplicate - priority players first (upcoming matches)
        player_queue = []
        seen = set()

        # Priority players (upcoming matches) go first and are always scraped
        for p in priority_players:
            if p.lower() not in seen:
                player_queue.append((p, True))
                seen.add(p.lower())

        # Then add players from the static list
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
            futures = {
                executor.submit(self._scrape_single_player, name, is_priority, cutoff_date): (name, is_priority)
                for name, is_priority in player_queue
            }

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

                    if completed % 25 == 0 or completed == len(futures):
                        log(f"  Progress: {completed}/{len(futures)} | Found: {players_found} | "
                            f"Skipped: {players_skipped} | Matches: {total_matches}")

                except Exception as e:
                    log(f"    Error processing {player_name}: {e}")
                    players_not_found.append(player_name)

        # Save caches (including any newly discovered slugs)
        self._save_scrape_cache()
        self._save_slug_cache()

        log(f"\nScraping complete:")
        log(f"  Players found: {players_found}")
        log(f"  Players skipped (cached): {players_skipped}")
        log(f"  Players not found: {len(players_not_found)}")
        log(f"  Total matches collected: {total_matches}")

        # Always save missing players to file
        if players_not_found:
            missing_file = Path(__file__).parent / "missing_players.json"
            missing_data = {
                "last_updated": datetime.now().isoformat(),
                "count": len(players_not_found),
                "players": sorted(players_not_found)
            }
            with open(missing_file, 'w', encoding='utf-8') as f:
                json.dump(missing_data, f, indent=2, ensure_ascii=False)
            log(f"\nMissing players saved to: missing_players.json")

            # Also log them
            log(f"\nPlayers not found ({len(players_not_found)}):")
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


def test_search():
    """Test the scraper functionality."""
    log("Testing scraper...")
    scraper = TennisDataScraper()

    # Build slug lookup
    scraper.build_slug_lookup()

    # Test finding some players
    test_players = ["Jannik Sinner", "Carlos Alcaraz", "Novak Djokovic", "Iga Swiatek", "Aryna Sabalenka"]

    for player in test_players:
        data = scraper.find_player_slug(player)
        if data:
            log(f"  {player} -> {data['slug']} ({data['tour']})")
        else:
            log(f"  {player} -> NOT FOUND")

    # Test fetching matches for one player
    log("\nTesting match fetch for Sinner...")
    session = scraper._create_session()
    sinner_data = scraper.find_player_slug("Jannik Sinner")
    if sinner_data:
        matches = scraper.fetch_player_matches(
            session,
            sinner_data['slug'],
            "Jannik Sinner",
            sinner_data['tour'],
            max_matches=10
        )
        log(f"  Found {len(matches)} matches")
        for m in matches[:5]:
            log(f"    {m['date']}: {m['winner_name']} d. {m['loser_name']} ({m['score']}) - {m['tournament']}")


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "test":
        test_search()
    else:
        scraper = TennisDataScraper()
        scraper.run_full_refresh(max_workers=3)
