from typing import Set, Deque, Optional, Dict, FrozenSet, List
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser
from collections import deque
import time
import logging
from datetime import datetime
import random
import json
from slugify import slugify
import threading
from concurrent.futures import ThreadPoolExecutor
import os

MAX_WORKERS = 4  # Number of concurrent threads

HTTP_SUCCESS = 200
REQUEST_TIMEOUT = 10
PROGRESS_INTERVAL = 100
DEFAULT_DELAY = 1
MAX_PAGES = 5_000

MAX_RETRIES = 3
INITIAL_BACKOFF = 5
MAX_BACKOFF = 120
RATE_LIMIT_CODES = frozenset([429, 503])

EXCLUDED_EXTENSIONS: FrozenSet[str] = frozenset(
    [".jpg", ".jpeg", ".png", ".gif", ".pdf", ".zip", ".css", ".js"]
)

USER_AGENT = "Mozilla/5.0 (compatible; BillyReidCrawler/1.0)"
ACCEPT_HEADER = "text/html,application/xhtml+xml,application/xml;q=0.9"
ACCEPT_LANGUAGE = "en-US,en;q=0.5"

REQUEST_HEADERS: Dict[str, str] = {
    "User-Agent": USER_AGENT,
    "Accept": ACCEPT_HEADER,
    "Accept-Language": ACCEPT_LANGUAGE,
}

LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
LOG_NAME = "WebsiteCrawler"
WEBSITES_FILE = "websites_to_crawl.txt"

PRODUCTS_PATH = "products"
PRODUCTS_PATTERN = f"/{PRODUCTS_PATH}/"
PRODUCT_URL_PATTERNS = [PRODUCTS_PATTERN]
LOGS = "logs"
RESULTS = "results"


def _read_websites() -> List[str]:
    try:
        with open(WEBSITES_FILE, "r", encoding="utf-8") as f:
            websites = [line.strip() for line in f if line.strip()]
            return websites
    except FileNotFoundError:
        print(f"Error: {WEBSITES_FILE} not found")
        return []
    except Exception as e:
        print(f"Error reading {WEBSITES_FILE}: {str(e)}")
        return []


def _crawl_website(website: str) -> None:
    try:
        thread_name = threading.current_thread().name
        print(f"\nStarting crawl of {website} in thread {thread_name}")
        crawler = WebCrawler(website, delay=DEFAULT_DELAY)
        crawler.crawl(max_pages=MAX_PAGES)
        print(f"Completed crawl of {website} in thread {thread_name}\n")
    except Exception as e:
        print(f"Error crawling {website}: {str(e)}")


def _ensure_logs_and_results_directory():
    os.makedirs(LOGS, exist_ok=True)
    os.makedirs(RESULTS, exist_ok=True)


class WebCrawler:
    def __init__(self, start_url: str, delay: int = DEFAULT_DELAY) -> None:
        self.start_url: str = start_url
        self.delay: int = delay
        self.visited: Set[str] = set()
        self.queue: Deque[str] = deque([start_url])
        self.domain: str = urlparse(start_url).netloc
        self.product_urls: Set[str] = set()
        self.domain_slug: str = slugify(self.domain)

        self._setup_logging()
        self._setup_robot_parser()

    def _setup_logging(self) -> None:
        self.logger = logging.getLogger(LOG_NAME)
        self.logger.setLevel(logging.INFO)

        timestamp: str = datetime.now().strftime("%Y%m%d_%H%M%S")
        fh = logging.FileHandler(
            f"{LOGS}/crawler_log_{self.domain_slug}_{timestamp}.txt"
        )
        fh.setLevel(logging.INFO)

        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)

        formatter = logging.Formatter(LOG_FORMAT)
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)

        self.logger.addHandler(fh)

    def _setup_robot_parser(self) -> None:
        self.robot_file_parser: RobotFileParser = RobotFileParser()
        self.robot_file_parser.set_url(
            f"{urlparse(self.start_url).scheme}://{self.domain}/robots.txt"
        )
        try:
            self.robot_file_parser.read()
            self.logger.info("Successfully parsed robots.txt")
        except Exception as e:
            self.logger.error(f"Error parsing robots.txt: {str(e)}")

    def _is_product_url(self, url: str) -> bool:
        parsed = urlparse(url)
        return any(pattern in parsed.path for pattern in PRODUCT_URL_PATTERNS)

    def _is_valid_url(self, url: str) -> bool:
        try:
            parsed = urlparse(url)
            if not (bool(parsed.netloc) and parsed.netloc == self.domain):
                return False

            if not self.robot_file_parser.can_fetch("*", url):
                return False

            parsed_url = urlparse(url)
            if any(
                parsed_url.path.lower().endswith(ext) for ext in EXCLUDED_EXTENSIONS
            ):
                return False

            return True
        except Exception as e:
            self.logger.error(f"Error validating URL {url}: {str(e)}")
            return False

    # Remove /collections/abc from the URL
    def _clean_product_url(self, url: str) -> str:
        parsed = urlparse(url)
        path_parts = parsed.path.split("/")

        if PRODUCTS_PATH in path_parts:
            product_index = path_parts.index(PRODUCTS_PATH)
            cleaned_path = f"{PRODUCTS_PATTERN}" + "/".join(
                path_parts[product_index + 1 :]
            )
            return f"{parsed.scheme}://{parsed.netloc}{cleaned_path}"
        return url

    def _get_links(self, html: str) -> Set[str]:
        soup = BeautifulSoup(html, "html.parser")
        links: Set[str] = set()

        for anchor in soup.find_all("a"):
            href: Optional[str] = anchor.get("href")
            if href:
                absolute_url: str = urljoin(self.start_url, href)
                if self._is_valid_url(absolute_url):
                    if self._is_product_url(absolute_url):
                        cleaned_url = self._clean_product_url(absolute_url)
                        self.product_urls.add(cleaned_url)
                        absolute_url = cleaned_url
                    links.add(absolute_url)

        return links

    def _make_request(self, url: str) -> Optional[requests.Response]:
        retries = 0
        backoff = INITIAL_BACKOFF

        while retries <= MAX_RETRIES:
            try:
                response = requests.get(
                    url, timeout=REQUEST_TIMEOUT, headers=REQUEST_HEADERS
                )

                if response.status_code == HTTP_SUCCESS:
                    return response

                if response.status_code in RATE_LIMIT_CODES:
                    if retries == MAX_RETRIES:
                        self.logger.error(f"Max retries reached for {url}")
                        return None

                    jitter = random.uniform(0, 1)
                    sleep_time = min(backoff + jitter, MAX_BACKOFF)

                    self.logger.warning(
                        f"Rate limited on {url}. Retrying in {sleep_time:.1f} seconds. "
                        f"Attempt {retries + 1}/{MAX_RETRIES}"
                    )

                    time.sleep(sleep_time)
                    backoff *= 2
                    retries += 1
                    continue

                return response

            except requests.RequestException as e:
                if retries == MAX_RETRIES:
                    self.logger.error(f"Max retries reached for {url}: {str(e)}")
                    return None

                sleep_time = min(backoff + random.uniform(0, 1), MAX_BACKOFF)
                self.logger.warning(
                    f"Error fetching {url}: {str(e)}. Retrying in {sleep_time:.1f} seconds. "
                    f"Attempt {retries + 1}/{MAX_RETRIES}"
                )

                time.sleep(sleep_time)
                backoff *= 2
                retries += 1

        return None

    def crawl(self, max_pages: Optional[int] = None) -> None:
        pages_crawled: int = 0
        start_time: float = time.time()

        self.logger.info(f"Starting crawl of {self.start_url}")
        self.logger.info(f"Max pages set to: {max_pages}")

        while self.queue and (max_pages is None or pages_crawled < max_pages):
            current_url: str = self.queue.popleft()

            if current_url in self.visited:
                continue

            self.logger.info(f"Crawling: {current_url}")
            response = self._make_request(current_url)

            if response:
                self.visited.add(current_url)

                if self._is_product_url(current_url):
                    self.logger.info(f"Found product URL: {current_url}")

                new_links: Set[str] = self._get_links(response.text)

                for link in new_links:
                    if link not in self.visited:
                        self.queue.append(link)

                pages_crawled += 1

                if pages_crawled % PROGRESS_INTERVAL == 0:
                    self.logger.info(f"Processed {pages_crawled} pages")
                    self.logger.info(
                        f"Found {len(self.product_urls)} product URLs so far"
                    )

                time.sleep(self.delay)

        self._save_crawl_stats(start_time)
        self._save_results()

    def _save_crawl_stats(self, start_time: float) -> None:
        end_time: float = time.time()
        duration: float = end_time - start_time

        self.logger.info("\nCrawling completed!")
        self.logger.info(f"Total pages crawled: {len(self.visited)}")
        self.logger.info(f"Total product URLs found: {len(self.product_urls)}")
        self.logger.info(f"Total time: {duration:.2f} seconds")
        self.logger.info(
            f"Average time per page: {duration/len(self.visited):.2f} seconds"
        )

    def _save_results(self) -> None:
        timestamp: str = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Save product URLs to JSON
        product_filename: str = (
            f"{RESULTS}/product_urls_{self.domain_slug}_{timestamp}.json"
        )
        product_data = {
            "domain": self.domain,
            "timestamp": timestamp,
            "total_products": len(self.product_urls),
            "urls": sorted(list(self.product_urls)),
        }

        with open(product_filename, "w", encoding="utf-8") as f:
            json.dump(product_data, f, indent=2)
        self.logger.info(f"Saved product URLs to {product_filename}")

        # Save crawled URLs to JSON
        crawled_filename: str = (
            f"{RESULTS}/crawled_urls_{self.domain_slug}_{timestamp}.json"
        )
        crawled_data = {
            "domain": self.domain,
            "timestamp": timestamp,
            "total_crawled": len(self.visited),
            "urls": sorted(list(self.visited)),
        }

        with open(crawled_filename, "w", encoding="utf-8") as f:
            json.dump(crawled_data, f, indent=2)
        self.logger.info(f"Saved crawled URLs to {crawled_filename}")


def main() -> None:
    websites = list(set(_read_websites()))
    if not websites:
        print("No websites to crawl. Exiting.")
        return

    print(f"Found {len(websites)} websites to crawl")
    print(f"Using {MAX_WORKERS} threads")

    _ensure_logs_and_results_directory()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(_crawl_website, website) for website in websites]        
        # Wait for all threads to complete
        for future in futures:
            future.result()


if __name__ == "__main__":
    main()