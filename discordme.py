import aiohttp
import asyncio
from typing import Dict, Optional, Tuple
import json
import logging
from datetime import datetime, timezone, timedelta
from aiohttp import ClientTimeout
from bs4 import BeautifulSoup
import os
import re
from dotenv import load_dotenv
from urllib.parse import unquote

class DiscordMeBrowser:
    def __init__(self, server_eid: str, max_retries: int = 3):
        # load env
        load_dotenv()

        self.base_url = "https://discord.me"
        self.server_eid = server_eid
        self.max_retries = max_retries
        self.timeout = ClientTimeout(total=30)  # 30s timeout

        self.headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Accept-Language": "en-US,en;q=0.9",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "Referer": f"{self.base_url}/server-manager/{self.server_eid}",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        }

        # retrieve only the discord_me_session cookie
        self.cookies = {
            "discord_me_session": os.getenv("DISCORD_ME_SESSION"),
        }

        # remove any cookies that are None
        self.cookies = {k: v for k, v in self.cookies.items() if v is not None}

        self.session: Optional[aiohttp.ClientSession] = None
        self._last_bump_check: Optional[datetime] = None

        # initialize CSRF token
        self.csrf_token: Optional[str] = None

    async def _make_request(self, method: str, url: str, **kwargs) -> Tuple[bool, Optional[str], Optional[dict]]:
        """Make a request with retry logic and error handling."""
        for attempt in range(self.max_retries):
            try:
                async with self.session.request(method, url, **kwargs) as response:
                    text = await response.text()
                    if response.status == 200:
                        # update cookies from response
                        self.session.cookie_jar.update_cookies(response.cookies, response.url)
                        return True, text, dict(response.headers)
                    elif response.status == 429:  # rate limited
                        retry_after = int(response.headers.get('Retry-After', 60))
                        logging.warning(f"Rate limited. Waiting {retry_after}s before retry.")
                        await asyncio.sleep(retry_after)
                        continue
                    elif response.status in {401, 403}:
                        return False, None, {'error': 'Authentication failed'}
                    else:
                        logging.error(f"Request failed with status {response.status}")
                        return False, None, {'error': f'HTTP {response.status}'}
            except asyncio.TimeoutError:
                logging.warning(f"Request timeout on attempt {attempt + 1}/{self.max_retries}")
                if attempt == self.max_retries - 1:
                    return False, None, {'error': 'Timeout'}
            except aiohttp.ClientError as e:
                logging.exception(f"Client error on attempt {attempt + 1}/{self.max_retries}")
                return False, None, {'error': str(e)}
            except Exception as e:
                logging.exception(f"Unexpected error on attempt {attempt + 1}/{self.max_retries}")
                return False, None, {'error': str(e)}

            await asyncio.sleep(min(2 ** attempt, 30))  # exponential backoff with max delay

        return False, None, {'error': 'Max retries exceeded'}

    async def init_session(self) -> bool:
        """Initialize session with error handling and validation."""
        try:
            if self.session and not self.session.closed:
                await self.session.close()

            # initialize cookie jar with provided cookies
            cookie_jar = aiohttp.CookieJar()
            for name, value in self.cookies.items():
                cookie_jar.update_cookies({name: value})

            self.session = aiohttp.ClientSession(
                timeout=self.timeout,
                cookie_jar=cookie_jar
            )

            success, text, headers = await self._make_request(
                'GET',
                f"{self.base_url}/dashboard",
                headers=self.headers,
                allow_redirects=True
            )

            if success and 'Your Dashboard' in text:
                logging.info("Session initialized successfully")
                return True

            logging.error("Failed to validate dashboard access")
            return False

        except Exception as e:
            logging.exception("Session initialization error")
            return False

    def get_last_reset_time(self, now: datetime) -> datetime:
        """Get the last reset time (Saturday at 00:00 UTC before 'now')."""
        # 'now' should be in UTC
        weekday = now.weekday()  # monday=0, ..., sunday=6
        days_since_reset = (weekday - 5) % 7  # saturday=5
        reset_date = (now - timedelta(days=days_since_reset)).date()
        reset_time = datetime.combine(reset_date, datetime.min.time(), tzinfo=timezone.utc)
        if now < reset_time:
            # if now is before reset_time, subtract 7 days
            reset_time -= timedelta(days=7)
        return reset_time

    def extract_csrf_token(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract CSRF token from the meta tag in the page."""
        csrf_meta = soup.find('meta', {'name': 'csrf-token'})
        if csrf_meta:
            return csrf_meta.get('content')
        return None

    def get_xsrf_token(self) -> Optional[str]:
        """Get the URL-decoded value of the XSRF-TOKEN cookie."""
        cookies = self.session.cookie_jar.filter_cookies(self.base_url)
        xsrf_cookie = cookies.get('XSRF-TOKEN')
        if xsrf_cookie:
            # URL-decode the cookie value
            return unquote(xsrf_cookie.value)
        return None

    async def check_bump_status(self) -> Dict:
        """Check server bump status by accessing the server manager page."""
        if not self.session or self.session.closed:
            if not await self.init_session():
                return {'success': False, 'error': 'Session initialization failed'}

        try:
            success, html, headers = await self._make_request(
                'GET',
                f"{self.base_url}/server-manager/{self.server_eid}",
                headers=self.headers
            )

            if not success:
                return {'success': False, 'error': headers.get('error', 'Unknown error')}

            soup = BeautifulSoup(html, 'html.parser')

            # extract CSRF token
            self.csrf_token = self.extract_csrf_token(soup)
            if not self.csrf_token:
                logging.error("CSRF token not found on the server manager page")
                return {'success': False, 'error': 'CSRF token not found'}

            # extract bump info
            bump_info = {
                'can_bump': False,
                'next_bump_time': None,
                'time_until_next_bump': None,
                'points': None,
                'missed_bumps': None,
            }

            # extract points
            points_elem = soup.find(string=re.compile(r"Bump Points:"))
            if points_elem:
                points_text = points_elem.parent.text.strip()
                match = re.search(r"Bump Points:\s*(\d+)", points_text)
                if match:
                    bump_info['points'] = int(match.group(1))

            # extract bump button data
            bump_button = soup.find('a', class_='bump-btn')
            if bump_button:
                can_bump_str = bump_button.get('data-boolean-canbump', '')
                next_bump_time_str = bump_button.get('data-date-nextbump', '')

                # convert next_bump_time_str to datetime
                if next_bump_time_str:
                    next_bump_time_str = next_bump_time_str.strip('"')
                    next_bump_time = datetime.fromisoformat(next_bump_time_str.replace('Z', '+00:00'))
                else:
                    next_bump_time = None

                # determine if the server can be bumped
                can_bump = can_bump_str in ('1', 'true', 'True')

                bump_info['can_bump'] = can_bump
                bump_info['next_bump_time'] = next_bump_time.isoformat() if next_bump_time else None

                if not can_bump and next_bump_time:
                    # calculate time until next bump
                    now = datetime.now(timezone.utc)
                    time_diff = next_bump_time - now
                    bump_info['time_until_next_bump'] = max(int(time_diff.total_seconds()), 0)

            else:
                logging.error("Bump button not found on the server manager page")
                return {'success': False, 'error': 'Bump button not found on the server manager page'}

            # compute missed bumps
            try:
                now_utc = datetime.now(timezone.utc)
                reset_time = self.get_last_reset_time(now_utc)
                hours_since_reset = (now_utc - reset_time).total_seconds() / 3600.0
                total_bump_windows = int(hours_since_reset // 6) + 1  # +1 to include current window
                total_possible_points = total_bump_windows * 2
                actual_points = bump_info.get('points', 0)
                missed_points = total_possible_points - actual_points
                missed_bumps = max(missed_points // 2, 0)
                bump_info['missed_bumps'] = int(missed_bumps)
            except Exception as e:
                logging.exception("Error computing missed bumps")
                bump_info['missed_bumps'] = None

            self._last_bump_check = datetime.now(timezone.utc)

            return {
                'success': True,
                'data': bump_info,
                'timestamp': self._last_bump_check.isoformat(),
            }

        except Exception as e:
            logging.exception("Error checking bump status")
            return {
                'success': False,
                'error': str(e)
            }

    async def perform_bump(self) -> Dict:
        """Perform the bump action if eligible."""
        if not self.session or self.session.closed:
            if not await self.init_session():
                return {'success': False, 'error': 'Session initialization failed'}

        try:
            # prepare the bump request URL and data
            bump_url = f"{self.base_url}/api/server/bump"
            payload = {
                'eid': self.server_eid,
            }
            headers = self.headers.copy()
            headers.update({
                'Accept': '*/*',
                'Accept-Encoding': 'gzip, deflate, br, zstd',
                'Accept-Language': 'en-US,en;q=0.7',
                'Cache-Control': 'no-cache',
                'Content-Type': 'application/json',
                'Origin': self.base_url,
                'Pragma': 'no-cache',
                'Referer': f"{self.base_url}/dashboard",
                'Sec-Fetch-Dest': 'empty',
                'Sec-Fetch-Mode': 'cors',
                'Sec-Fetch-Site': 'same-origin',
                'Sec-GPC': '1',
                'X-CSRF-TOKEN': self.csrf_token,
                'X-Requested-With': 'XMLHttpRequest',
            })

            xsrf_token = self.get_xsrf_token()
            if xsrf_token:
                headers['X-XSRF-TOKEN'] = xsrf_token
            else:
                logging.error("XSRF-TOKEN cookie not found")
                return {'success': False, 'error': 'XSRF-TOKEN cookie not found'}

            # make the POST request
            success, response_text, response_headers = await self._make_request(
                'POST',
                bump_url,
                headers=headers,
                json=payload
            )

            if success:
                response_data = json.loads(response_text)
                if response_data.get('success'):
                    logging.info("Server bumped successfully")
                    return {'success': True, 'message': 'Server bumped successfully'}
                else:
                    error_message = response_data.get('message', 'Unknown error')
                    logging.error(f"Bump failed: {error_message}")
                    return {'success': False, 'error': error_message}
            else:
                error_message = response_headers.get('error', 'Unknown error')
                logging.error(f"Bump request failed: {error_message}")
                return {'success': False, 'error': error_message}

        except Exception as e:
            logging.exception("Error performing bump")
            return {'success': False, 'error': str(e)}

    async def close(self):
        """Clean up resources."""
        if self.session and not self.session.closed:
            await self.session.close()

async def main():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    # load your server EID from environment variable or directly
    server_eid = os.getenv("SERVER_EID") or "your_server_eid_here"  # replace with your server's EID if not in env
    client = DiscordMeBrowser(server_eid)

    try:
        status = await client.check_bump_status()
        print(json.dumps(status, indent=2))

        if status['success']:
            bump_info = status['data']
            if bump_info['can_bump']:
                bump_result = await client.perform_bump()
                print(json.dumps(bump_result, indent=2))
            else:
                print("Cannot bump now. Next bump time:", bump_info['next_bump_time'])
        else:
            print("Failed to check bump status:", status.get('error'))
    finally:
        await client.close()

if __name__ == "__main__":
    asyncio.run(main())
