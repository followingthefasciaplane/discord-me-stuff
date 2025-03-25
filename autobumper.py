import aiohttp
import asyncio
from typing import Dict, Optional, Tuple, List
import json
import logging
import os
import re
from datetime import datetime, timezone, timedelta
from aiohttp import ClientTimeout
from bs4 import BeautifulSoup
from urllib.parse import unquote
from dotenv import load_dotenv

class DiscordMeAutoBumper:
    def __init__(self, server_eid: str, max_retries: int = 3, check_interval: int = 300):
        # load env
        load_dotenv()

        # base configuration
        self.base_url = "https://discord.me"
        self.server_eid = server_eid
        self.max_retries = max_retries
        self.timeout = ClientTimeout(total=30)  # 30s timeout
        self.check_interval = check_interval
        self.running = False

        # define bump windows (in UTC)
        self.bump_windows = [
            (0, 5),    # 00:00 - 05:59
            (6, 11),   # 06:00 - 11:59
            (12, 17),  # 12:00 - 17:59
            (18, 23)   # 18:00 - 23:59
        ]

        # HTTP headers
        self.headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Accept-Language": "en-US,en;q=0.9",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "Referer": f"{self.base_url}/server-manager/{self.server_eid}",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        }

        # retrieve session cookie
        self.cookies = {
            "discord_me_session": os.getenv("DISCORD_ME_SESSION"),
        }

        # remove any cookies that are None
        self.cookies = {k: v for k, v in self.cookies.items() if v is not None}

        # initialize session and state
        self.session: Optional[aiohttp.ClientSession] = None
        self._last_bump_check: Optional[datetime] = None
        self.csrf_token: Optional[str] = None
        
        # bump tracking
        self.current_bumped_windows = set()
        self.bump_history = []

    # --- HTTP and session Management ---
    
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

            # exponential backoff with max delay
            await asyncio.sleep(min(2 ** attempt, 30))

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

    def get_xsrf_token(self) -> Optional[str]:
        """Get the URL-decoded value of the XSRF-TOKEN cookie."""
        cookies = self.session.cookie_jar.filter_cookies(self.base_url)
        xsrf_cookie = cookies.get('XSRF-TOKEN')
        if xsrf_cookie:
            # URL-decode the cookie value
            return unquote(xsrf_cookie.value)
        return None

    # --- time and window management ---
    
    def get_current_window(self, now: Optional[datetime] = None) -> int:
        """Get the current bump window (0-3) based on UTC hour."""
        if now is None:
            now = datetime.now(timezone.utc)
        
        current_hour = now.hour
        
        for i, (start_hour, end_hour) in enumerate(self.bump_windows):
            if start_hour <= current_hour <= end_hour:
                return i
                
        # should never happen, but just in case
        return -1

    def get_window_time_range(self, window_index: int) -> Tuple[datetime, datetime]:
        """Get the start and end time for a specific window."""
        now = datetime.now(timezone.utc)
        today = now.replace(hour=0, minute=0, second=0, microsecond=0)
        
        start_hour, end_hour = self.bump_windows[window_index]
        
        window_start = today.replace(hour=start_hour)
        window_end = today.replace(hour=end_hour, minute=59, second=59)
        
        return window_start, window_end

    def get_next_window_start(self, now: Optional[datetime] = None) -> datetime:
        """Get the start time of the next window."""
        if now is None:
            now = datetime.now(timezone.utc)
            
        current_window = self.get_current_window(now)
        next_window = (current_window + 1) % 4
        
        # calculate next window start time
        start_hour, _ = self.bump_windows[next_window]
        
        # if we're moving to window 0, it's tomorrow
        if next_window == 0 and current_window == 3:
            next_day = now + timedelta(days=1)
            next_window_start = next_day.replace(hour=start_hour, minute=0, second=0, microsecond=0)
        else:
            next_window_start = now.replace(hour=start_hour, minute=0, second=0, microsecond=0)
            
        return next_window_start

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

    def get_next_reset_time(self, now: datetime) -> datetime:
        """Get the next reset time (next Saturday at 00:00 UTC after 'now')."""
        last_reset = self.get_last_reset_time(now)
        return last_reset + timedelta(days=7)

    def should_reset_bumped_windows(self) -> bool:
        """Check if we need to reset our bumped windows tracking."""
        now = datetime.now(timezone.utc)
        
        # reset tracking at the start of a new day (UTC)
        day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        
        # If the last bump check was before the start of today, reset
        if self._last_bump_check and self._last_bump_check < day_start:
            return True
            
        return False

    # --- bump status and actions ---
    
    def extract_csrf_token(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract CSRF token from the meta tag in the page."""
        csrf_meta = soup.find('meta', {'name': 'csrf-token'})
        if csrf_meta:
            return csrf_meta.get('content')
        return None

    async def check_bump_status(self) -> Dict:
        """Check server bump status by accessing the server manager page."""
        if not self.session or self.session.closed:
            if not await self.init_session():
                return {'success': False, 'error': 'Session initialization failed'}

        try:
            success, html, headers = await self._make_request(
                'GET',
                f"{self.base_url}/dashboard",
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
                'current_window': self.get_current_window(),
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
                    # record the bump in our window tracking
                    current_window = self.get_current_window()
                    self.current_bumped_windows.add(current_window)
                    
                    # record in history
                    now = datetime.now(timezone.utc)
                    self.bump_history.append({
                        'timestamp': now.isoformat(),
                        'window': current_window,
                        'success': True
                    })
                    
                    logging.info(f"Server bumped successfully in window {current_window}")
                    return {'success': True, 'message': 'Server bumped successfully', 'window': current_window}
                else:
                    error_message = response_data.get('message', 'Unknown error')
                    logging.error(f"Bump failed: {error_message}")
                    
                    # record failure in history
                    now = datetime.now(timezone.utc)
                    self.bump_history.append({
                        'timestamp': now.isoformat(),
                        'window': self.get_current_window(),
                        'success': False,
                        'error': error_message
                    })
                    
                    return {'success': False, 'error': error_message}
            else:
                error_message = response_headers.get('error', 'Unknown error')
                logging.error(f"Bump request failed: {error_message}")
                return {'success': False, 'error': error_message}

        except Exception as e:
            logging.exception("Error performing bump")
            return {'success': False, 'error': str(e)}

    # --- main auto-bump loop ---
    
    async def bump_loop(self):
        """Main auto-bump loop that implements the window-based bumping strategy."""
        self.running = True
        
        # initialize session
        if not await self.init_session():
            logging.error("Failed to initialize session. Check your credentials.")
            return
            
        logging.info(f"Starting Discord.me auto-bumper for server EID: {self.server_eid}")
        logging.info(f"Check interval: {self.check_interval} seconds")
        logging.info(f"Bump windows (UTC): {self.bump_windows}")
        
        try:
            while self.running:
                try:
                    now = datetime.now(timezone.utc)
                    current_window = self.get_current_window(now)
                    
                    # reset window tracking if needed (new day)
                    if self.should_reset_bumped_windows():
                        logging.info("New day detected, resetting bump window tracking")
                        self.current_bumped_windows = set()
                    
                    # check if we've already bumped in this window
                    if current_window in self.current_bumped_windows:
                        next_window_start = self.get_next_window_start(now)
                        wait_time = (next_window_start - now).total_seconds()
                        
                        logging.info(f"Already bumped in window {current_window}. "
                                    f"Next window {(current_window + 1) % 4} starts in {wait_time:.0f} seconds "
                                    f"at {next_window_start.isoformat()}")
                        
                        # wait until next window (minus a bit to ensure we're ready)
                        wait_time = min(wait_time, self.check_interval)
                        await asyncio.sleep(max(wait_time, 60))  # at least 60 seconds
                        continue
                    
                    # check bump status
                    status = await self.check_bump_status()
                    
                    if status['success']:
                        bump_info = status['data']
                        
                        if bump_info['can_bump']:
                            logging.info(f"Server can be bumped in window {current_window}! Attempting bump...")
                            
                            # perform the bump
                            bump_result = await self.perform_bump()
                            
                            if bump_result['success']:
                                points = bump_info.get('points', 'unknown')
                                logging.info(f"[{now.isoformat()}] Bump successful in window {current_window}! "
                                           f"Points: {points}")
                                
                                # after successful bump, wait a bit longer
                                await asyncio.sleep(self.check_interval * 2)
                                continue
                            else:
                                logging.error(f"Bump failed: {bump_result.get('error')}")
                        else:
                            next_bump = bump_info.get('next_bump_time', 'unknown')
                            time_until = bump_info.get('time_until_next_bump', 'unknown')
                            
                            if isinstance(time_until, int) and time_until > 0:
                                logging.info(f"Next bump available in {time_until} seconds ({next_bump})")
                                
                                sleep_time = min(time_until - 30, self.check_interval)
                                if sleep_time > 0:
                                    await asyncio.sleep(sleep_time)
                                    continue
                            else:
                                logging.info(f"Cannot bump now. Next bump time: {next_bump}")
                    else:
                        logging.error(f"Failed to check bump status: {status.get('error')}")
                        
                        # if we failed to check status, try to re-initialize the session
                        await self.init_session()
                    
                except Exception as e:
                    logging.exception(f"Error in bump loop: {str(e)}")
                
                # default wait before checking again
                await asyncio.sleep(self.check_interval)
                
        finally:
            # clean up when stopping
            await self.close()
            self.running = False
    
    async def close(self):
        """Clean up resources."""
        if self.session and not self.session.closed:
            await self.session.close()
            logging.info("Session closed")

    async def stop(self):
        """Stop the auto-bumper gracefully."""
        self.running = False
        await self.close()
        logging.info("Auto-bumper stopped")
        
    def get_statistics(self) -> Dict:
        """Get statistics about bumping performance."""
        now = datetime.now(timezone.utc)
        last_reset = self.get_last_reset_time(now)
        next_reset = self.get_next_reset_time(now)
        
        # filter bumps since last reset
        bumps_since_reset = [b for b in self.bump_history 
                            if datetime.fromisoformat(b['timestamp']) >= last_reset]
        
        # count successful bumps per window
        window_counts = [0, 0, 0, 0]
        for bump in bumps_since_reset:
            if bump['success'] and 0 <= bump['window'] < 4:
                window_counts[bump['window']] += 1
        
        return {
            'total_bumps_since_reset': len([b for b in bumps_since_reset if b['success']]),
            'bumps_per_window': window_counts,
            'last_reset': last_reset.isoformat(),
            'next_reset': next_reset.isoformat(),
            'current_window': self.get_current_window(),
            'bumped_windows_today': list(self.current_bumped_windows),
        }

# --- main entry point ---

async def main():
    # configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler("discord_me_bumper.log"),
            logging.StreamHandler()
        ]
    )
    
    # load environment variables
    load_dotenv()
    
    # get server EID from environment variable
    server_eid = os.getenv("SERVER_EID")
    if not server_eid:
        logging.error("SERVER_EID environment variable not set")
        return
        
    # check for required environment variables
    if not os.getenv("DISCORD_ME_SESSION"):
        logging.error("DISCORD_ME_SESSION environment variable not set")
        return
    
    # create and start auto-bumper
    # check every 5 minutes by default
    check_interval = int(os.getenv("CHECK_INTERVAL", "300"))
    bumper = DiscordMeAutoBumper(server_eid, check_interval=check_interval)
    
    try:
        # run the bump loop
        await bumper.bump_loop()
    except KeyboardInterrupt:
        logging.info("Keyboard interrupt received, shutting down...")
    except Exception as e:
        logging.exception(f"Unexpected error: {str(e)}")
    finally:
        await bumper.stop()

if __name__ == "__main__":
    asyncio.run(main())
