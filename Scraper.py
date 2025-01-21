import asyncio
import aiohttp
import json
import ssl
import certifi
import sys
import time
import logging

# Configure logging to stderr with INFO level
logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s', stream=sys.stderr)
logger = logging.getLogger(__name__)

league_id = 314
max_concurrent_requests = 50
total_pages = 214849
failed_attempts = []
output_file = 'player_data.json'

ssl_context = ssl.create_default_context(cafile=certifi.where())
progress_interval = 10000

async def fetch_page(session, page, semaphore, f):
    url = f'https://fantasy.premierleague.com/api/leagues-classic/{league_id}/standings/?page_standings={page}'
    max_attempts = 5

    for attempt in range(max_attempts):
        async with semaphore:
            msg = f"Fetching page {page} (Attempt {attempt+1}/{max_attempts})..."
            print("[INFO]", msg, flush=True)
            logger.info(msg)

            start = time.time()
            try:
                async with session.get(url, ssl=ssl_context) as response:
                    if response.status == 429:
                        wait_time = 5 * (2 ** attempt)
                        msg = f"Rate limit hit on page {page}. Waiting {wait_time}s before retrying..."
                        print("[WARN]", msg, flush=True)
                        logger.warning(msg)
                        await asyncio.sleep(wait_time)
                        continue

                    if not (200 <= response.status < 300):
                        msg = f"Page {page}: HTTP {response.status}. Retrying..."
                        print("[ERROR]", msg, flush=True)
                        logger.error(msg)
                        await asyncio.sleep(2 ** attempt)
                        continue

                    data = await response.json()
                    if 'standings' in data and 'results' in data['standings']:
                        results = data['standings']['results']
                        for player in results:
                            player_obj = {
                                'Full Name': player['player_name'],
                                'Team Name': player['entry_name'],
                                'Player ID': player['entry']
                            }
                            f.write(json.dumps(player_obj, ensure_ascii=False) + '\n')
                            f.flush()

                        duration = time.time() - start
                        msg = f"Page {page} fetched successfully with {len(results)} players in {duration:.2f}s."
                        print("[INFO]", msg, flush=True)
                        logger.info(msg)
                        return
                    else:
                        msg = f"Page {page}: Expected keys not found in the response."
                        print("[ERROR]", msg, flush=True)
                        logger.error(msg)
                        failed_attempts.append(page)
                        return

            except aiohttp.ClientResponseError as cre:
                msg = (f"ClientResponseError on page {page}, status {cre.status}. "
                       f"Attempt {attempt+1}/{max_attempts}")
                print("[ERROR]", msg, flush=True)
                logger.error(msg)
                if 500 <= cre.status < 600:
                    wait_time = (2 ** attempt)
                    wmsg = f"Server error on page {page}. Waiting {wait_time}s before retry..."
                    print("[WARN]", wmsg, flush=True)
                    logger.warning(wmsg)
                    await asyncio.sleep(wait_time)
                else:
                    await asyncio.sleep(2)
                    if attempt == max_attempts - 1:
                        failed_attempts.append(page)
                        return

            except (aiohttp.ClientConnectionError, aiohttp.ClientOSError) as e:
                wait_time = (2 ** attempt)
                msg = f"Connection error on page {page}: {e}. Waiting {wait_time}s before retry..."
                print("[ERROR]", msg, flush=True)
                logger.error(msg)
                await asyncio.sleep(wait_time)

            except asyncio.CancelledError:
                msg = "Task was cancelled."
                print("[ERROR]", msg, flush=True)
                logger.error(msg)
                raise

            except Exception as e:
                wait_time = (2 ** attempt)
                msg = f"Unexpected error on page {page}: {e}. Waiting {wait_time}s before retry..."
                print("[ERROR]", msg, flush=True)
                logger.error(msg)
                await asyncio.sleep(wait_time)

    msg = f"All attempts failed for page {page}."
    print("[ERROR]", msg, flush=True)
    logger.error(msg)
    failed_attempts.append(page)


async def main():
    msg = (f"Starting scraper.\n"
           f"League ID: {league_id}, Total Pages: {total_pages}, Concurrency: {max_concurrent_requests}\n"
           f"Output file: {output_file}")
    print("[INFO]", msg, flush=True)
    logger.info(msg)

    start_time = time.time()

    semaphore = asyncio.Semaphore(max_concurrent_requests)
    connector = aiohttp.TCPConnector(limit_per_host=max_concurrent_requests, ssl=ssl_context)

    async with aiohttp.ClientSession(connector=connector) as session:
        with open(output_file, 'w', encoding='utf-8') as f:
            tasks = []
            for page in range(1, total_pages + 1):
                task = asyncio.create_task(fetch_page(session, page, semaphore, f))
                tasks.append(task)

                if page % progress_interval == 0:
                    pmsg = f"Scheduled {page}/{total_pages} pages so far..."
                    print("[INFO]", pmsg, flush=True)
                    logger.info(pmsg)

            print("[INFO] All fetch tasks scheduled, now awaiting completion...", flush=True)
            logger.info("All fetch tasks scheduled, now awaiting completion...")
            results = await asyncio.gather(*tasks, return_exceptions=True)

    elapsed = time.time() - start_time
    failed_count = len(failed_attempts)
    success_count = total_pages - failed_count

    print("[INFO] Data fetching complete.", flush=True)
    logger.info("Data fetching complete.")
    print(f"[INFO] Total time: {elapsed:.2f}s", flush=True)
    logger.info(f"Total time: {elapsed:.2f}s")
    print(f"[INFO] Succeeded: {success_count}, Failed: {failed_count}", flush=True)
    logger.info(f"Succeeded: {success_count}, Failed: {failed_count}")

    with open('failed_attempts.json', 'w', encoding='utf-8') as f_fail:
        json.dump({'Failed Pages': failed_attempts}, f_fail, ensure_ascii=False)

    if failed_count > 0:
        msg = "Some pages failed. See failed_attempts.json for details."
        print("[WARN]", msg, flush=True)
        logger.warning(msg)
    else:
        msg = "No failed pages!"
        print("[INFO]", msg, flush=True)
        logger.info(msg)

    print("[INFO] Scraper finished.", flush=True)
    logger.info("Scraper finished.")


if __name__ == "__main__":
    # Force unbuffered output by setting PYTHONUNBUFFERED=1 or using python -u
    # Also ensure that you're running in a real terminal, not a silent environment.
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("[ERROR] Script interrupted by user.", flush=True)
        logger.error("Script interrupted by user.")
        sys.exit(1)
    except Exception as e:
        print("[ERROR] Unhandled exception at top-level:", e, flush=True)
        logger.error(f"Unhandled exception at top-level: {e}")
        sys.exit(1)
