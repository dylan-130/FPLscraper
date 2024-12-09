import asyncio
import aiohttp
import pandas as pd
import ssl
import certifi
import json
import logging
import random
import math

# Configure logging
logging.basicConfig(
    level=logging.INFO,  # Set logging level to INFO
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# Confirm script start
print("Script started.", flush=True)
logger.info("Script started.")

# File paths
input_json = "player_data.json"    # Input JSON file
output_json = "player_leagues.json"  # Output JSON file

# Asynchronous function to fetch league data for a player
async def fetch_league_data(session, player_id, semaphore, retries=5):
    url = f"https://fantasy.premierleague.com/api/entry/{player_id}/"
    attempt = 1
    while attempt <= retries:
        try:
            await asyncio.sleep(random.uniform(0.1, 0.5))  # Random delay to spread out requests
            logger.debug(f"Attempt {attempt} for Player ID {player_id}")
            async with semaphore:
                timeout = aiohttp.ClientTimeout(total=30)  # Set total timeout to 30 seconds
                try:
                    async with session.get(url, timeout=timeout) as response:
                        if response.status == 429:
                            retry_after = int(response.headers.get('Retry-After', 60))
                            logger.warning(f"Rate limit hit for Player ID {player_id}. Waiting {retry_after} seconds.")
                            await asyncio.sleep(retry_after)
                            continue  # Retry the same attempt
                        response.raise_for_status()
                        data = await response.json()
                        leagues = data.get("leagues", {}).get("classic", [])
                        logger.info(f"Successfully fetched data for Player ID: {player_id}")
                        return leagues
                except asyncio.TimeoutError:
                    logger.warning(f"Timeout error on attempt {attempt} for Player ID {player_id}")
                    raise  # Re-raise to be caught by the outer except block
        except Exception as e:
            logger.warning(f"Attempt {attempt} failed for Player ID {player_id}: {e}")
            if attempt < retries:
                backoff_time = 2 ** attempt + random.uniform(0, 0.5)
                logger.info(f"Retrying after {backoff_time:.2f} seconds.")
                await asyncio.sleep(backoff_time)
            else:
                logger.error(f"All attempts failed for Player ID {player_id}")
                return []
            attempt += 1  # Increment attempt counter after a failed attempt

# Main asynchronous function to process all players
async def process_players(input_json, output_json, concurrency_limit=50, batch_size=1000):
    logger.info("Beginning to process players.")
    try:
        # Read the input JSON
        logger.info(f"Reading player data from {input_json}")
        df = pd.read_json(input_json)
        df.reset_index(drop=True, inplace=True)
        total_players = len(df)
        logger.info(f"Loaded {total_players} players from {input_json}")

        # Set up SSL context
        ssl_context = ssl.create_default_context(cafile=certifi.where())

        # Create an aiohttp session with unlimited connections
        logger.info("Setting up aiohttp client session")
        connector = aiohttp.TCPConnector(ssl=ssl_context, limit=0)
        async with aiohttp.ClientSession(connector=connector) as session:
            semaphore = asyncio.Semaphore(concurrency_limit)

            output_data = []
            total_batches = math.ceil(total_players / batch_size)
            logger.info(f"Processing data in {total_batches} batches of {batch_size} players each.")

            for batch_num in range(total_batches):
                start_idx = batch_num * batch_size
                end_idx = min(start_idx + batch_size, total_players)
                batch_df = df.iloc[start_idx:end_idx]
                logger.info(f"Processing batch {batch_num + 1}/{total_batches}, players {start_idx + 1} to {end_idx}.")

                tasks = []
                player_data_list = []

                # Create tasks for the current batch
                for idx, row in batch_df.iterrows():
                    player_name = row["Player Name"]
                    player_id = row["Player ID"]
                    task = asyncio.create_task(fetch_league_data(session, player_id, semaphore))
                    tasks.append(task)
                    player_data_list.append({"Player Name": player_name, "Player ID": player_id})
                    logger.debug(f"Queued task for Player ID: {player_id}")

                # Run tasks concurrently for the current batch
                logger.info(f"Running tasks for batch {batch_num + 1}/{total_batches}.")
                results = await asyncio.gather(*tasks)
                logger.info(f"Completed batch {batch_num + 1}/{total_batches}.")

                # Process results for the current batch
                for idx, leagues in enumerate(results):
                    player_name = player_data_list[idx]["Player Name"]
                    player_id = player_data_list[idx]["Player ID"]
                    if leagues:
                        for league in leagues:
                            output_data.append({
                                "Player Name": player_name,
                                "Player ID": player_id,
                                "League ID": league["id"],
                                "League Name": league["name"]
                            })
                        logger.info(f"Processed data for Player ID: {player_id}")
                    else:
                        logger.warning(f"No data for Player ID: {player_id}")

                # Optionally save interim results after each batch
                # with open(output_json, 'w') as json_file:
                #     json.dump(output_data, json_file, indent=4)

            # Save to JSON after processing all batches
            logger.info(f"Saving league data to {output_json}")
            with open(output_json, 'w') as json_file:
                json.dump(output_data, json_file, indent=4)
            logger.info(f"League data collected and saved to {output_json}")

    except Exception as e:
        logger.exception(f"An error occurred in process_players: {e}")
        print(f"An exception occurred in process_players: {e}", flush=True)

# Run the asyncio loop
if __name__ == "__main__":
    logger.info("Starting leagues.py script")
    try:
        asyncio.run(process_players(input_json, output_json, concurrency_limit=50, batch_size=1000))
    except Exception as e:
        logger.exception(f"An error occurred while running the script: {e}")
        print(f"An exception occurred: {e}", flush=True)

# Confirm script end
