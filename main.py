import os
import re
import math
import time
import json
import base64
import signal
import asyncio
import logging
import aiohttp
import urllib.parse
from datetime import datetime
from motor.motor_asyncio import AsyncIOMotorClient
from aiohttp import web, ClientConnectionError, ClientTimeout
from dotenv import load_dotenv
from pyrogram import Client, filters, enums
from pyrogram.errors import FloodWait, UserNotParticipant, AuthBytesInvalid, PeerIdInvalid, LimitInvalid, Timeout, FileReferenceExpired
from pyrogram.types import Message, InlineKeyboardButton, InlineKeyboardMarkup, CallbackQuery
from pyrogram.session import Session, Auth
from pyrogram.file_id import FileId, FileType
from pyrogram import raw
from pyrogram.raw.types import InputPhotoFileLocation, InputDocumentFileLocation # Fixed: Missing imports added back

# -------------------------------------------------------------------------------- #
# KeralaCaptain Bot - Ultimate Streaming Fix V3.3 (Recursion Fix)                 #
# Fixed: Infinite loop in get_location; non-recursive guard                       #
# -------------------------------------------------------------------------------- #

# Load configurations from .env file
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='[%(asctime)s - %(levelname)s] - %(message)s')
LOGGER = logging.getLogger(__name__)
logging.getLogger("pyrogram").setLevel(logging.WARNING)
logging.getLogger("aiohttp.web").setLevel(logging.ERROR)

class Config:
    API_ID = int(os.environ.get("API_ID", 0))
    API_HASH = os.environ.get("API_HASH", "")
    BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
    BACKUP_BOT_TOKEN = os.environ.get("BACKUP_BOT_TOKEN", "") # For backup forwarding/streaming
    OWNER_ID = int(os.environ.get("OWNER_ID", 0))

    WP_URL = os.environ.get("WP_URL", "")
    WP_USER = os.environ.get("WP_USER", "")
    WP_PASSWORD = os.environ.get("WP_PASSWORD", "")
    WP_API_KEY = os.environ.get("WP_API_KEY", "")

    MONGO_URI = os.environ.get("MONGO_URI", "")
    LOG_CHANNEL_ID = int(os.environ.get("LOG_CHANNEL_ID", 0))
    STREAM_URL = os.environ.get("STREAM_URL", "").rstrip('/')
    PORT = int(os.environ.get("PORT", 8080))
    
    TMDB_API_KEY = os.environ.get("TMDB_API_KEY", "")
    PING_INTERVAL = int(os.environ.get("PING_INTERVAL", 1200))
    ON_HEROKU = 'DYNO' in os.environ
    # --- ADD THIS NEW LINE ---
    BOT_ROLE = os.environ.get("BOT_ROLE", "SLAVE") # Default role is SLAVE

# --- VALIDATE ESSENTIAL CONFIGURATIONS (ROLE-BASED) ---
# Check for variables based on the bot's role

if Config.BOT_ROLE.upper() == 'MASTER':
    # Master bot needs all variables for admin tasks
    required_vars = [
        Config.API_ID, Config.API_HASH, Config.BOT_TOKEN, Config.OWNER_ID,
        Config.MONGO_URI, Config.LOG_CHANNEL_ID, Config.STREAM_URL,
        Config.WP_URL, Config.WP_USER, Config.WP_PASSWORD, Config.TMDB_API_KEY
    ]
    if not all(required_vars):
        LOGGER.critical("FATAL: One or more required variables are missing for MASTER bot. Cannot start.")
        exit(1)
else: # This logic is for SLAVE/WORKER bots
    # Worker bots only need a smaller set of variables to stream
    required_vars = [
        Config.API_ID, Config.API_HASH, Config.BOT_TOKEN,
        Config.MONGO_URI, Config.LOG_CHANNEL_ID, Config.STREAM_URL
    ]
    if not all(required_vars):
        LOGGER.critical("FATAL: One or more required variables are missing for WORKER bot. Cannot start.")
        exit(1)

# --- END OF VALIDATION ---

# -------------------------------------------------------------------------------- #
# HELPER FUNCTIONS & CLASSES                                                     #
# -------------------------------------------------------------------------------- #

# --- Base64 Encoding/Decoding for Stream URLs ---
async def encode(string: str) -> str:
    string_bytes = string.encode("ascii")
    base64_bytes = base64.urlsafe_b64encode(string_bytes)
    return (base64_bytes.decode("ascii")).strip("=")

async def decode(base64_string: str) -> str:
    base64_string = base64_string.strip("=")
    base64_bytes = (base64_string + "=" * (-len(base64_string) % 4)).encode("ascii")
    string_bytes = base64.urlsafe_b64decode(base64_bytes)
    return string_bytes.decode("ascii")

# --- File Parsing ---
def parse_filename(filename):
    clean_name = re.sub(r'[\._\-\(\)\[\]]', ' ', filename)
    year_match = re.search(r'\b(19[89]\d|20\d{2})\b', clean_name)
    year = year_match.group(0) if year_match else None
    query = re.split(r'\b(19[89]\d|20\d{2})\b', clean_name)[0].strip()
    query = re.sub(r'\b(1080p|720p|480p|HD|FHD|WEB[-_]?DL|BluRay|x264|x265|Malayalam|Mal)\b', '', query, flags=re.I).strip()
    language = "Malayalam" if re.search(r'\b(malayalam|mal)\b', clean_name, re.I) else None
    return query, year, language

def get_file_quality(filename):
    filename_lower = filename.lower()
    if "1080p" in filename_lower: return "1080p"
    if "720p" in filename_lower: return "720p"
    if "480p" in filename_lower: return "480p"
    if "240p" in filename_lower: return "240p"
    return "SD"

def humanbytes(size):
    if not size: return "0 B"
    power = 1024
    n = 0
    power_labels = {0: ' ', 1: 'K', 2: 'M', 3: 'G', 4: 'T'}
    while size > power:
        size /= power
        n += 1
    return f"{round(size, 2)} {power_labels[n]}B"

# -------------------------------------------------------------------------------- #
# THIRD-PARTY API INTEGRATION                                                    #
# -------------------------------------------------------------------------------- #

class WordPressAPI:
    """Handles communication with the WordPress site via a custom REST API."""
    def __init__(self, url, user, password, api_key):
        self.api_url = f"{url.rstrip('/')}/wp-json/keralacaptain/v1"
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36'
        }

        self._session = aiohttp.ClientSession(
            auth=aiohttp.BasicAuth(user, password),
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=60) # Upgraded: Longer timeout for scale
        )

        if api_key:
            self._session.headers.update({'X-API-KEY': api_key})
        LOGGER.info("WordPressAPI client initialized with a custom User-Agent.")

    async def _make_request(self, method, endpoint, **kwargs):
        url = f"{self.api_url}/{endpoint}"
        try:
            async with self._session.request(method, url, timeout=60, **kwargs) as res: # Upgraded timeout
                res_text = await res.text() 
                LOGGER.info(f"WordPress API Request to {url} returned status {res.status}. Response: {res_text[:500]}")
                res.raise_for_status()
                
                if res.status in [200, 201]:
                    try:
                        json_response = json.loads(res_text)
                        return json_response if json_response is not None else {"status": "success_no_json"}
                    except (json.JSONDecodeError, aiohttp.ContentTypeError):
                        return {"status": "success_no_content", "message": "Operation successful with no JSON body."}
                
                if res.status == 204:
                    return {"status": "success", "message": "Operation successful with no content."}

                return {}
                
        except aiohttp.ClientResponseError as e:
            error_text = ""
            try:
                error_text = await e.text()
            except Exception:
                pass
            LOGGER.error(f"WordPress API Error ({method} {url}): Status={e.status}, Message='{e.message}', Response='{error_text}'")
            return {"status": "error", "message": f"HTTP Error: {e.status} - {e.message}", "details": error_text}
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            LOGGER.error(f"WordPress API Connection Error ({method} {url}): {e}")
            return {"status": "error", "message": f"Connection Error: {e}"}
            
    async def post_content(self, data: dict):
        payload = {
            'title': data.get('title'),
            'content': data.get('overview'),
            'status': 'publish',
            'post_type': data.get('type', 'movie'),
            'stream_link': data.get('stream_link'),
            'year': data.get('release_year'),
            'rating': data.get('rating'),
            'duration': data.get('duration'),
            'poster_url': data.get('poster_url'),
            'language': data.get('language'),
            'tmdb_id': data.get('tmdb_id'),
            'trailer_url': data.get('trailer_url'),
            'qualities': data.get('qualities', {}),
            'genres': data.get('genres', []),
            'cast': data.get('cast', [])
        }
        return await self._make_request('POST', 'create', json=payload)

    async def update_stream_link_and_qualities(self, post_id: int, new_link: str, new_qualities: dict):
        payload = {'stream_link': new_link, 'qualities': new_qualities}
        return await self._make_request('POST', f'updatelink/{post_id}', json=payload)
            
    async def delete_content(self, post_id: int):
        return await self._make_request('DELETE', f'delete/{post_id}')
            
    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

class TMDbAPI:
    """Handles all API requests to The Movie Database (TMDB)."""
    BASE_URL = "https://api.themoviedb.org/3"

    def __init__(self):
        self.semaphore = asyncio.Semaphore(4) # New: Rate limit to 4 concurrent (under 40/10s)

    async def _get(self, endpoint, params=None):
        async with self.semaphore: # New: Acquire semaphore for rate limiting
            params = params or {}
            params['api_key'] = Config.TMDB_API_KEY
            url = f"{self.BASE_URL}/{endpoint}"
            async with aiohttp.ClientSession(timeout=ClientTimeout(total=30)) as session: # Upgraded timeout
                try:
                    async with session.get(url, params=params) as response:
                        response.raise_for_status()
                        data = await response.json()
                        if 'errors' in data: # Backoff on rate errors
                            LOGGER.warning(f"TMDB rate limit hit: {data['errors']}. Retrying in 10s.")
                            await asyncio.sleep(10)
                            return await self._get(endpoint, params) # Retry once
                        return data
                except aiohttp.ClientError as e:
                    LOGGER.error(f"TMDB API request failed for {url}: {e}")
                    return None

    async def search(self, query, year=None, content_type='movie'):
        params = {'query': query}
        endpoint = f"search/{content_type}"
        if year:
            key = "primary_release_year" if content_type == 'movie' else "first_air_date_year"
            params[key] = year
        data = await self._get(endpoint, params)
        return data.get('results', []) if data else []

    async def get_details(self, tmdb_id, content_type='movie'):
        params = {'append_to_response': 'credits,videos'}
        return await self._get(f"{content_type}/{tmdb_id}", params)

# -------------------------------------------------------------------------------- #
# DATABASE OPERATIONS                                                            #
# -------------------------------------------------------------------------------- #

# ================================================================================ #
# DATABASE OPERATIONS (v2 with Backup Collection)
# ================================================================================ #

# --- Database and Collection Setup ---
db_client = AsyncIOMotorClient(Config.MONGO_URI)
db = db_client['BotCDatabase']

# Primary collection for all normal operations
media_collection = db['media']

# New: A separate collection that acts as a live backup
media_backup_collection = db['media_backup'] 

# This collection for user conversations does not need a backup
user_conversations_col = db['conversations']


# --- Database Functions ---

async def check_duplicate(tmdb_id):
    """Checks for duplicates only in the main collection."""
    return await media_collection.find_one({"tmdb_id": tmdb_id})


async def add_media_to_db(data):
    """
    CHANGED: Inserts new media data into both the main and backup collections.
    """
    await media_collection.insert_one(data)
    await media_backup_collection.insert_one(data) # Also write to the backup


async def get_media_by_post_id(post_id: int):
    """Reads media data only from the main collection for regular use."""
    return await media_collection.find_one({"wp_post_id": post_id})


async def update_media_links_in_db(post_id: int, new_message_ids: dict, new_stream_link: str):
    """
    CHANGED: Updates links in both the main and backup collections.
    """
    update_query = {
        "$set": {"message_ids": new_message_ids, "stream_link": new_stream_link}
    }
    await media_collection.update_one({"wp_post_id": post_id}, update_query)
    await media_backup_collection.update_one({"wp_post_id": post_id}, update_query) # Also update the backup


async def delete_media_from_db(post_id: int):
    """
    CHANGED: Deletes media data from both the main and backup collections.
    """
    # Perform deletion on both collections to keep them in sync
    result_main = await media_collection.delete_one({"wp_post_id": post_id})
    await media_backup_collection.delete_one({"wp_post_id": post_id}) # Also delete from the backup
    
    # Return the result from the main operation as before
    return result_main


async def get_stats():
    """Calculates stats based only on the main collection."""
    movies_count = await media_collection.count_documents({"type": "movie"})
    series_count = await media_collection.count_documents({"type": "series"})
    return movies_count, series_count


async def get_all_media_for_library(page: int = 0, limit: int = 10):
    """Fetches the library list only from the main collection."""
    cursor = media_collection.find().sort("added_at", -1).skip(page * limit).limit(limit)
    return await cursor.to_list(length=limit)


async def get_user_conversation(chat_id):
    """Manages user conversation state (no changes needed)."""
    return await user_conversations_col.find_one({"_id": chat_id})


async def update_user_conversation(chat_id, data):
    """Manages user conversation state (no changes needed)."""
    if data:
        await user_conversations_col.update_one({"_id": chat_id}, {"$set": data}, upsert=True)
    else:
        await user_conversations_col.delete_one({"_id": chat_id})


async def get_post_id_from_msg_id(msg_id: int):
    """
    Helper for stream refreshing. Reads only from the main collection.
    """
    doc = await media_collection.find_one({"message_ids": {"$in": [msg_id]}})
    return doc['wp_post_id'] if doc else None

# ================================================================================ #
# END OF DATABASE OPERATIONS SECTION
# ================================================================================ #

# -------------------------------------------------------------------------------- #
# STREAMING ENGINE & WEB SERVER                                                  #
# -------------------------------------------------------------------------------- #

multi_clients = {}
work_loads = {}
class_cache = {}
processed_media_groups = {} # Upgraded: Dict for per-user {chat_id: {media_group_id: True}}
next_client_idx = 0 # New: For round-robin on ties
stream_errors = 0 # New: Global error counter
last_error_reset = time.time() # New: For periodic reset

# Upgraded ByteStreamer with import guard
class ByteStreamer:
    def __init__(self, client: Client):
        self.client: Client = client
        self.cached_file_ids = {} # Cache for file properties
        self.session_cache = {} # New: {dc_id: (session, timestamp)} for TTL
        asyncio.create_task(self.clean_cache_regularly())

    async def clean_cache_regularly(self):
        # Upgraded: Every 20min for high traffic
        while True:
            await asyncio.sleep(1200)
            self.cached_file_ids.clear()
            self.session_cache.clear() # New: Evict stale sessions
            LOGGER.info("Cleared ByteStreamer's cached file properties and sessions.")

    async def get_file_properties(self, message_id: int):
        if message_id in self.cached_file_ids:
            return self.cached_file_ids[message_id]

        message = await self.client.get_messages(Config.LOG_CHANNEL_ID, message_id)
        if not message or message.empty or not (message.document or message.video):
            raise FileNotFoundError

        media = message.document or message.video
        file_id = FileId.decode(media.file_id)
        setattr(file_id, "file_size", media.file_size or 0)
        setattr(file_id, "mime_type", media.mime_type or "video/mp4")
        setattr(file_id, "file_name", media.file_name or "Unknown.mp4")

        self.cached_file_ids[message_id] = file_id
        return file_id

    async def generate_media_session(self, file_id: FileId) -> Session:
        media_session = self.client.media_sessions.get(file_id.dc_id)
        dc_id = file_id.dc_id

        # New: TTL check before ping
        if dc_id in self.session_cache:
            session, ts = self.session_cache[dc_id]
            if time.time() - ts < 300: # 5min TTL
                LOGGER.debug(f"Reusing TTL-cached media session for DC {dc_id}")
                return session

        # Existing ping logic as fallback
        if media_session:
            try:
                await media_session.send(raw.functions.help.GetConfig(), timeout=10) # Bumped timeout
                self.session_cache[dc_id] = (media_session, time.time()) # Cache on success
                LOGGER.debug(f"Reusing pinged media session for DC {dc_id}")
                return media_session
            except Exception as e:
                LOGGER.warning(f"Existing media session for DC {dc_id} is stale: {e}. Recreating.")
                try:
                    await media_session.stop()
                except: pass
                if dc_id in self.client.media_sessions:
                    del self.client.media_sessions[dc_id]
                media_session = None

        LOGGER.info(f"Creating new media session for DC {dc_id}")
        if dc_id != await self.client.storage.dc_id():
            media_session = Session(self.client, dc_id, await Auth(self.client, dc_id, await self.client.storage.test_mode()).create(), await self.client.storage.test_mode(), is_media=True)
            await media_session.start()
            for i in range(3):
                try:
                    exported_auth = await self.client.invoke(raw.functions.auth.ExportAuthorization(dc_id=dc_id))
                    await media_session.send(raw.functions.auth.ImportAuthorization(id=exported_auth.id, bytes=exported_auth.bytes))
                    break
                except AuthBytesInvalid as e:
                    LOGGER.warning(f"AuthBytesInvalid on attempt {i+1}: {e}")
                    if i == 2: raise
                    await asyncio.sleep(1)
        else:
            media_session = Session(self.client, dc_id, await self.client.storage.auth_key(), await self.client.storage.test_mode(), is_media=True)
            await media_session.start()

        self.client.media_sessions[dc_id] = media_session
        self.session_cache[dc_id] = (media_session, time.time()) # Cache new
        return media_session

    @staticmethod
    def get_location(file_id: FileId):
        # FIX: Removed the problematic try...except NameError block
        # The raw types (InputPhotoFileLocation, InputDocumentFileLocation) are
        # now imported at the top of the file, so they should always be available.
        # This prevents the recursion bug.
        if file_id.file_type == FileType.PHOTO:
            return InputPhotoFileLocation(id=file_id.media_id, access_hash=file_id.access_hash, file_reference=file_id.file_reference, thumb_size=file_id.thumbnail_size)
        else:
            return InputDocumentFileLocation(id=file_id.media_id, access_hash=file_id.access_hash, file_reference=file_id.file_reference, thumb_size=file_id.thumbnail_size)


    async def yield_file(self, file_id: FileId, offset: int, chunk_size: int, message_id: int):
        media_session = await self.generate_media_session(file_id)
        location = self.get_location(file_id)

        current_offset = offset
        retry_count = 0
        max_retries = 3

        while True:
            try:
                chunk = await media_session.send(
                    raw.functions.upload.GetFile(location=location, offset=current_offset, limit=chunk_size),
                    timeout=30 # Upgraded timeout
                )
                
                if isinstance(chunk, raw.types.upload.File) and chunk.bytes:
                    yield chunk.bytes
                    if len(chunk.bytes) < chunk_size:
                        break
                    current_offset += len(chunk.bytes)
                else:
                    break

            except FileReferenceExpired:
                retry_count += 1
                if retry_count > max_retries:
                    raise # Bubble up after retries
                LOGGER.warning(f"FileReferenceExpired for msg {message_id}, retry {retry_count}/{max_retries}. Refreshing...")
                # New: Auto-refresh by re-forwarding
                original_msg = await self.client.get_messages(Config.LOG_CHANNEL_ID, message_id)
                if original_msg:
                    refreshed_msg = await forward_file_safely(original_msg)
                    if refreshed_msg:
                        # Update cache/DB with new ID
                        new_file_id = await self.get_file_properties(refreshed_msg.id)
                        self.cached_file_ids[message_id] = new_file_id # Reuse key for simplicity
                        # Update DB
                        post_id = await get_post_id_from_msg_id(message_id)
                        if post_id:
                            media_doc = await get_media_by_post_id(post_id)
                            if media_doc:
                                # Update message_id in qualities (assume key is quality from filename)
                                old_qualities = media_doc['message_ids']
                                new_qualities = {k: refreshed_msg.id if v == message_id else v for k, v in old_qualities.items()}
                                await update_media_links_in_db(post_id, new_qualities, media_doc['stream_link'])
                        location = self.get_location(new_file_id) # Recreate location
                        await asyncio.sleep(2) # Rate limit
                        continue # Retry fetch
                raise # Failed refresh

            except FloodWait as e:
                LOGGER.warning(f"FloodWait of {e.value} seconds on get_file. Waiting...")
                await asyncio.sleep(e.value)
                continue

routes = web.RouteTableDef()

@routes.get("/", allow_head=True)
async def root_route_handler(request):
    return web.Response(text=f"Welcome to KeralaCaptain's Streaming Service!", content_type='text/html')

# New: Health endpoint
@routes.get("/health")
async def health_handler(request):
    global stream_errors, last_error_reset
    if time.time() - last_error_reset > 60: # New: Reset every min
        stream_errors = 0
        last_error_reset = time.time()
    active_sessions = len(multi_clients)
    cache_size = 0
    if multi_clients:
        sample_client = list(multi_clients.values())[0]
        if sample_client in class_cache:
            cache_size = len(class_cache[sample_client].cached_file_ids)
    return web.json_response({
        "status": "ok",
        "active_clients": active_sessions,
        "cache_size": cache_size,
        "stream_errors_last_min": stream_errors
    })

# New: Favicon
@routes.get("/favicon.ico")
async def favicon_handler(request):
    return web.Response(status=204) # No content

# REMOVED: /watch route as per instruction
# @routes.get("/watch/{message_ids_str}")
# @aiohttp_jinja2.template('player.html')
# async def stream_page_handler(request: web.Request):
#     try:
#         message_ids_str = request.match_info['message_ids_str']
#         message_ids = [int(i) for i in message_ids_str.split('-')]

#         possible_qualities = ["1080p", "720p", "480p", "240p", "SD"]
        
#         search_query = {
#             "$or": [
#                 {f"message_ids.{quality}": {"$in": message_ids}} for quality in possible_qualities
#             ]
#         }
#         media_doc = await media_collection.find_one(search_query)

#         if not media_doc:
#             LOGGER.error(f"Database lookup failed for message_ids: {message_ids}")
#             raise web.HTTPNotFound(text="<h1>404: Content Not Found</h1><p>This link may be old or invalid. The database lookup failed.</p>")

#         stream_qualities = {}
#         for quality, msg_id in media_doc.get('message_ids', {}).items():
#             stream_qualities[quality] = f"{Config.STREAM_URL}/stream/{msg_id}"

#         sorted_stream_qualities = dict(sorted(stream_qualities.items(), key=lambda item: int(re.sub(r'[^0-9]', '', item[0] or '0')), reverse=True))

#         context = {
#             "file_name": media_doc.get("title", "Untitled"),
#             "qualities": sorted_stream_qualities,
#             "download_url": "#download-coming-soon",
#             "site_url": Config.WP_URL,
#             "site_name": "KeralaCaptain",
#             "telegram_channel_url": "https://t.me/KeralaCaptain",
#             "tmdb_id": media_doc.get("tmdb_id"),
#             "wp_url": Config.WP_URL.rstrip('/')
#         }
#         return context
#     except web.HTTPNotFound as e:
#         return e
#     except Exception as e:
#         LOGGER.error(f"Error in stream page handler: {e}", exc_info=True)
#         return web.Response(text="<h1>500: Internal Server Error</h1>", status=500, content_type='text/html')

# Replace your entire stream_handler function with this final, complete version.
@routes.get("/stream/{message_id:\d+}")
async def stream_handler(request: web.Request):
    client_index = None # Initialize client_index to ensure it's available in the finally block
    try:
        # --- SECURITY CHECK 1: Referer Check ---
        referer = request.headers.get('Referer')
        allowed_referer = 'https://XTubePro.xo.je/' # Your WordPress domain

        if not referer or not referer.startswith(allowed_referer):
            LOGGER.warning(f"Blocked hotlink attempt. Referer: {referer}")
            return web.Response(status=403, text="403 Forbidden: Direct access is not allowed.")
            
        # --- END OF SECURITY CHECK ---

        message_id = int(request.match_info['message_id'])
        range_header = request.headers.get("Range", 0)

        # This is your excellent load balancing logic
        min_load = min(work_loads.values())
        candidates = [cid for cid, load in work_loads.items() if load == min_load]
        # (Assuming next_client_idx is defined globally)
        global next_client_idx
        if len(candidates) > 1:
            client_index = candidates[next_client_idx % len(candidates)]
            next_client_idx += 1
        else:
            client_index = candidates[0]
            
        faster_client = multi_clients[client_index]
        work_loads[client_index] += 1 # Increase workload

        if faster_client not in class_cache:
            class_cache[faster_client] = ByteStreamer(faster_client)
        tg_connect = class_cache[faster_client]

        file_id = await tg_connect.get_file_properties(message_id)
        file_size = file_id.file_size

        from_bytes = 0
        if range_header:
            from_bytes_str, _ = range_header.replace("bytes=", "").split("-")
            from_bytes = int(from_bytes_str)

        if from_bytes >= file_size:
            return web.Response(status=416, reason="Range Not Satisfiable")

        chunk_size = 1024 * 1024
        offset = from_bytes - (from_bytes % chunk_size)
        first_part_cut = from_bytes - offset
        
        cors_headers = { 'Access-Control-Allow-Origin': allowed_referer }
        
        resp = web.StreamResponse(
            status=206 if range_header else 200,
            headers={
                "Content-Type": file_id.mime_type,
                "Content-Range": f"bytes {from_bytes}-{file_size - 1}/{file_size}",
                "Content-Length": str(file_size - from_bytes),
                "Accept-Ranges": "bytes",
                **cors_headers
            }
        )
        await resp.prepare(request)

        # Your bot has the auto-refresh logic inside yield_file, so we pass message_id
        body_generator = tg_connect.yield_file(file_id, offset, chunk_size, message_id)

        is_first_chunk = True
        async for chunk in body_generator:
            try:
                if is_first_chunk and first_part_cut > 0:
                    await resp.write(chunk[first_part_cut:])
                    is_first_chunk = False
                else:
                    await resp.write(chunk)
            except (ConnectionError, asyncio.CancelledError):
                LOGGER.warning(f"Client disconnected while writing chunk for message {message_id}.")
                return resp

        return resp

    except (FileReferenceExpired, AuthBytesInvalid) as e:
        LOGGER.error(f"FATAL STREAM ERROR for {message_id}: {type(e).__name__}. Client needs to refresh.")
        return web.Response(status=410, text="Stream link expired, please refresh the page.")

    except Exception as e:
        LOGGER.critical(f"Unhandled stream error for {message_id}: {e}", exc_info=True)
        return web.Response(status=500)

    finally:
        # This block is crucial and will always run, ensuring the workload is decremented.
        if client_index is not None:
            work_loads[client_index] -= 1
            LOGGER.debug(f"Decremented workload for client {client_index}. Current workloads: {work_loads}")

async def web_server():
    web_app = web.Application(client_max_size=30_000_000)
    web_app.add_routes(routes)
    return web_app
            
# -------------------------------------------------------------------------------- #
# BOT & CLIENT INITIALIZATION                                                    #
# -------------------------------------------------------------------------------- #

main_bot = Client("KeralaCaptainBot", api_id=Config.API_ID, api_hash=Config.API_HASH, bot_token=Config.BOT_TOKEN)
backup_bot = Client("KeralaCaptainBackupBot", api_id=Config.API_ID, api_hash=Config.API_HASH, bot_token=Config.BACKUP_BOT_TOKEN) if Config.BACKUP_BOT_TOKEN else None

wp_api = None
tmdb_api = None

class TokenParser:
    def parse_from_env(self):
        return {c + 2: t for c, (_, t) in enumerate(filter(lambda n: n[0].startswith("MULTI_TOKEN"), sorted(os.environ.items())))}

async def initialize_clients():
    multi_clients[0] = main_bot
    work_loads[0] = 0
    if backup_bot:
        multi_clients[1] = backup_bot
        work_loads[1] = 0
    
    all_tokens = TokenParser().parse_from_env()
    if not all_tokens:
        LOGGER.info("No additional clients found.")
        return

    async def start_client(client_id, token):
        try:
            client = await Client(name=str(client_id), api_id=Config.API_ID, api_hash=Config.API_HASH, bot_token=token, no_updates=True, in_memory=True).start()
            work_loads[client_id] = 0
            return client_id, client
        except Exception as e:
            LOGGER.error(f"Failed to start Client {client_id}: {e}")
            return None

    clients = await asyncio.gather(*[start_client(i, token) for i, token in all_tokens.items()])
    multi_clients.update({cid: client for cid, client in clients if client is not None})
    
    if len(multi_clients) > 1:
        LOGGER.info(f"Successfully initialized {len(multi_clients)} clients. Multi-Client mode is ON.")

async def forward_file_safely(message_to_forward: Message):
    """
    Sends a file to the log channel using send_cached_media.
    """
    try:
        media = message_to_forward.document or message_to_forward.video
        if not media:
            LOGGER.error("Message has no media to send.")
            return None
            
        file_id = media.file_id
            
        LOGGER.info(f"Sending cached media for message {message_to_forward.id} using main bot...")
        return await main_bot.send_cached_media(
            chat_id=Config.LOG_CHANNEL_ID,
            file_id=file_id,
            caption=getattr(message_to_forward, 'caption', '')
        )
            
    except Exception as e:
        LOGGER.warning(f"Main bot failed to send cached media: {e}. Trying backup bot...")
        if backup_bot:
            try:
                media = message_to_forward.document or message_to_forward.video
                if not media:
                    LOGGER.error("Backup: Message has no media to send.")
                    return None
                    
                file_id = media.file_id

                LOGGER.info(f"Sending cached media for message {message_to_forward.id} using backup bot...")
                return await backup_bot.send_cached_media(
                    chat_id=Config.LOG_CHANNEL_ID,
                    file_id=file_id,
                    caption=getattr(message_to_forward, 'caption', '')
                )
            except Exception as backup_e:
                LOGGER.error(f"Backup bot also failed to send cached media: {backup_e}")
                return None
        else:
            LOGGER.error("Backup bot is not configured, cannot retry.")
            return None

# -------------------------------------------------------------------------------- #
# BOT HANDLERS                                                                   #
# -------------------------------------------------------------------------------- #

@main_bot.on_message(filters.command("start") & filters.private & filters.user(Config.OWNER_ID))
async def start_command(client, message):
    await message.reply_text(
        "**Welcome to the KeralaCaptain Bot!**\n\nThis is your personal dashboard for managing content on your website.",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("➕ Add Movie", callback_data="add_movie")],
            [InlineKeyboardButton("📺 Add Series", callback_data="add_series")],
            [InlineKeyboardButton("⚙️ Admin Panel", callback_data="apanel_start")]
        ])
    )
    await update_user_conversation(message.chat.id, None)

@main_bot.on_callback_query(filters.regex("^main_menu$"))
async def main_menu_cb(client, cb: CallbackQuery):
    await cb.answer()
    await cb.message.edit_text(
        "**Welcome to the KeralaCaptain Bot!**",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("➕ Add Movie", callback_data="add_movie")],
            [InlineKeyboardButton("📺 Add Series", callback_data="add_series")],
            [InlineKeyboardButton("⚙️ Admin Panel", callback_data="apanel_start")]
        ])
    )
    await update_user_conversation(cb.message.chat.id, None)

@main_bot.on_callback_query(filters.regex("^(add_movie|add_series)$"))
async def add_content_start_cb(client, cb: CallbackQuery):
    await cb.answer()
    content_type = "movie" if cb.data == "add_movie" else "series"
    chat_id = cb.message.chat.id
    
    conv_data = {"stage": "awaiting_files", "type": content_type, "files": {}}
    await update_user_conversation(chat_id, conv_data)
    
    prompt = "Movie" if content_type == "movie" else "Series Episode(s)"
    await cb.message.edit_text(
        f"🎬 **Add New {prompt}**\n\nPlease send all media files for this {prompt} as a single message or album.",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="cancel_conv")]])
    )

@main_bot.on_message(filters.private & (filters.media_group | filters.document | filters.video) & filters.user(Config.OWNER_ID))
async def media_handler(client, message: Message):
    chat_id = message.chat.id
    conv = await get_user_conversation(chat_id)
    if not conv or conv.get("stage") != "awaiting_files": return

    media_group_key = f"{chat_id}:{message.media_group_id}" if message.media_group_id else None
    if media_group_key and media_group_key in processed_media_groups:
        return
    if media_group_key:
        processed_media_groups[media_group_key] = True # Upgraded: Per-user
        async def clear_group_id_after_delay():
            await asyncio.sleep(10)
            processed_media_groups.pop(media_group_key, None)
        asyncio.create_task(clear_group_id_after_delay())

    if message.media_group_id:
        await asyncio.sleep(2)
        media_group_messages = await client.get_media_group(chat_id, message.id)
    else:
        media_group_messages = [message]

    files = {
        (msg.document or msg.video).file_name: msg.id
        for msg in media_group_messages if msg.document or msg.video
    }

    if not files: return

    await update_user_conversation(chat_id, {"files": files})
    await message.reply_text(f"✅ Received {len(files)} file(s). Processing...")
    await process_files_and_search_tmdb(client, message, chat_id)


async def process_files_and_search_tmdb(client, message, chat_id):
    conv = await get_user_conversation(chat_id)
    await update_user_conversation(chat_id, {"stage": "tmdb_search"})
    
    files = conv.get("files", {})
    if not files:
        await message.reply_text("No valid files were received. Please try again.")
        await update_user_conversation(chat_id, None)
        return

    first_filename = next(iter(files))
    query, year, language = parse_filename(first_filename)
    await update_user_conversation(chat_id, {'detected_language': language})

    results = await tmdb_api.search(query, year, conv["type"])

    if not results:
        await message.reply_text(f"Sorry, couldn't find any content matching '{query}'. Please try again.",
                                     reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back to Menu", callback_data="main_menu")]]))
        await update_user_conversation(chat_id, None)
        return

    buttons = []
    for result in results[:4]:
        title = result.get('title') or result.get('name')
        release_date = result.get('release_date') or result.get('first_air_date')
        year_str = datetime.strptime(release_date, '%Y-%m-%d').year if release_date else ""
        buttons.append([InlineKeyboardButton(f"{title} ({year_str})", callback_data=f"tmdb_{result['id']}")])
    
    buttons.append([InlineKeyboardButton("❌ None of these", callback_data="cancel_conv")])
    await message.reply_text(
        "**Select the correct item from TMDB:**",
        reply_markup=InlineKeyboardMarkup(buttons)
    )

@main_bot.on_callback_query(filters.regex(r"^tmdb_"))
async def tmdb_selected_handler(client, cb: CallbackQuery):
    chat_id = cb.message.chat.id
    conv = await get_user_conversation(chat_id)
    if not conv: return

    await cb.answer("Confirming selection...")
    msg = cb.message
    await update_user_conversation(chat_id, {"stage": "confirm_details"})
    
    tmdb_id = cb.data.split("_")[1]
    
    if await check_duplicate(tmdb_id):
        await msg.edit_text("⚠️ **Duplicate Content!**\n\nThis item already exists.",
                                     reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back to Menu", callback_data="main_menu")]]))
        await update_user_conversation(chat_id, None)
        return

    details = await tmdb_api.get_details(tmdb_id, conv["type"])
    if not details:
        await msg.edit_text("❌ Failed to fetch details from TMDB. Aborting.",
                                     reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back to Menu", callback_data="main_menu")]]))
        await update_user_conversation(chat_id, None)
        return
            
    await update_user_conversation(chat_id, {'tmdb_id': tmdb_id, 'tmdb_details': details})
    
    title = details.get('title') or details.get('name')
    year = (details.get('release_date') or details.get('first_air_date', ''))[:4]
    overview = (details.get('overview') or 'No description available.')[:400] + "..."
    qualities = sorted([get_file_quality(f) for f in conv["files"].keys()], reverse=True)

    text = f"**Please confirm the details:**\n\n"
    text += f"**Title:** `{title}`\n"
    text += f"**Year:** `{year}`\n"
    text += f"**Qualities Found:** `{', '.join(qualities)}`\n\n"
    text += f"**Overview:** {overview}"

    buttons = [
        [InlineKeyboardButton("✅ Publish to Website", callback_data="publish")],
        [InlineKeyboardButton("✏️ Edit Details (Not Implemented)", callback_data="edit_details")],
        [InlineKeyboardButton("❌ Cancel", callback_data="cancel_conv")]
    ]
    
    poster_path = details.get('poster_path')
    if poster_path:
        await client.send_photo(chat_id, f"https://image.tmdb.org/t/p/w500{poster_path}", caption=text, reply_markup=InlineKeyboardMarkup(buttons))
        await msg.delete()
    else:
        await msg.edit_text(text, reply_markup=InlineKeyboardMarkup(buttons))

# Replace your entire existing publish_handler function with this one
@main_bot.on_callback_query(filters.regex("^publish$"))
async def publish_handler(client, cb: CallbackQuery):
    chat_id = cb.message.chat.id
    conv = await get_user_conversation(chat_id)
    if not conv or conv.get("stage") != "confirm_details": return

    await cb.answer("Publishing... Please wait.")
    msg = cb.message
    edit_func = msg.edit_caption if msg.photo else msg.edit_text

    try: await edit_func("`Step 1/4:` Forwarding files...")
    except Exception: return

    message_ids_by_quality = {}
    for filename, file_message_id in sorted(conv["files"].items()):
        try:
            log_msg = await forward_file_safely(await main_bot.get_messages(chat_id, file_message_id))
            if not log_msg: raise Exception("Failed to forward file.")
            message_ids_by_quality[get_file_quality(filename)] = log_msg.id
            await asyncio.sleep(1)
        except Exception as e:
            LOGGER.error(f"Error forwarding file {filename}: {e}")
            return await edit_func(f"❌ Error forwarding file: `{filename}`. Aborting.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back to Menu", callback_data="main_menu")]]))

    await edit_func("`Step 2/4:` Preparing data...")
    details = conv['tmdb_details']
    duration = details.get('runtime') or (details.get('episode_run_time', [0])[0] if details.get('episode_run_time') else 0)

    # Note: We are sending a temporary stream_link here. We will update it with the correct one after getting the post_id.
    wp_payload = {
        "title": details.get('title') or details.get('name'),
        "overview": details.get('overview'),
        "poster_url": f"https://image.tmdb.org/t/p/original{details.get('poster_path')}" if details.get('poster_path') else "",
        "release_year": (details.get('release_date') or details.get('first_air_date', ''))[:4],
        "duration": duration,
        "rating": round(details.get('vote_average', 0), 1),
        "language": conv.get('detected_language', 'en'),
        "cast": [actor['name'] for actor in details.get('credits', {}).get('cast', [])[:10]],
        "genres": [genre['name'] for genre in details.get('genres', [])],
        "stream_link": "#tobeupdated", # Temporary placeholder
        "tmdb_id": conv["tmdb_id"],
        "trailer_url": f"https://www.youtube.com/watch?v={next((v['key'] for v in details.get('videos', {}).get('results', []) if v['type'] == 'Trailer'), None)}" ,
        "qualities": message_ids_by_quality,
        "type": conv["type"],
    }

    await edit_func("`Step 3/4:` Creating post on WordPress...")
    response = await wp_api.post_content(wp_payload)

    if not (response and isinstance(response, dict) and response.get('post_id')):
        return await edit_func(f"❌ **Failed!** Could not post to WordPress.\n**Reason:** `{response.get('message', 'Unknown API Error')}`", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back to Menu", callback_data="main_menu")]]))

    wp_post_id = response['post_id']
    
    # --- THIS IS THE CRUCIAL NEW STEP ---
    # Now that we have the post_id, we create the correct player link
    # and update the post with it.
    await edit_func(f"`Step 4/4:` Updating post with the correct player link...")
    correct_player_link = f"{Config.WP_URL.rstrip('/')}/player/?view_id={wp_post_id}"
    
    # Update the post with the final, correct link
    await wp_api.update_stream_link_and_qualities(wp_post_id, correct_player_link, message_ids_by_quality)
    # --- END OF NEW STEP ---

    post_link = response.get('link', correct_player_link)
    final_text = f"✅ **Success!**\n\n🎬 **{wp_payload['title']}** has been posted.\n🔗 **View Post:** [Click Here]({post_link})"

    # Update our local DB with the correct link as well
    db_payload = {**wp_payload, 'stream_link': correct_player_link, 'wp_post_id': wp_post_id, 'message_ids': message_ids_by_quality, 'added_at': datetime.utcnow(), 'added_by': cb.from_user.id}
    await add_media_to_db(db_payload)
    
    await edit_func(final_text, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back to Menu", callback_data="main_menu")]]))
    await update_user_conversation(chat_id, None)

@main_bot.on_callback_query(filters.regex("^apanel_start$"))
async def apanel_start_cb(client, cb: CallbackQuery):
    await cb.answer()
    await cb.message.edit_text(
        "**⚙️ Admin Panel**\n\nSelect an option from below:",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("📚 My Library", callback_data="apanel_library:0")],
            [InlineKeyboardButton("🔗 Fix/Replace Link", callback_data="apanel_fixlink")],
            [InlineKeyboardButton("📊 Stats", callback_data="apanel_stats")],
            [InlineKeyboardButton("✅ Force Refresh All Links", callback_data="force_refresh_start")],
            [InlineKeyboardButton("⬅️ Back to Main Menu", callback_data="main_menu")]
        ])
    )

# Replace your entire existing run_update_process function with this one
async def run_update_process(message: Message, post_ids_to_update: list = None):
    """Helper function to run the update logic for all or a specific list of post IDs."""
    
    # This feature now updates the WordPress Player link, not the Koyeb URL.
    wp_player_url_base = f"{Config.WP_URL.rstrip('/')}/player/"
    
    status_msg = message
    
    if post_ids_to_update:
        query = {"wp_post_id": {"$in": post_ids_to_update}}
        await status_msg.edit_text(f"🔄 **Retrying {len(post_ids_to_update)} failed items...**")
        delay_between_updates = 5 # Slower delay for retries
    else:
        query = {}
        await status_msg.edit_text("🔄 **Starting link update process...**\n\nFetching all entries from the database. Please wait.")
        delay_between_updates = 2 # Normal delay

    all_media = await media_collection.find(query).to_list(length=None)
    total_items = len(all_media)
    updated_count = 0
    failed_items = []

    if total_items == 0:
        return await status_msg.edit_text("No items found to update.")

    for i, media_doc in enumerate(all_media):
        post_id = media_doc.get('wp_post_id')
        message_ids = media_doc.get('message_ids')

        if not post_id or not message_ids:
            failed_items.append({"id": post_id if post_id else "N/A", "title": media_doc.get('title', 'Unknown Title')})
            continue

        # --- THIS IS THE CRUCIAL CHANGE ---
        # Construct the new, correct WordPress player link
        new_watch_link = f"{wp_player_url_base}?view_id={post_id}"
        # --- END OF CHANGE ---
        
        try:
            # Update local MongoDB
            await media_collection.update_one(
                {"_id": media_doc["_id"]},
                {"$set": {"stream_link": new_watch_link}}
            )

            # Update WordPress
            wp_response = await wp_api.update_stream_link_and_qualities(post_id, new_watch_link, message_ids)
            if not wp_response or wp_response.get('status') == 'error':
                raise Exception(f"WordPress API failed: {wp_response.get('message', 'Unknown error')}")

            updated_count += 1
        except Exception as e:
            LOGGER.error(f"Failed to update link for Post ID {post_id}: {e}")
            failed_items.append({"id": post_id, "title": media_doc.get('title', 'Unknown')})

        if (i + 1) % 5 == 0 or (i + 1) == total_items:
            try:
                await status_msg.edit_text(
                    f"🔄 **Progress:** {i + 1}/{total_items} items checked.\n\n"
                    f"✅ **Updated:** {updated_count}\n"
                    f"❌ **Failed:** {len(failed_items)}"
                )
            except Exception: pass # Ignore if message is not modified
        
        await asyncio.sleep(delay_between_updates)

    # ... (The rest of the function for the final report remains the same) ...
    final_text = (
        f"✅ **Update Process Complete!**\n\n"
        f"- **Total Items Processed:** {total_items}\n"
        f"- **Successfully Updated:** {updated_count}\n"
        f"- **Failed to Update:** {len(failed_items)}"
    )
    
    buttons = [[InlineKeyboardButton("⬅️ Back to Admin Panel", callback_data="apanel_start")]]
    
    if failed_items:
        failed_ids = [item['id'] for item in failed_items if item.get('id') is not None]
        await update_user_conversation(message.chat.id, {"failed_ids_for_retry": failed_ids})
        
        failed_titles = "\n".join([f"- `{item['title']}` (ID: {item['id']})" for item in failed_items[:10]])
        final_text += f"\n\n**Failed Items:**\n{failed_titles}"
        if len(failed_items) > 10:
            final_text += f"\n...and {len(failed_items) - 10} more."
            
        buttons.insert(0, [InlineKeyboardButton("🔄 Retry Failed Items", callback_data="force_refresh_retry")])

    await status_msg.edit_text(final_text, reply_markup=InlineKeyboardMarkup(buttons))

@main_bot.on_callback_query(filters.regex("^force_refresh_start$"))
async def force_refresh_start_cb(client, cb: CallbackQuery):
    await cb.answer()
    await cb.message.edit_text(
        "⚠️ **Are you sure?**\n\nThis will update the `STREAM_URL` for all entries in your database and WordPress site. This process cannot be undone.",
        reply_markup=InlineKeyboardMarkup([
            [
                InlineKeyboardButton("✅ Yes, Start", callback_data="force_refresh_confirm"),
                InlineKeyboardButton("❌ No, Go Back", callback_data="apanel_start")
            ],
            [InlineKeyboardButton("Close", callback_data="close_panel")]
        ])
    )

@main_bot.on_callback_query(filters.regex("^force_refresh_confirm$"))
async def force_refresh_confirm_cb(client, cb: CallbackQuery):
    await cb.answer("Starting process...", show_alert=True)
    await run_update_process(cb.message)

@main_bot.on_callback_query(filters.regex("^force_refresh_retry$"))
async def force_refresh_retry_cb(client, cb: CallbackQuery):
    await cb.answer("Retrying failed items...", show_alert=True)
    conv = await get_user_conversation(cb.message.chat.id)
    failed_ids = conv.get("failed_ids_for_retry")
    if not failed_ids:
        return await cb.message.edit_text("No failed items found to retry.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back to Admin Panel", callback_data="apanel_start")]]))
    
    await run_update_process(cb.message, post_ids_to_update=failed_ids)

@main_bot.on_callback_query(filters.regex("^close_panel$"))
async def close_panel_cb(client, cb: CallbackQuery):
    await cb.answer("Panel closed.")
    await cb.message.delete()

@main_bot.on_callback_query(filters.regex("^apanel_library:(.+)"))
async def apanel_library_cb(client, cb: CallbackQuery):
    await cb.answer("Fetching library...")
    msg = cb.message
    current_page = int(cb.data.split(":")[1])
    limit = 5

    media_items = await get_all_media_for_library(page=current_page, limit=limit)
    total_count = await media_collection.count_documents({})
    
    text = "**📚 My Library**\n\n"
    buttons = []

    if media_items:
        for item in media_items:
            title = item.get('title', 'Unknown Title')
            post_id = item.get('wp_post_id', 'N/A')
            text += f"▪️ **{title}** (`ID: {post_id}`)\n"
            buttons.append([InlineKeyboardButton(f"View/Manage: {title}", callback_data=f"library_item:{post_id}")])
        text += f"\nPage {current_page + 1} of {math.ceil(total_count / limit)}"
    else:
        text += "No content in your library yet."

    nav_buttons = []
    if current_page > 0:
        nav_buttons.append(InlineKeyboardButton("⬅️ Previous", callback_data=f"apanel_library:{current_page - 1}"))
    if (current_page + 1) * limit < total_count:
        nav_buttons.append(InlineKeyboardButton("➡️ Next", callback_data=f"apanel_library:{current_page + 1}"))
    if nav_buttons:
        buttons.append(nav_buttons)

    buttons.append([InlineKeyboardButton("⬅️ Back to Admin Panel", callback_data="apanel_start")])
    
    await msg.edit_text(text, reply_markup=InlineKeyboardMarkup(buttons))

@main_bot.on_callback_query(filters.regex("^library_item:(.+)"))
async def library_item_details_cb(client, cb: CallbackQuery):
    await cb.answer("Fetching item details...")
    post_id = int(cb.data.split(":")[1])
    media_item = await get_media_by_post_id(post_id)

    if not media_item:
        await cb.message.edit_text("Item not found in library.",
                                         reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back to Library", callback_data="apanel_library:0")]]))
        return

    title = media_item.get('title', 'Unknown')
    wp_link = f"{Config.WP_URL.rstrip('/')}/{media_item.get('type', 'movie')}/{post_id}"

    text = f"**🎬 {title}**\n\n" \
           f"**WP Post ID:** `{post_id}`\n" \
           f"**TMDB ID:** `{media_item.get('tmdb_id', 'N/A')}`\n" \
           f"**Stream Link:** [View on Website]({wp_link})\n" \
           f"**Added At:** `{media_item.get('added_at', 'N/A').strftime('%Y-%m-%d %H:%M:%S')}`\n\n" \
           f"**Qualities:** {', '.join(media_item.get('message_ids', {}).keys())}"

    buttons = [
        [InlineKeyboardButton("🔗 Fix/Replace Link", callback_data=f"fix_item_link:{post_id}")],
        [InlineKeyboardButton("🗑️ Delete", callback_data=f"delete_item:{post_id}")],
        [InlineKeyboardButton("⬅️ Back to Library", callback_data="apanel_library:0")]
    ]
    await cb.message.edit_text(text, reply_markup=InlineKeyboardMarkup(buttons), disable_web_page_preview=True)

@main_bot.on_callback_query(filters.regex("^fix_item_link:(.+)"))
async def fix_item_link_direct_cb(client, cb: CallbackQuery):
    await cb.answer()
    post_id = int(cb.data.split(":")[1])
    chat_id = cb.message.chat.id
    
    media = await get_media_by_post_id(post_id)
    if not media:
        await cb.message.edit_text(f"No media found in the database with Post ID `{post_id}`.",
                                         reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back to Admin Panel", callback_data="apanel_start")]]))
        return

    await update_user_conversation(chat_id, {"stage": "awaiting_new_files", "post_id_to_fix": post_id})
    await cb.message.edit_text(
        f"✅ Post ID `{post_id}` found for **{media['title']}**.\n\nNow, please send the new file(s) for this content as an album or a single file.",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="cancel_conv")]])
    )

@main_bot.on_callback_query(filters.regex("^delete_item:(.+)"))
async def delete_item_cb(client, cb: CallbackQuery):
    await cb.answer("Processing delete request...")
    post_id = int(cb.data.split(":")[1])
    
    await cb.message.edit_text(f"🗑️ Deleting Post ID `{post_id}`...")

    db_deletion_result = await delete_media_from_db(post_id)

    if db_deletion_result.deleted_count == 0:
        await cb.message.edit_text(f"❌ **Error!** Could not find and delete Post ID `{post_id}` from the bot's library. Aborting.",
                                       reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back to Admin Panel", callback_data="apanel_start")]]))
        return
        
    await cb.message.edit_text(f"✅ Deleted from bot's library.\n\n🗑️ Now deleting from WordPress...")

    wp_response = await wp_api.delete_content(post_id)

    if wp_response and wp_response.get('status', '').startswith('success'):
        await cb.message.edit_text(f"✅ **Success!**\n\nPost ID `{post_id}` was deleted from both the bot's library and WordPress.",
                                       reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back to Admin Panel", callback_data="apanel_start")]]))
    else:
        wp_error = wp_response.get('message', 'Unknown error')
        await cb.message.edit_text(f"⚠️ **Partial Success!**\n\nPost ID `{post_id}` was deleted from the bot's library, but **failed to delete from WordPress**.\n\n"
                                       f"**Reason:** `{wp_error}`\n\nYou may need to delete it from your WordPress dashboard manually.",
                                       reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back to Admin Panel", callback_data="apanel_start")]]))


@main_bot.on_callback_query(filters.regex("^apanel_stats$"))
async def apanel_stats_cb(client, cb: CallbackQuery):
    await cb.answer("Fetching stats...")
    movies, series = await get_stats()
    await cb.message.edit_text(
        f"**📊 Bot Statistics**\n\n"
        f"🎬 **Total Movies:** `{movies}`\n"
        f"📺 **Total Series:** `{series}`",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back to Admin Panel", callback_data="apanel_start")]])
    )
    
@main_bot.on_callback_query(filters.regex("^apanel_fixlink$"))
async def apanel_fixlink_start_cb(client, cb: CallbackQuery):
    await cb.answer()
    chat_id = cb.message.chat.id
    await update_user_conversation(chat_id, {"stage": "awaiting_post_id"})
    await cb.message.edit_text(
        "**🔗 Fix/Replace Link**\n\nPlease enter the WordPress Post ID of the content you want to fix.",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="cancel_conv")]])
    )

@main_bot.on_message(filters.private & filters.text & filters.user(Config.OWNER_ID))
async def text_message_handler(client, message: Message):
    chat_id = message.chat.id
    conv = await get_user_conversation(chat_id)
    if not conv: return

    stage = conv.get("stage")
    if stage == "awaiting_post_id":
        if not message.text.isdigit():
            return await message.reply_text("Invalid ID. Please send a numeric WordPress Post ID.")
        
        post_id = int(message.text)
        media = await get_media_by_post_id(post_id)
        if not media:
            return await message.reply_text(f"No media found in the database with Post ID `{post_id}`.",
                                                 reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back to Admin Panel", callback_data="apanel_start")]]))
            
        await update_user_conversation(chat_id, {"stage": "awaiting_new_files", "post_id_to_fix": post_id})
        await message.reply_text(
            f"✅ Post ID `{post_id}` found for **{media['title']}**.\n\nNow, please send the new file(s) for this content as an album or a single file.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="cancel_conv")]])
        )

@main_bot.on_message(filters.private & (filters.media_group | filters.document | filters.video) & filters.user(Config.OWNER_ID), group=2)
async def fix_link_media_handler(client, message: Message):
    chat_id = message.chat.id
    conv = await get_user_conversation(chat_id)
    if not conv or conv.get("stage") != "awaiting_new_files": return

    post_id = conv['post_id_to_fix']
    status_msg = await message.reply_text("Processing new files...")

    media_group_key = f"{chat_id}:{message.media_group_id}" if message.media_group_id else None
    if media_group_key and media_group_key in processed_media_groups: return
    if media_group_key:
        processed_media_groups[media_group_key] = True
        async def clear_group_id():
            await asyncio.sleep(10)
            processed_media_groups.pop(media_group_key, None)
        asyncio.create_task(clear_group_id())

    if message.media_group_id:
        await asyncio.sleep(2)
        media_group_messages = await client.get_media_group(chat_id, message.id)
    else:
        media_group_messages = [message]

    new_message_ids_by_quality = {}
    
    await status_msg.edit("Forwarding new files...")
    for msg in media_group_messages:
        if msg.document or msg.video:
            log_msg = await forward_file_safely(msg)
            if not log_msg: return await status_msg.edit("❌ Failed to forward new files. Aborting.")
            quality = get_file_quality((msg.document or msg.video).file_name)
            new_message_ids_by_quality[quality] = log_msg.id
            await asyncio.sleep(1)

    await status_msg.edit("Generating new stream link...")
    new_stream_link = f"{Config.STREAM_URL}/watch/{'-'.join(map(str, new_message_ids_by_quality.values()))}"

    await status_msg.edit("Updating WordPress...")
    wp_response = await wp_api.update_stream_link_and_qualities(post_id, new_stream_link, new_message_ids_by_quality)
    if not wp_response or wp_response.get('status') == 'error':
        return await status_msg.edit(f"❌ Failed to update WordPress. Reason: {wp_response.get('message', 'Unknown')}")

    await status_msg.edit("Updating local database...")
    await update_media_links_in_db(post_id, new_message_ids_by_quality, new_stream_link)

    await status_msg.edit(f"✅ Success! Link for Post ID `{post_id}` updated.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back to Admin Panel", callback_data="apanel_start")]]))
    await update_user_conversation(chat_id, None)

@main_bot.on_callback_query(filters.regex("^cancel_conv$"))
async def cancel_conversation_handler(client, cb: CallbackQuery):
    await cb.answer("Operation Cancelled.")
    await update_user_conversation(cb.message.chat.id, None)
    await cb.message.delete()
    await start_command(client, cb.message)

# -------------------------------------------------------------------------------- #
# APPLICATION LIFECYCLE                                                          #
# -------------------------------------------------------------------------------- #

async def ping_server():
    """Pings the server to keep it alive on platforms like Heroku."""
    while True:
        await asyncio.sleep(Config.PING_INTERVAL)
        try:
            async with aiohttp.ClientSession(timeout=ClientTimeout(total=10)) as session:
                async with session.get(Config.STREAM_URL) as resp:
                    LOGGER.info(f"Pinged server with status: {resp.status}")
        except Exception as e:
            LOGGER.warning(f"Failed to ping server: {e}")

# --- DELETE your old main_startup_shutdown_logic function and REPLACE it with this entire block ---

if __name__ == "__main__":
    async def main_startup_shutdown_logic():
        """Handles the graceful startup and shutdown of all services."""
        global wp_api, tmdb_api
        
        # Log the starting mode based on the BOT_ROLE environment variable
        LOGGER.info(f"Application starting up in '{Config.BOT_ROLE}' mode...")
        
        # --- ROLE-BASED LOGIC ---
        # Initialize APIs only if the bot is a MASTER.
        # For SLAVE bots, these will be None, effectively disabling admin handlers.
        if Config.BOT_ROLE.upper() == 'MASTER':
            LOGGER.info("MASTER mode enabled: Initializing WordPress and TMDb APIs.")
            wp_api = WordPressAPI(Config.WP_URL, Config.WP_USER, Config.WP_PASSWORD, Config.WP_API_KEY)
            tmdb_api = TMDbAPI()
        else:
            LOGGER.info("SLAVE mode enabled: Initializing for streaming only. APIs are disabled.")
            wp_api = None
            tmdb_api = None
        # --- END OF ROLE-BASED LOGIC ---
        
        # DB Indexing is needed for all bots to read data efficiently
        await media_collection.create_index("tmdb_id", unique=True)
        await media_collection.create_index("wp_post_id", unique=True)
        LOGGER.info("DB indexes ensured for tmdb_id and wp_post_id.")
        
        try:
            await main_bot.start()
            bot_info = await main_bot.get_me()
            LOGGER.info(f"Main Bot @{bot_info.username} started.")
        except FloodWait as e:
            LOGGER.error(f"Telegram FloodWait on main bot startup. Waiting for {e.value} seconds.")
            await asyncio.sleep(e.value + 5)
            await main_bot.start()
            bot_info = await main_bot.get_me()
            LOGGER.info(f"Main Bot @{bot_info.username} started after wait.")
        except Exception as e:
            LOGGER.critical(f"Failed to start main bot: {e}", exc_info=True)
            raise

        if Config.BACKUP_BOT_TOKEN:
            try:
                await backup_bot.start()
                bot_info = await backup_bot.get_me()
                LOGGER.info(f"Backup Bot @{bot_info.username} started.")
            except Exception as e:
                LOGGER.critical(f"Failed to start backup bot: {e}")
        else:
            LOGGER.warning("BACKUP_BOT_TOKEN not provided. Backup bot will not be used.")
            
        await initialize_clients()
        
        if Config.ON_HEROKU:
            asyncio.create_task(ping_server())
        
        web_app = await web_server()
        
        runner = web.AppRunner(web_app)
        await runner.setup()
        site = web.TCPSite(runner, "0.0.0.0", Config.PORT)
        await site.start()
        LOGGER.info(f"Web server started on port {Config.PORT}.")
        
        # Only the MASTER bot will send a startup message to the owner
        if Config.BOT_ROLE.upper() == 'MASTER':
            try:
                await main_bot.send_message(Config.OWNER_ID, "**✅ MASTER Bot has restarted and all services are online!**")
            except Exception as e:
                LOGGER.warning(f"Could not send startup message: {e}")
                
        await asyncio.Event().wait()

    loop = asyncio.get_event_loop()

    async def shutdown_handler(sig):
        LOGGER.info(f"Received exit signal {sig.name}... shutting down gracefully.")
        
        if main_bot and main_bot.is_connected:
            LOGGER.info("Stopping main bot...")
            await main_bot.stop()
        if backup_bot and backup_bot.is_connected:
            LOGGER.info("Stopping backup bot...")
            await backup_bot.stop()
        
        if wp_api:
            LOGGER.info("Closing WordPress API session...")
            await wp_api.close()
        
        tasks = [t for t in asyncio.all_tasks(loop) if t is not asyncio.current_task()]
        if tasks:
            LOGGER.info(f"Cancelling {len(tasks)} outstanding tasks...")
            [task.cancel() for task in tasks]
            await asyncio.gather(*tasks, return_exceptions=True)
        
        loop.stop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(
            sig,
            lambda s=sig: asyncio.create_task(shutdown_handler(s))
        )

    try:
        LOGGER.info("Application starting up...")
        loop.run_until_complete(main_startup_shutdown_logic())
        loop.run_forever()
    except Exception as e:
        LOGGER.critical(f"A critical error forced the application to stop: {e}", exc_info=True)
    finally:
        LOGGER.info("Event loop stopped. Final cleanup.")
        if loop.is_running():
            loop.stop()
        if not loop.is_closed():
            loop.close()
        LOGGER.info("Shutdown complete. Goodbye!")
