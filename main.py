# -*- coding: utf-8 -*-
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
import uuid
import urllib.parse
from datetime import datetime
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import DESCENDING
from aiohttp import web, ClientConnectionError, ClientTimeout
from dotenv import load_dotenv
from pyrogram import Client, filters, enums
from pyrogram.errors import FloodWait, UserNotParticipant, AuthBytesInvalid, PeerIdInvalid, LimitInvalid, Timeout, FileReferenceExpired
from pyrogram.types import Message, InlineKeyboardButton, InlineKeyboardMarkup, CallbackQuery
from pyrogram.session import Session, Auth
from pyrogram.file_id import FileId, FileType
from pyrogram import raw as Raw
from pyrogram.raw.types import InputPhotoFileLocation, InputDocumentFileLocation

# ---------------------------------- #
# 1. CONFIGURATION & LOGGING
# ---------------------------------- #
load_dotenv()

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format='[%(asctime)s - %(levelname)s] - %(name)s - %(message)s')
LOGGER = logging.getLogger(__name__)
logging.getLogger("pyrogram").setLevel(logging.WARNING)
logging.getLogger("aiohttp.web").setLevel(logging.ERROR)
logging.getLogger("aiohttp.client").setLevel(logging.WARNING)
# -------------------

class Config:
    """Configuration class for the Streaming-Only Bot."""
    API_ID = int(os.environ.get("API_ID", 0))
    API_HASH = os.environ.get("API_HASH", "")
    BOT_TOKEN = os.environ.get("BOT_TOKEN", "")

    # --- Critical for Streaming ---
    # !!! Ensure these match the MASTER bot's settings !!!
    MONGO_URI = os.environ.get("MONGO_URI", "")
    LOG_CHANNEL_ID = int(os.environ.get("LOG_CHANNEL_ID", 0))
    # --- ADDED: Backup Log Channel ID ---
    BACKUP_LOG_CHANNEL_ID = int(os.environ.get("BACKUP_LOG_CHANNEL_ID", 0)) # Read from env

    # --- Streaming URL & Port ---
    # !!! This MUST be the public URL where THIS bot is running !!!
    STREAM_URL = os.environ.get("STREAM_URL", "").rstrip('/')
    PORT = int(os.environ.get("PORT", 8080))

    # --- Optional for scaling & reliability ---
    BACKUP_BOT_TOKEN = os.environ.get("BACKUP_BOT_TOKEN", "") # Optional backup bot for forwarding on error
    # Add MULTI_TOKEN_1, MULTI_TOKEN_2 etc. to your .env if needed

    # --- Optional: WP_URL needed ONLY for Referer check ---
    WP_URL = os.environ.get("WP_URL", "https://xtubepro.xo.je/").rstrip('/') # Used for Referer check in /stream

# --- Validate essential configurations for the streaming bot ---
required_vars = [
    Config.API_ID, Config.API_HASH, Config.BOT_TOKEN,
    Config.MONGO_URI, Config.LOG_CHANNEL_ID,
    Config.STREAM_URL, Config.PORT
]
required_var_names = [
    "API_ID", "API_HASH", "BOT_TOKEN",
    "MONGO_URI", "LOG_CHANNEL_ID",
    "STREAM_URL", "PORT"
]

missing_vars = [name for name, var in zip(required_var_names, required_vars) if not var]

if missing_vars:
    LOGGER.critical(f"FATAL: Streaming Bot - Missing required variables: {', '.join(missing_vars)}. Cannot start.")
    exit(1)
else:
    LOGGER.info("Streaming Bot: All essential environment variables are present.")

# --- Warn if WP_URL is missing for Referer check ---
if not Config.WP_URL:
    LOGGER.warning("WP_URL is not set. Referer check for streaming might not work as expected.")

# ---------------------------------- #
# 2. GLOBAL VARIABLES & DB SETUP
# ---------------------------------- #
db_client = AsyncIOMotorClient(Config.MONGO_URI)
# Ensure DB name matches MASTER bot
db = db_client['BotCUnifiedDatabase'] # Or 'BotCDatabase' if that's what Master uses
media_collection = db['media']
# Backup collection is primarily written by Master, but Slave might update on FileRefExpired
media_backup_collection = db['media_backup']
# Settings collection might be needed for get_setting (e.g., if used by ByteStreamer indirectly)
settings_collection = db['settings']

# --- For multi-client streaming ---
multi_clients = {}
work_loads = {}
class_cache = {}
next_client_idx = 0
bot_username = "streaming_bot" # Default username

# ---------------------------------- #
# 3. DATABASE FUNCTIONS (Read-focused)
# ---------------------------------- #

async def get_setting(key: str, default=None):
    """Retrieve a setting from the database."""
    # Needed if any shared logic uses settings
    try:
        doc = await settings_collection.find_one({"_id": key})
        return doc['value'] if doc else default
    except Exception as e:
        LOGGER.error(f"Error getting setting '{key}': {e}")
        return default

async def get_media_by_post_id(post_id: int):
    """Reads media data only from the main collection."""
    try:
        return await media_collection.find_one({"wp_post_id": post_id})
    except Exception as e:
        LOGGER.error(f"Error getting media by post_id {post_id}: {e}")
        return None

# --- MODIFIED: Update function needed for FileReferenceExpired fix ---
async def update_media_links_in_db(post_id: int, new_message_ids: dict, new_stream_link: str, new_backup_message_ids: dict = None):
    """Updates links in both the main and backup collections (used on FileRefExpired)."""
    try:
        update_data = {"message_ids": new_message_ids, "stream_link": new_stream_link}
        if new_backup_message_ids is not None:
            update_data["backup_message_ids"] = new_backup_message_ids

        update_query = {"$set": update_data}
        # Update both collections for consistency
        await media_collection.update_one({"wp_post_id": post_id}, update_query)
        await media_backup_collection.update_one({"wp_post_id": post_id}, update_query)
        LOGGER.info(f"Updated links in DB for post_id {post_id} after FileRefExpired.")
    except Exception as e:
        LOGGER.error(f"Error updating DB for post_id {post_id} after FileRefExpired: {e}")

# --- MODIFIED: get_post_id_from_msg_id to check backup_message_ids ---
async def get_post_id_from_msg_id(msg_id: int):
    """
    Finds the wp_post_id associated with a given message_id.
    Looks in both 'message_ids' and 'backup_message_ids'.
    """
    try:
        # Check if the msg_id exists as a value in the 'message_ids' dictionary
        doc_main = await media_collection.find_one({f"message_ids.{key}": msg_id for key in await media_collection.distinct("message_ids") if isinstance(media_collection.find_one().get("message_ids"), dict)})

        if doc_main:
            return doc_main.get('wp_post_id')

        # If not found in main IDs, check backup IDs
        # This query is complex, might need optimization or direct check if performance is an issue
        # Simple check: find any document where the backup_message_ids field exists and contains the msg_id as a value
        doc_backup = await media_collection.find_one({"backup_message_ids": {"$exists": True}, "$where": f"for(var key in this.backup_message_ids) {{ if(this.backup_message_ids[key] == {msg_id}) return true; }} return false;"})


        if doc_backup:
             LOGGER.warning(f"Found post_id using backup_message_ids for msg_id {msg_id}.")
             return doc_backup.get('wp_post_id')

        return None # Not found in either
    except Exception as e:
        LOGGER.error(f"Error finding post_id for msg_id {msg_id}: {e}")
        return None


# --- Functions not strictly needed by Slave, but kept for potential shared logic ---
def humanbytes(size: int) -> str:
    """Converts bytes to a human-readable format."""
    if not size: return "0 B"
    power = 1024; n = 0
    Dic_powerN = {0: " B", 1: " KB", 2: " MB", 3: " GB", 4: " TB"}
    while size > power: size /= power; n += 1
    return f"{size:.2f}{Dic_powerN[n]}"

# ---------------------------------- #
# 4. STREAMING ENGINE (`ByteStreamer`)
# (Includes FileReferenceExpired handling)
# ---------------------------------- #

class ByteStreamer:
    """Core streaming engine, optimized for reliability."""
    def __init__(self, client: Client):
        self.client: Client = client
        self.cached_file_ids = {} # Cache for file properties
        self.session_cache = {} # {dc_id: (session, timestamp)} for TTL
        asyncio.create_task(self.clean_cache_regularly())

    async def clean_cache_regularly(self):
        while True:
            await asyncio.sleep(1200) # Clean cache every 20 minutes
            self.cached_file_ids.clear()
            self.session_cache.clear()
            LOGGER.info("ByteStreamer's cache and sessions cleared.")

    async def get_file_properties(self, message_id: int):
        # Check cache first
        if message_id in self.cached_file_ids:
            return self.cached_file_ids[message_id]

        # Fetch from Telegram log channel
        try:
            # Use the correct LOG_CHANNEL_ID from Config
            message = await self.client.get_messages(Config.LOG_CHANNEL_ID, message_id)
        except Exception as e:
            LOGGER.error(f"Failed to get message {message_id} from log channel {Config.LOG_CHANNEL_ID}: {e}")
            raise FileNotFoundError(f"Message {message_id} not accessible.")

        if not message or message.empty or not (message.document or message.video):
            LOGGER.error(f"Message {message_id} in log channel {Config.LOG_CHANNEL_ID} is empty or not media.")
            raise FileNotFoundError(f"Message {message_id} is invalid.")

        media = message.document or message.video
        try:
            file_id = FileId.decode(media.file_id)
            setattr(file_id, "file_size", media.file_size or 0)
            setattr(file_id, "mime_type", media.mime_type or "video/mp4")
            setattr(file_id, "file_name", media.file_name or f"stream_{message_id}.mp4")

            # Cache the result
            self.cached_file_ids[message_id] = file_id
            return file_id
        except Exception as e:
            LOGGER.error(f"Error decoding FileId for message {message_id}: {e}")
            raise FileNotFoundError(f"Could not process file info for {message_id}.")

    async def generate_media_session(self, file_id: FileId) -> Session:
        media_session = self.client.media_sessions.get(file_id.dc_id)
        dc_id = file_id.dc_id

        # Check TTL cache
        if dc_id in self.session_cache:
            session, ts = self.session_cache[dc_id]
            if time.time() - ts < 300: # 5min TTL
                return session

        # Ping existing session if not TTL cached
        if media_session:
            try:
                await media_session.send(Raw.functions.help.GetConfig(), timeout=10)
                self.session_cache[dc_id] = (media_session, time.time()) # Update cache timestamp
                return media_session
            except Exception as e:
                LOGGER.warning(f"Existing media session for DC {dc_id} stale ({e}). Recreating.")
                try: await media_session.stop()
                except: pass
                if dc_id in self.client.media_sessions: del self.client.media_sessions[dc_id]
                media_session = None # Force recreation

        # Create new session if needed
        LOGGER.info(f"Creating new media session for DC {dc_id}")
        if dc_id != await self.client.storage.dc_id():
            # Export/Import Auth for different DC
            try:
                media_session = Session(self.client, dc_id, await Auth(self.client, dc_id, await self.client.storage.test_mode()).create(), await self.client.storage.test_mode(), is_media=True)
                await media_session.start()
                for i in range(3): # Retry auth import
                    try:
                        exported_auth = await self.client.invoke(Raw.functions.auth.ExportAuthorization(dc_id=dc_id))
                        await media_session.send(Raw.functions.auth.ImportAuthorization(id=exported_auth.id, bytes=exported_auth.bytes))
                        break # Success
                    except AuthBytesInvalid as abe:
                        LOGGER.warning(f"AuthBytesInvalid on DC {dc_id} attempt {i+1}: {abe}")
                        if i == 2: raise # Raise after max retries
                        await asyncio.sleep(1)
            except Exception as auth_e:
                LOGGER.error(f"Failed to create/auth media session for DC {dc_id}: {auth_e}")
                raise # Propagate error
        else:
            # Use existing auth key for same DC
            media_session = Session(self.client, dc_id, await self.client.storage.auth_key(), await self.client.storage.test_mode(), is_media=True)
            await media_session.start()

        self.client.media_sessions[dc_id] = media_session
        self.session_cache[dc_id] = (media_session, time.time()) # Cache the new session
        return media_session

    @staticmethod
    def get_location(file_id: FileId):
        # Correctly uses imported types
        if file_id.file_type == FileType.PHOTO:
            return InputPhotoFileLocation(id=file_id.media_id, access_hash=file_id.access_hash, file_reference=file_id.file_reference, thumb_size=file_id.thumbnail_size)
        else:
            return InputDocumentFileLocation(id=file_id.media_id, access_hash=file_id.access_hash, file_reference=file_id.file_reference, thumb_size=file_id.thumbnail_size)

    # --- MODIFIED: Includes attempt to fix FileReferenceExpired using forward_file_safely ---
    async def yield_file(self, file_id: FileId, offset: int, chunk_size: int, message_id: int):
        media_session = await self.generate_media_session(file_id)
        location = self.get_location(file_id)

        current_offset = offset
        retry_count = 0
        max_retries = 3

        while True:
            try:
                chunk = await media_session.send(
                    Raw.functions.upload.GetFile(location=location, offset=current_offset, limit=chunk_size),
                    timeout=30
                )

                if isinstance(chunk, Raw.types.upload.File) and chunk.bytes:
                    yield chunk.bytes
                    if len(chunk.bytes) < chunk_size: break
                    current_offset += len(chunk.bytes)
                else: break # No more bytes

            except FileReferenceExpired:
                retry_count += 1
                if retry_count > max_retries:
                    LOGGER.error(f"FileReferenceExpired max retries reached for msg {message_id}. Cannot stream.")
                    raise # Give up after retries

                LOGGER.warning(f"FileReferenceExpired for msg {message_id}, retry {retry_count}/{max_retries}. Attempting refresh...")

                try:
                    original_msg = await self.client.get_messages(Config.LOG_CHANNEL_ID, message_id)
                    if not original_msg: raise FileNotFoundError("Original message not found for refresh.")

                    # --- Attempt to refresh by forwarding ---
                    # forward_file_safely will use main_bot and backup_bot if available
                    refreshed_msg, new_backup_msg = await forward_file_safely(original_msg)

                    if refreshed_msg:
                        new_msg_id = refreshed_msg.id
                        new_backup_msg_id = new_backup_msg.id if new_backup_msg else None

                        # Update cache immediately
                        new_file_id_obj = await self.get_file_properties(new_msg_id)
                        # Replace old message_id key with new FileId object in cache
                        if message_id in self.cached_file_ids:
                             del self.cached_file_ids[message_id]
                        self.cached_file_ids[new_msg_id] = new_file_id_obj
                        LOGGER.info(f"Refreshed msg_id: {message_id} -> {new_msg_id} (Backup: {new_backup_msg_id}). Cache updated.")


                        # Find the associated wp_post_id to update the DB
                        post_id = await get_post_id_from_msg_id(message_id) # Find post using OLD ID
                        if post_id:
                            media_doc = await get_media_by_post_id(post_id)
                            if media_doc:
                                old_message_ids = media_doc.get('message_ids', {})
                                old_backup_ids = media_doc.get('backup_message_ids', {})
                                new_message_ids_dict = old_message_ids.copy()
                                new_backup_ids_dict = old_backup_ids.copy()
                                updated = False

                                # Find which quality used the old message_id
                                for quality, q_msg_id in old_message_ids.items():
                                    if q_msg_id == message_id:
                                        new_message_ids_dict[quality] = new_msg_id
                                        # Update corresponding backup ID if it exists and was refreshed
                                        if quality in old_backup_ids and new_backup_msg_id:
                                             new_backup_ids_dict[quality] = new_backup_msg_id
                                        updated = True
                                        break # Found the quality

                                if updated:
                                    await update_media_links_in_db(
                                        post_id,
                                        new_message_ids_dict,
                                        media_doc.get('stream_link', ''), # Keep old stream link
                                        new_backup_ids_dict
                                    )
                                else:
                                     LOGGER.warning(f"Could not find quality key for old msg_id {message_id} in post {post_id} during refresh.")

                        else:
                            LOGGER.warning(f"Could not find wp_post_id for expired msg_id {message_id} to update DB.")

                        # --- Critical: Update variables for the current stream attempt ---
                        message_id = new_msg_id # Use the NEW message ID going forward
                        file_id = new_file_id_obj # Use the NEW file_id object
                        location = self.get_location(file_id) # Update location
                        # Current offset remains the same
                        LOGGER.info(f"Retrying stream fetch with refreshed msg_id {message_id}")
                        await asyncio.sleep(2) # Short delay before retry
                        continue # Retry the getFile request

                    else:
                        LOGGER.error(f"Failed to forward message {message_id} for refresh.")
                        raise FileReferenceExpired("Refresh failed.") # Propagate if forwarding failed

                except Exception as refresh_e:
                    LOGGER.error(f"Error during FileReferenceExpired handling for msg {message_id}: {refresh_e}")
                    raise # Re-raise the exception if handling failed

            except FloodWait as e:
                LOGGER.warning(f"FloodWait of {e.value} seconds on get_file for {message_id}. Waiting...")
                await asyncio.sleep(e.value)
                continue # Retry after wait
            except Exception as e:
                LOGGER.error(f"Unexpected error in yield_file for {message_id}: {e}", exc_info=True)
                break # Exit loop on other errors

# ---------------------------------- #
# 5. FORWARDING FUNCTION
# (Needed by ByteStreamer for FileRefExpired fix)
# ---------------------------------- #

# --- MODIFIED: Activated forward_file_safely for FileRefExpired ---
async def forward_file_safely(message_to_forward: Message):
    """
    Forwards a file to the main log channel and (if configured) the backup log channel.
    Uses main_bot first, falls back to backup_bot if available.
    Returns (main_log_msg, backup_log_msg)
    """
    main_log_msg = None
    backup_log_msg = None
    media = message_to_forward.document or message_to_forward.video
    if not media:
        LOGGER.error(f"Message {message_to_forward.id} has no media to forward.")
        return None, None

    file_id = media.file_id
    caption = getattr(message_to_forward, 'caption', '')
    active_main_bot = multi_clients.get(0) # Get main bot instance
    active_backup_bot = multi_clients.get(1) # Get backup bot instance if exists

    # --- 1. Forward to MAIN log channel ---
    try:
        if active_main_bot:
            main_log_msg = await active_main_bot.send_cached_media(
                chat_id=Config.LOG_CHANNEL_ID, file_id=file_id, caption=caption
            )
            LOGGER.info(f"Forwarded to main log ({Config.LOG_CHANNEL_ID}). New ID: {main_log_msg.id} (using main bot)")
        elif active_backup_bot: # Try backup if main isn't available (shouldn't happen in normal run)
             main_log_msg = await active_backup_bot.send_cached_media(
                chat_id=Config.LOG_CHANNEL_ID, file_id=file_id, caption=caption
             )
             LOGGER.info(f"Forwarded to main log ({Config.LOG_CHANNEL_ID}). New ID: {main_log_msg.id} (using backup bot as fallback)")
        else:
             LOGGER.error("No active bot available to forward to main log channel.")
             return None, None # Critical failure if main log fails

    except Exception as e:
        LOGGER.warning(f"Failed to send to main log using main bot: {e}. Trying backup bot...")
        if active_backup_bot:
            try:
                main_log_msg = await active_backup_bot.send_cached_media(
                    chat_id=Config.LOG_CHANNEL_ID, file_id=file_id, caption=caption
                )
                LOGGER.info(f"Forwarded to main log ({Config.LOG_CHANNEL_ID}). New ID: {main_log_msg.id} (using backup bot)")
            except Exception as backup_e:
                LOGGER.error(f"Backup bot also failed to send to main log: {backup_e}")
                return None, None # Critical failure
        else:
            LOGGER.error("Main bot failed, and no backup bot configured. Cannot forward to main log.")
            return None, None # Critical failure

    # --- 2. Forward to BACKUP log channel (if configured) ---
    if Config.BACKUP_LOG_CHANNEL_ID != 0 and Config.BACKUP_LOG_CHANNEL_ID != Config.LOG_CHANNEL_ID:
        try:
            if active_main_bot:
                backup_log_msg = await active_main_bot.send_cached_media(
                    chat_id=Config.BACKUP_LOG_CHANNEL_ID, file_id=file_id, caption=caption
                )
                LOGGER.info(f"Forwarded to backup log ({Config.BACKUP_LOG_CHANNEL_ID}). New ID: {backup_log_msg.id} (using main bot)")
            elif active_backup_bot:
                 backup_log_msg = await active_backup_bot.send_cached_media(
                    chat_id=Config.BACKUP_LOG_CHANNEL_ID, file_id=file_id, caption=caption
                 )
                 LOGGER.info(f"Forwarded to backup log ({Config.BACKUP_LOG_CHANNEL_ID}). New ID: {backup_log_msg.id} (using backup bot as fallback)")

        except Exception as e:
            LOGGER.warning(f"Failed to send to backup log using main bot: {e}. Trying backup bot...")
            if active_backup_bot:
                try:
                    backup_log_msg = await active_backup_bot.send_cached_media(
                        chat_id=Config.BACKUP_LOG_CHANNEL_ID, file_id=file_id, caption=caption
                    )
                    LOGGER.info(f"Forwarded to backup log ({Config.BACKUP_LOG_CHANNEL_ID}). New ID: {backup_log_msg.id} (using backup bot)")
                except Exception as backup_e:
                    LOGGER.error(f"Backup bot also failed to send to backup log: {backup_e}")
            else:
                LOGGER.warning("Main bot failed to send to backup log, and no backup bot configured.")
        # Failure to send to backup log is not critical, so we don't return None

    return main_log_msg, backup_log_msg
# --- END MODIFIED FUNCTION ---

# ---------------------------------- #
# 6. WEB SERVER & ROUTES
# ---------------------------------- #

routes = web.RouteTableDef()

@routes.get("/", allow_head=True)
async def root_route_handler(request):
    # Simple response indicating the bot is running
    return web.Response(text=f"Streaming Bot Service Active!", content_type='text/html')

@routes.get("/health")
async def health_handler(request):
    """Provides health status including active clients and cache size."""
    global stream_errors, last_error_reset
    # Reset error count periodically (e.g., every 60 seconds)
    if time.time() - last_error_reset > 60:
        stream_errors = 0
        last_error_reset = time.time()

    active_sessions = len(multi_clients)
    cache_size = 0
    if multi_clients:
        # Get cache size from the main client's ByteStreamer instance
        main_client_instance = multi_clients.get(0)
        if main_client_instance and main_client_instance in class_cache:
            cache_size = len(class_cache[main_client_instance].cached_file_ids)

    return web.json_response({
        "status": "ok",
        "active_clients": active_sessions,
        "cache_size": cache_size,
        "stream_errors_last_min": stream_errors,
        "bot_username": bot_username,
        "log_channel": Config.LOG_CHANNEL_ID,
        "backup_log_channel": Config.BACKUP_LOG_CHANNEL_ID if Config.BACKUP_LOG_CHANNEL_ID else "Not Configured"
    })

@routes.get("/favicon.ico")
async def favicon_handler(request):
    # Respond with No Content for favicon requests
    return web.Response(status=204)

# --- Core Streaming Route ---
@routes.get("/stream/{message_id:\d+}")
async def stream_handler(request: web.Request):
    """Handles byte-range requests for streaming video files."""
    client_index = None # Ensure defined for finally block
    global stream_errors # Access global error counter
    message_id_str = request.match_info['message_id'] # Get raw message_id string

    try:
        message_id = int(message_id_str)
        range_header = request.headers.get("Range", 0)

        # --- Referer Check (Crucial Security) ---
        referer = request.headers.get('Referer')
        # Use WP_URL from config if available, otherwise allow any or specific domains
        allowed_referer_base = Config.WP_URL # e.g., "https://yoursite.com"
        is_allowed = False
        if allowed_referer_base:
            # Allow if referer starts with the configured WP_URL
            if referer and referer.startswith(allowed_referer_base):
                 is_allowed = True
            # Optional: Allow direct access or testing without referer (e.g., VLC, curl)
            # elif not referer:
            #     is_allowed = True # Uncomment to allow direct access
        else:
             LOGGER.warning("WP_URL not set, skipping Referer check. This is less secure.")
             is_allowed = True # Allow if WP_URL is not configured

        if not is_allowed:
            LOGGER.warning(f"Blocked stream request for {message_id}. Invalid Referer: {referer}. Allowed: {allowed_referer_base}")
            stream_errors += 1
            return web.Response(status=403, text="403 Forbidden: Access denied.")
        # --- End Referer Check ---

        # --- Client Load Balancing ---
        if not work_loads:
            LOGGER.error("No worker clients available (work_loads empty).")
            stream_errors += 1
            return web.Response(status=503, text="Service Unavailable: No streaming workers ready.")

        min_load = min(work_loads.values())
        candidates = [cid for cid, load in work_loads.items() if load == min_load]

        if not candidates:
             LOGGER.error("Client selection error: No candidates found despite workloads existing.")
             stream_errors += 1
             return web.Response(status=503, text="Service Unavailable: Worker selection failed.")

        global next_client_idx
        if len(candidates) > 1:
            # Round-robin if multiple clients have the minimum load
            client_index = candidates[next_client_idx % len(candidates)]
            next_client_idx += 1
        else:
            client_index = candidates[0]

        if client_index not in multi_clients:
             LOGGER.error(f"Selected client index {client_index} not found in multi_clients.")
             stream_errors += 1
             return web.Response(status=503, text="Service Unavailable: Worker client error.")

        selected_client = multi_clients[client_index]
        work_loads[client_index] += 1 # Increment load count
        LOGGER.debug(f"Streaming msg {message_id} via client {client_index}. Workloads: {work_loads}")
        # --- End Client Selection ---

        # --- Get ByteStreamer Instance ---
        if selected_client not in class_cache:
            class_cache[selected_client] = ByteStreamer(selected_client)
        tg_connect = class_cache[selected_client]
        # --- End Get ByteStreamer ---

        # --- Fetch File Properties (Handles FileNotFoundError) ---
        file_id_obj = await tg_connect.get_file_properties(message_id)
        file_size = file_id_obj.file_size
        if file_size <= 0:
             LOGGER.warning(f"File size reported as 0 for message {message_id}. Cannot stream.")
             raise FileNotFoundError(f"File size is zero for {message_id}")
        # --- End Fetch File Properties ---

        # --- Handle Range Request ---
        from_bytes = 0
        to_bytes = file_size - 1 # Default to end of file
        status_code = 200 # Default to full content

        if range_header:
            try:
                range_match = re.match(r'bytes=(\d+)-(\d*)', range_header)
                if range_match:
                    from_bytes = int(range_match.group(1))
                    if range_match.group(2): # If end byte is specified
                        to_bytes = int(range_match.group(2))
                    status_code = 206 # Partial Content
                else: # Handle "bytes=123-" format
                     range_val = range_header.replace("bytes=", "").split("-")[0]
                     from_bytes = int(range_val) if range_val else 0
                     status_code = 206
            except ValueError:
                LOGGER.warning(f"Invalid Range header: {range_header}")
                return web.Response(status=400, text="400 Bad Request: Invalid Range Header")

            if from_bytes >= file_size or to_bytes < from_bytes:
                LOGGER.warning(f"Range Not Satisfiable: Request Range {range_header}, File Size {file_size}")
                return web.Response(status=416, reason="Range Not Satisfiable")
        # --- End Handle Range Request ---

        # --- Calculate Streaming Parameters ---
        chunk_size = 1 * 1024 * 1024 # 1MB chunks
        stream_offset = from_bytes - (from_bytes % chunk_size) # Align start offset
        first_chunk_cut = from_bytes - stream_offset # Bytes to skip in the first chunk
        stream_length = (to_bytes - from_bytes) + 1
        # --- End Calculate Parameters ---

        # --- Prepare Response Headers ---
        headers = {
            "Content-Type": file_id_obj.mime_type,
            "Accept-Ranges": "bytes",
            "Content-Length": str(stream_length),
            "Content-Disposition": f'inline; filename="{file_id_obj.file_name}"',
            # CORS Header (adjust if needed, uses WP_URL base)
            "Access-Control-Allow-Origin": allowed_referer_base if allowed_referer_base else "*",
            "Access-Control-Allow-Headers": "Range, Accept-Encoding",
            "Access-Control-Expose-Headers": "Content-Range, Content-Length, Accept-Ranges",
        }
        if status_code == 206:
            headers["Content-Range"] = f"bytes {from_bytes}-{to_bytes}/{file_size}"
        # --- End Prepare Headers ---

        # --- Stream the Data ---
        resp = web.StreamResponse(status=status_code, headers=headers)
        await resp.prepare(request)

        body_gen = tg_connect.yield_file(file_id_obj, stream_offset, chunk_size, message_id)
        bytes_sent = 0
        first_chunk = True

        async for chunk in body_gen:
            if bytes_sent >= stream_length: # Stop if we've sent enough
                break

            data_to_send = chunk
            if first_chunk and first_chunk_cut > 0:
                data_to_send = chunk[first_chunk_cut:]
                first_chunk = False

            # Ensure we don't send more bytes than requested
            if (bytes_sent + len(data_to_send)) > stream_length:
                data_to_send = data_to_send[:stream_length - bytes_sent]

            try:
                await resp.write(data_to_send)
                bytes_sent += len(data_to_send)
            except (ConnectionResetError, asyncio.CancelledError, ClientConnectionError) as client_e:
                LOGGER.warning(f"Client disconnected ({type(client_e).__name__}) during stream for {message_id}. Sent {humanbytes(bytes_sent)}.")
                return resp # Stop sending

        if bytes_sent < stream_length:
             LOGGER.warning(f"Stream ended prematurely for {message_id}. Sent {humanbytes(bytes_sent)} of {humanbytes(stream_length)} requested.")

        # await resp.write_eof() # Let aiohttp handle closing
        LOGGER.info(f"Stream finished for {message_id}. Sent {humanbytes(bytes_sent)}. Client: {request.remote}")
        return resp
        # --- End Stream Data ---

    # --- Error Handling ---
    except FileNotFoundError as e:
        LOGGER.error(f"Stream Error (404) for {message_id_str}: {e}")
        stream_errors += 1
        return web.Response(status=404, text=f"404 Not Found: {e}")
    except (FileReferenceExpired, AuthBytesInvalid) as tg_e:
        LOGGER.error(f"Stream Error (410) for {message_id_str}: {type(tg_e).__name__}. Needs refresh.")
        stream_errors += 1
        return web.Response(status=410, text="Stream link expired or invalid. Please try refreshing the source page.")
    except Exception as e:
        LOGGER.critical(f"Unhandled Stream Error (500) for {message_id_str}: {e}", exc_info=True)
        stream_errors += 1
        return web.Response(status=500, text="500 Internal Server Error")
    # --- End Error Handling ---

    finally:
        # --- Decrement Workload ---
        if client_index is not None and client_index in work_loads:
            work_loads[client_index] = max(0, work_loads[client_index] - 1) # Prevent negative load
            LOGGER.debug(f"Decremented workload for client {client_index}. Workloads: {work_loads}")
        # --- End Decrement Workload ---


async def web_server():
    """Configures and returns the AIOHTTP web application."""
    web_app = web.Application(client_max_size=30 * 1024 * 1024) # 30MB max request size
    web_app.add_routes(routes)
    return web_app

# ---------------------------------- #
# 7. BOT & CLIENT INITIALIZATION
# ---------------------------------- #

# Main bot instance (used for getting file info, handling FileRefExpired)
main_bot = Client("StreamingBot_Main", api_id=Config.API_ID, api_hash=Config.API_HASH, bot_token=Config.BOT_TOKEN)
# Optional backup bot instance (used as fallback by forward_file_safely)
backup_bot = Client("StreamingBot_Backup", api_id=Config.API_ID, api_hash=Config.API_HASH, bot_token=Config.BACKUP_BOT_TOKEN) if Config.BACKUP_BOT_TOKEN else None

# WordPress and TMDb APIs are not initialized for this streaming-only bot
wp_api = None
tmdb_api = None

class TokenParser:
    """Parses MULTI_TOKEN variables from environment."""
    def parse_from_env(self):
        return {c + 2: t for c, (_, t) in enumerate(filter(lambda n: n[0].startswith("MULTI_TOKEN"), sorted(os.environ.items())))}

async def initialize_clients():
    """Initializes main, backup (if exists), and multi-token clients."""
    global bot_username
    # Add main bot (Client 0)
    if main_bot:
        multi_clients[0] = main_bot
        work_loads[0] = 0
        try:
            user = await main_bot.get_me()
            bot_username = user.username or f"bot_{user.id}" # Use ID if no username
            LOGGER.info(f"Main streaming client username: @{bot_username}")
        except Exception as e:
            LOGGER.error(f"Could not get username for main bot: {e}")
    else:
        LOGGER.critical("Main bot object not created before initialize_clients!")
        return # Cannot continue without main bot

    # Add backup bot (Client 1) if configured and initialized
    if backup_bot:
        multi_clients[1] = backup_bot
        work_loads[1] = 0

    # Parse and start multi-token clients
    multi_tokens = TokenParser().parse_from_env()
    if not multi_tokens:
        LOGGER.info("No additional MULTI_TOKEN clients found.")
    else:
        LOGGER.info(f"Found {len(multi_tokens)} additional MULTI_TOKEN clients.")

    async def start_multi_client(client_id, token):
        """Helper to start each additional client."""
        try:
            session_name = f"StreamingBot_Multi_{client_id}"
            # in_memory=True and no_updates=True are good optimizations for streaming workers
            client = await Client(name=session_name, api_id=Config.API_ID, api_hash=Config.API_HASH, bot_token=token, no_updates=True, in_memory=True).start()
            work_loads[client_id] = 0 # Initialize workload
            LOGGER.info(f"MULTI_TOKEN Client {client_id} started successfully.")
            return client_id, client
        except Exception as e:
            LOGGER.error(f"Failed to start MULTI_TOKEN Client {client_id}: {e}")
            return None

    # Start multi-token clients concurrently
    started_clients = await asyncio.gather(*[start_multi_client(i, token) for i, token in multi_tokens.items()])

    # Add successfully started multi-token clients to the pool
    multi_clients.update({cid: client for cid, client in started_clients if client is not None})

    # Ensure all active clients have a workload entry (should already be set, but good practice)
    for cid in multi_clients:
        if cid not in work_loads:
            work_loads[cid] = 0

    LOGGER.info(f"Total active streaming clients: {len(multi_clients)}. Initial workloads: {work_loads}")


# ---------------------------------- #
# 8. BOT HANDLERS (Minimal)
# ---------------------------------- #

# Only a simple /start command to check if the bot is alive
@main_bot.on_message(filters.command("start") & filters.private)
async def start_command_streaming_bot(client, message):
    """Handles /start command, showing bot status."""
    # Use global bot_username fetched during initialization
    await message.reply_text(
        f"**Streaming Bot Service**\n\n"
        f"👤 **Username:** @{bot_username}\n"
        f"⚙️ **Status:** Online\n"
        f"📺 **Mode:** Streaming Only\n"
        f"📡 **Service URL:** {Config.STREAM_URL}\n\n"
        f"This bot handles video streaming requests.",
        disable_web_page_preview=True
    )

# No other handlers (add, admin, poster etc.) are needed

# ---------------------------------- #
# 9. APPLICATION LIFECYCLE
# ---------------------------------- #

async def ping_server():
    """Periodically pings the service's health endpoint to keep it alive."""
    # Use a shorter interval if needed for free tiers, min 5 mins (300s)
    ping_interval = max(300, Config.PING_INTERVAL if hasattr(Config, 'PING_INTERVAL') else 1200)
    await asyncio.sleep(60) # Initial delay
    while True:
        ping_url = f"{Config.STREAM_URL}/health"
        try:
            async with aiohttp.ClientSession(timeout=ClientTimeout(total=20)) as session:
                async with session.get(ping_url) as resp:
                    if resp.status == 200:
                        LOGGER.info(f"Health check ping successful (Status: {resp.status})")
                    else:
                         LOGGER.warning(f"Health check ping returned status: {resp.status}")
        except Exception as e:
            LOGGER.warning(f"Health check ping failed: {e}")
        await asyncio.sleep(ping_interval)


if __name__ == "__main__":
    async def main_startup_shutdown_logic():
        """Handles startup of clients, web server, and graceful shutdown."""
        LOGGER.info(f"Initializing Streaming-Only Bot...")

        # Initialize Pyrogram Clients (Main, Backup, Multi)
        try:
            # Start main bot first as it's essential
            await main_bot.start()
            # Initialize others (backup, multi) - relies on main_bot being started
            await initialize_clients()
        except FloodWait as e:
             LOGGER.error(f"FloodWait during startup ({e.value}s). Exiting.")
             # Or implement a wait/retry if desired: await asyncio.sleep(e.value + 5); await main_bot.start()...
             exit(1) # Exit if essential bot fails startup
        except Exception as e:
            LOGGER.critical(f"Critical error during Pyrogram client startup: {e}", exc_info=True)
            exit(1) # Exit if essential startup fails

        # Check if at least one client is active
        if not multi_clients:
             LOGGER.critical("FATAL: No Pyrogram clients were initialized successfully. Cannot start streaming service.")
             exit(1)

        # Start Pinger Task (if on Heroku/Koyeb etc.)
        if "DYNO" in os.environ or "KOYEB_APP_NAME" in os.environ:
             LOGGER.info("Detected cloud platform. Starting keep-alive pinger task.")
             asyncio.create_task(ping_server())

        # Start Web Server
        web_app = await web_server()
        runner = web.AppRunner(web_app)
        await runner.setup()
        site = web.TCPSite(runner, "0.0.0.0", Config.PORT)
        try:
            await site.start()
            LOGGER.info(f"Web server started successfully on port {Config.PORT}. Ready for requests at {Config.STREAM_URL}")
        except Exception as e:
            LOGGER.critical(f"Failed to start web server on port {Config.PORT}: {e}", exc_info=True)
            # Attempt to stop clients before exiting
            for client_instance in multi_clients.values():
                 if client_instance.is_connected: await client_instance.stop()
            exit(1)

        # Keep running until shutdown signal
        await asyncio.Event().wait()


    loop = asyncio.get_event_loop()

    async def shutdown_handler(sig):
        """Gracefully stops clients and the event loop."""
        LOGGER.warning(f"Received shutdown signal ({sig.name}). Initiating graceful shutdown...")

        # Stop Web Server Runner first (optional, stops accepting new connections)
        # if 'runner' in locals() and runner:
        #     await runner.cleanup()

        # Stop Pyrogram Clients
        active_clients = list(multi_clients.values())
        stop_tasks = []
        for i, client_instance in enumerate(active_clients):
             if client_instance and client_instance.is_connected:
                 LOGGER.info(f"Stopping client instance {i}...")
                 stop_tasks.append(asyncio.create_task(client_instance.stop()))
        if stop_tasks:
             await asyncio.gather(*stop_tasks, return_exceptions=True)
        LOGGER.info("All active Pyrogram clients stopped.")

        # Cancel remaining asyncio tasks
        tasks = [t for t in asyncio.all_tasks(loop) if t is not asyncio.current_task()]
        if tasks:
            LOGGER.info(f"Cancelling {len(tasks)} outstanding asyncio tasks...")
            [task.cancel() for task in tasks]
            await asyncio.gather(*tasks, return_exceptions=True) # Wait for cancellation
        LOGGER.info("Outstanding tasks cancelled.")

        # Stop the event loop
        LOGGER.info("Stopping event loop.")
        loop.stop()

    # Register signal handlers for graceful shutdown
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, lambda s=sig: asyncio.create_task(shutdown_handler(s)))
        except NotImplementedError:
             # Handle environments where signals might not work (e.g., some Windows setups)
             LOGGER.warning("Signal handlers not fully supported on this platform.")

    # Run the main application logic
    try:
        loop.run_until_complete(main_startup_shutdown_logic())
    except KeyboardInterrupt:
        LOGGER.info("KeyboardInterrupt received. Shutting down...")
        # Manually trigger shutdown if KeyboardInterrupt is caught directly
        if not loop.is_running(): loop.run_until_complete(shutdown_handler(signal.SIGINT))
    except Exception as e:
        LOGGER.critical(f"Application stopped due to a critical error: {e}", exc_info=True)
    finally:
        # Final cleanup
        if loop.is_running():
            loop.stop()
        if not loop.is_closed():
            loop.close()
        logging.shutdown() # Ensure all logs are flushed
        print("Streaming Bot Shutdown complete.") # Use print as logging might be shut down
