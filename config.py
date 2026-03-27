import os
import re
from urllib.parse import quote_plus
from pyrogram import filters

# ═══════════════════════════════════════════════════════════════════════════════
# TELEGRAM BOT CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════

API_ID = 21457002
API_HASH = "6f9f6b8fb05ef1f4d9916e901f27bf52"
BOT_TOKEN = "8507183742:AAGJNPeHy0WOCB06et_5KCMx8ZOB-vALnYU"

LOGGER_ID = -1003646583089
OWNER_ID = 8557740388

# ═══════════════════════════════════════════════════════════════════════════════
# MONGODB CONFIGURATION (Railway March 2026)
# ═══════════════════════════════════════════════════════════════════════════════

MONGO_USER = "mongo"
MONGO_PASSWORD = "CfKZWFPbVMiyRGNPuCVPCOgcZksFwPOM"
MONGO_HOST = "mongodb.railway.internal"
MONGO_PORT = "27017"
MONGO_DB_NAME = "music"

# Build MongoDB URI
_encoded_pass = quote_plus(MONGO_PASSWORD)
MONGO_DB_URI = (
    f"mongodb://{MONGO_USER}:{_encoded_pass}@{MONGO_HOST}:{MONGO_PORT}"
    f"/{MONGO_DB_NAME}?authSource=admin"
)
print(f"[CONFIG] MONGO_DB_URI: {MONGO_DB_URI.replace(MONGO_PASSWORD, '***HIDDEN***')}")

# ═══════════════════════════════════════════════════════════════════════════════
# YOUTUBE & PROXY CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════

YOUTUBE_ENABLED = True
YOUTUBE_USE_PYTUBE = False
YTPROXY_URL = None
YOUTUBE_PROXY = None
YOUTUBE_PROXY_LIST = []

# Живые Invidious-инстансы на март 2026
YOUTUBE_INVIDIOUS_INSTANCES = [
    "https://yewtu.be",
    "https://inv.nadeko.net",
    "https://vid.puffyan.us",
    "https://invidious.private.coffee",
    "https://iv.ggtyler.dev",
    "https://invidious.fdn.fr",
    "https://invidious.tiekoetter.com",
    "https://invidious.flokinet.to",
    "https://invidious.darkness.services",
]

YT_API_KEY = "AIzaSyAyFW-9snpxGwFa5cu-p81jjE8Fg1h_6rk"
YOUTUBE_FALLBACK_SEARCH_LIMIT = 5

# Maximum number of external services to try for downloads
# Options: 1-13 (more = more reliable but slower)
# Set to 5 to try 5 services, 13 to try all services
EXTERNAL_SERVICES_MAX_ATTEMPT = int(os.getenv("EXTERNAL_SERVICES_MAX_ATTEMPT", "5"))

# ═══════════════════════════════════════════════════════════════════════════════
# DURATION & RATE LIMITS
# ═══════════════════════════════════════════════════════════════════════════════

DURATION_LIMIT_MIN = 300  # 5 minutes in seconds
DURATION_LIMIT = int(f"{DURATION_LIMIT_MIN//60}:00".split(":")[0]) * 60 + (DURATION_LIMIT_MIN % 60)

# ═══════════════════════════════════════════════════════════════════════════════
# ASSISTANT & AUTO-LEAVE
# ═══════════════════════════════════════════════════════════════════════════════

AUTO_LEAVING_ASSISTANT = False
ASSISTANT_LEAVE_TIME = 5400  # 1.5 hours

# ═══════════════════════════════════════════════════════════════════════════════
# SPOTIFY CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════

SPOTIFY_CLIENT_ID = "1c21247d714244ddbb09925dac565aed"
SPOTIFY_CLIENT_SECRET = "709e1a2969664491b58200860623ef19"
PLAYLIST_FETCH_LIMIT = 25

# ═══════════════════════════════════════════════════════════════════════════════
# FILE SIZE LIMITS
# ═══════════════════════════════════════════════════════════════════════════════

TG_AUDIO_FILESIZE_LIMIT = 2 * 1024 ** 3   # 2 GB
TG_VIDEO_FILESIZE_LIMIT = 2 * 1024 ** 3   # 2 GB

# ═══════════════════════════════════════════════════════════════════════════════
# CACHE & MEMORY
# ═══════════════════════════════════════════════════════════════════════════════

PRIVATE_BOT_MODE_MEM = 1
CACHE_DURATION = 24 * 3600    # 24 hours
CACHE_SLEEP = 3600            # 1 hour

# ═══════════════════════════════════════════════════════════════════════════════
# USERBOT STRING SESSIONS (Pyrogram)
# ══════════════════════════════════

STRING1 = "AgFHaGoAVHa9Q15n2IaDNygtcPNPGHBussJjD7XfLJjKV1b-sDdVsBUJ5SAPUoGx6LSJ9EugCx3uTvPNLoosVuiSDI8viGjPOp1sdN30utmvnCzyKIX0IEtPMzx38jkA3fBEWkfwJ-XziR9nkLUzXvn1I3SIVPj6FVPUSq3SW0qO-0nAPO0kIWZRzFTtRLldjDo67E2S3ge1V_dde4upSgJS6MrsWEY0FL6MYCpObLMZ__SGuY5Qq4exbJMGaCpwS5u_DtTuX-LOxMfte5JXR9FOGY3KxBD9UkRIUraQp2VD0PMacbj8bFNApDXwLr9FEjjch8xOydYQfRfL5CIws4dmsu8wxgAAAAH6ziPRAA"
STRING2 = None
STRING3 = None
STRING4 = None
STRING5 = None

# ═══════════════════════════════════════════════════════════════════════════════
# BLOCKED USERS & STATE DICTIONARIES
# ═══════════════════════════════════════════════════════════════════════════════

BANNED_USERS = filters.user()
adminlist = {}
lyrical = {}
votemode = {}
autoclean = []
confirmer = {}
file_cache = {}

# ═══════════════════════════════════════════════════════════════════════════════
# IMAGE URLs (START, PING, PLAYLISTS, etc.)
# ═══════════════════════════════════════════════════════════════════════════════

START_IMG_URL = [
    "https://image2url.com/r2/default/images/1769269338835-d5ce1f25-55d6-45fc-b9ad-c04ae647827e.jpg",
    "https://image2url.com/r2/default/images/1769269355185-77c5d002-ce9a-47ce-aba1-d1b033e60472.jpg",
    "https://image2url.com/r2/default/images/1769269377267-3084111d-b3fe-4e5e-be58-418b26f25c4d.jpg",
    "https://image2url.com/r2/default/images/1769269399286-a06b9ba6-3f29-47a5-9a32-9f0c3e0a905c.jpg",
    "https://image2url.com/r2/default/images/1769269443873-5d739aec-a837-45be-aa83-409ae4259c5e.jpg",
    "https://image2url.com/r2/default/images/1769269553883-e7fa9182-2d84-4961-a2bf-4ae63e810b1e.jpg"
]

PING_IMG_URL = "https://image2url.com/r2/default/images/1768792821746-ad62ab76-1fdc-45d7-8b5e-a5343577d6bb.jpg"
PLAYLIST_IMG_URL = "https://image2url.com/r2/default/images/1768793789039-2d4017a9-b0a3-43ec-837c-82855012c3fb.jpg"
TELEGRAM_AUDIO_URL = "https://image2url.com/r2/default/images/1768793789039-2d4017a9-b0a3-43ec-837c-82855012c3fb.jpg"
TELEGRAM_VIDEO_URL = "https://image2url.com/r2/default/images/1768793789039-2d4017a9-b0a3-43ec-837c-82855012c3fb.jpg"
STREAM_IMG_URL = "https://image2url.com/r2/default/images/1768793789039-2d4017a9-b0a3-43ec-837c-82855012c3fb.jpg"
SOUNDCLOUD_IMG_URL = "https://image2url.com/r2/default/images/1768793789039-2d4017a9-b0a3-43ec-837c-82855012c3fb.jpg"
YOUTUBE_IMG_URL = "https://image2url.com/r2/default/images/1768793789039-2d4017a9-b0a3-43ec-837c-82855012c3fb.jpg"
SPOTIFY_ARTIST_IMG_URL = "https://image2url.com/r2/default/images/1768793789039-2d4017a9-b0a3-43ec-837c-82855012c3fb.jpg"
SPOTIFY_ALBUM_IMG_URL = "https://image2url.com/r2/default/images/1768793789039-2d4017a9-b0a3-43ec-837c-82855012c3fb.jpg"
SPOTIFY_PLAYLIST_IMG_URL = "https://image2url.com/r2/default/images/1768793789039-2d4017a9-b0a3-43ec-837c-82855012c3fb.jpg"

DEFAULT_THUMB = YOUTUBE_IMG_URL[0]

# ═══════════════════════════════════════════════════════════════════════════════
# UTILITY FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════════

def time_to_seconds(time):
    """Convert MM:SS format to seconds"""
    stringt = str(time)
    return sum(int(x) * 60 ** i for i, x in enumerate(reversed(stringt.split(":"))))
