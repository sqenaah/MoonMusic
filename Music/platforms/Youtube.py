import asyncio
import glob
import io
import json
import os
import random
import re
import sys
import string
from concurrent .futures import ThreadPoolExecutor
from typing import Optional ,Union
import aiohttp
import requests
import yt_dlp
from pyrogram .enums import MessageEntityType
from pyrogram .types import Message
from requests .adapters import HTTPAdapter
from urllib3 .util .retry import Retry
from Music .utils .youtube_search import VideosSearch ,CustomSearch
import base64
import subprocess
import shutil
from Music import LOGGER
from Music .utils .database import is_on_off
from Music .utils .formatters import time_to_seconds
from Music .utils .external_extractors import try_external_mp3_extraction ,retry_with_backoff ,try_invidious_extraction
import functools
ITALIC_TO_REGULAR =str .maketrans ({119860 :'A',119861 :'B',119862 :'C',119863 :'D',119864 :'E',119865 :'F',119866 :'G',119867 :'H',119868 :'I',119869 :'J',119870 :'K',119871 :'L',119872 :'M',119873 :'N',119874 :'O',119875 :'P',119876 :'Q',119877 :'R',119878 :'S',119879 :'T',119880 :'U',119881 :'V',119882 :'W',119883 :'X',119884 :'Y',119885 :'Z',119886 :'a',119887 :'b',119888 :'c',119889 :'d',119890 :'e',119891 :'f',119892 :'g',119893 :'h',119894 :'i',119895 :'j',119896 :'k',119897 :'l',119898 :'m',119899 :'n',119900 :'o',119901 :'p',119902 :'q',119903 :'r',119904 :'s',119905 :'t',119906 :'u',119907 :'v',119908 :'w',119909 :'x',119910 :'y',119911 :'z',120328 :'A',120329 :'B',120330 :'C',120331 :'D',120332 :'E',120333 :'F',120334 :'G',120335 :'H',120336 :'I',120337 :'J',120338 :'K',120339 :'L',120340 :'M',120341 :'N',120342 :'O',120343 :'P',120344 :'Q',120345 :'R',120346 :'S',120347 :'T',120348 :'U',120349 :'V',120350 :'W',120351 :'X',120352 :'Y',120353 :'Z',120354 :'a',120355 :'b',120356 :'c',120357 :'d',120358 :'e',120359 :'f',120360 :'g',120361 :'h',120362 :'i',120363 :'j',120364 :'k',120365 :'l',120366 :'m',120367 :'n',120368 :'o',120369 :'p',120370 :'q',120371 :'r',120372 :'s',120373 :'t',120374 :'u',120375 :'v',120376 :'w',120377 :'x',120378 :'y',120379 :'z',120380 :'A',120381 :'B',120382 :'C',120383 :'D',120384 :'E',120385 :'F',120386 :'G',120387 :'H',120388 :'I',120389 :'J',120390 :'K',120391 :'L',120392 :'M',120393 :'N',120394 :'O',120395 :'P',120396 :'Q',120397 :'R',120398 :'S',120399 :'T',120400 :'U',120401 :'V',120402 :'W',120403 :'X',120404 :'Y',120405 :'Z',120406 :'a',120407 :'b',120408 :'c',120409 :'d',120410 :'e',120411 :'f',120412 :'g',120413 :'h',120414 :'i',120415 :'j',120416 :'k',120417 :'l',120418 :'m',120419 :'n',120420 :'o',120421 :'p',120422 :'q',120423 :'r',120424 :'s',120425 :'t',120426 :'u',120427 :'v',120428 :'w',120429 :'x',120430 :'y',120431 :'z'})

def convert_italic_unicode (text ):
    return text .translate (ITALIC_TO_REGULAR )

from config import INVIDIOUS_THIRDPARTY_FALLBACK ,YT_API_KEY ,YTPROXY_URL as YTPROXY ,YOUTUBE_PROXY ,YOUTUBE_USE_PYTUBE ,YOUTUBE_INVIDIOUS_INSTANCES ,YOUTUBE_FALLBACK_SEARCH_LIMIT ,YOUTUBE_ENABLED ,YOUTUBE_PROXY_LIST

def _resolve_cookiefile_path ():
    env_path =os .getenv ('YT_COOKIES_PATH')or os .getenv ('COOKIES_PATH')
    if env_path and os .path .exists (env_path ):
        return env_path
    for candidate in ('cookies.txt','/app/cookies.txt'):
        if os .path .exists (candidate ):
            return candidate
    return env_path or None

COOKIEFILE_PATH =_resolve_cookiefile_path ()

logger =LOGGER (__name__ )

def _run_ydl_suppressed(ydl_opts, urls):
    """Run yt-dlp download while suppressing stdout/stderr and return exception string or None."""
    old_stderr = sys.stderr
    old_stdout = sys.stdout
    sys.stderr = io.StringIO()
    sys.stdout = io.StringIO()
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download(urls)
        return None
    except Exception as e:
        return str(e)
    finally:
        sys.stderr = old_stderr
        sys.stdout = old_stdout

def _log_method (vid_id ,method ,api =None ):

    try :
        logger .info (f'MethodUsed: {vid_id } -> {method }')
        if api is not None and hasattr (api ,'dl_stats')and isinstance (api .dl_stats ,dict ):
            key =method if method in ('yt_dlp','invidious','pytube','external_service','direct_stream','legacy_youtube_dl')else method
            api .dl_stats .setdefault (key ,0 )
            api .dl_stats [key ]+=1
    except Exception :
        pass

async def check_file_size (link ):
    if not YOUTUBE_ENABLED :
        logger .warning ('YouTube downloads disabled by configuration; skipping size check')
        return None

    async def get_format_info (link ):
        cmd =['yt-dlp','-J',link ]
        if JS_RUNTIME_CLI :
            cmd [1 :1 ]=JS_RUNTIME_CLI
        proxy =_choose_proxy (0 )
        if proxy :
            cmd .extend (['--proxy',proxy ])
            if COOKIEFILE_PATH :
                cmd.extend(['--cookies', COOKIEFILE_PATH])
            proc = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        stdout ,stderr =await proc .communicate ()
        if proc .returncode !=0 :
            print (f'Error:\n{stderr .decode ()}')
            return None
        return json .loads (stdout .decode ())

    def parse_size (formats ):
        total_size =0
        for format in formats :
            if 'filesize'in format :
                total_size +=format ['filesize']
        return total_size
    info =await get_format_info (link )
    if info is None :
        return None
    formats =info .get ('formats',[])
    if not formats :
        print ('No formats found.')
        return None
    total_size =parse_size (formats )
    return total_size

async def shell_cmd (cmd ):
    proc =await asyncio .create_subprocess_shell (cmd ,stdout =asyncio .subprocess .PIPE ,stderr =asyncio .subprocess .PIPE )
    out ,errorz =await proc .communicate ()
    if errorz :
        if 'unavailable videos are hidden'in errorz .decode ('utf-8').lower ():
            return out .decode ('utf-8')
        else :
            return errorz .decode ('utf-8')
    return out .decode ('utf-8')

def _detect_js_runtime ():
    if shutil .which ('node'):
        return 'node'
    if shutil .which ('deno'):
        return 'deno'
    return None

JS_RUNTIME =_detect_js_runtime ()
if JS_RUNTIME =='node':
    JS_RUNTIME_CLI =['--js-runtimes','node']
    JS_RUNTIMES_DICT ={'node':{'interpreter':'node'}}
elif JS_RUNTIME =='deno':
    JS_RUNTIME_CLI =['--js-runtimes','deno']
    JS_RUNTIMES_DICT ={'deno':{}}
else :
    JS_RUNTIME_CLI =[]
    JS_RUNTIMES_DICT =None

def create_ydl (opts :dict ):
    o =opts .copy ()if isinstance (opts ,dict )else {}
    if JS_RUNTIMES_DICT and 'js_runtimes'not in o :
        o ['js_runtimes']=JS_RUNTIMES_DICT
    # include cookiefile if provided via env var to support authenticated downloads
    try:
        if COOKIEFILE_PATH:
            o['cookiefile'] = COOKIEFILE_PATH
    except Exception:
        pass
    return yt_dlp .YoutubeDL (o )

def _choose_proxy (attempt :int =0 ):
    try :
        if YOUTUBE_PROXY_LIST :
            return random .choice (YOUTUBE_PROXY_LIST )
        return YOUTUBE_PROXY
    except Exception :
        return YOUTUBE_PROXY

async def _download_with_external_services_only (youtube_url :str ,filepath :str ,log_id :str ,api =None ,metric :str ='external_service',label :str ='media')->Optional [str ]:
    os .makedirs (os .path .dirname (filepath )or '.',exist_ok =True )
    if os .path .exists (filepath ):
        return filepath
    try :
        logger .info (f'   → External-only {label } extraction...')
        ext_result =await try_external_mp3_extraction (youtube_url ,filepath ,timeout =45 ,max_attempts =None )
        if ext_result and os .path .exists (filepath ):
            logger .info (f'✅ External-only {label } extraction succeeded for {log_id }')
            _log_method (log_id ,metric ,api )
            return filepath
    except Exception as ext_e :
        logger .debug (f'External-only {label } extraction failed for {log_id }: {type (ext_e ).__name__ }: {ext_e }')
    if INVIDIOUS_THIRDPARTY_FALLBACK :
        try :
            logger .info (f'   → Invidious third-party {label } extraction...')
            inv_result =await try_invidious_extraction (youtube_url ,filepath ,timeout =60 )
            if inv_result and os .path .exists (filepath ):
                logger .info (f'✅ Invidious third-party {label } extraction succeeded for {log_id }')
                _log_method (log_id ,'invidious',api )
                return filepath
        except Exception as inv_e :
            logger .debug (f'Invidious third-party {label } extraction failed for {log_id }: {type (inv_e ).__name__ }: {inv_e }')
    logger .error (f'❌ External-only {label } extraction failed for {log_id }')
    logger .warning ('   Direct YouTube and pytube download paths are disabled. Third-party extractors were exhausted.')
    return None

class YouTubeAPI :

    def __init__ (self ):
        self .base ='https://www.youtube.com/watch?v='
        self .regex ='(?:youtube\\.com|youtu\\.be)'
        self .status ='https://www.youtube.com/oembed?url='
        self .listbase ='https://youtube.com/playlist?list='
        self .reg =re .compile ('\\x1B(?:[@-Z\\\\-_]|\\[[0-?]*[ -/]*[@-~])')
        self .dl_stats ={
        'total_requests':0 ,
        'okflix_downloads':0 ,
        'existing_files':0 ,
        'yt_dlp':0 ,
        'invidious':0 ,
        'pytube':0 ,
        'external_service':0 ,
        'direct_stream':0 ,
        'legacy_youtube_dl':0
        }
        self .invidious_index =0
        self .fallback_search_limit =YOUTUBE_FALLBACK_SEARCH_LIMIT

    def _next_invidious (self ):
        if not YOUTUBE_INVIDIOUS_INSTANCES :
            return None
        inst =YOUTUBE_INVIDIOUS_INSTANCES [self .invidious_index %len (YOUTUBE_INVIDIOUS_INSTANCES )]
        self .invidious_index =(self .invidious_index +1 )%len (YOUTUBE_INVIDIOUS_INSTANCES )
        return inst
    async def exists (self ,link :str ,videoid :Union [bool ,str ]=None ):
        if videoid :
            link =self .base +link
        if re .search (self .regex ,link ):
            return True
        else :
            return False

    async def url (self ,message_1 :Message )->Union [str ,None ]:
        messages =[message_1 ]
        if message_1 .reply_to_message :
            messages .append (message_1 .reply_to_message )
        text =''
        offset =None
        length =None
        for message in messages :
            if offset :
                break
            if message .entities :
                for entity in message .entities :
                    if entity .type ==MessageEntityType .URL :
                        text =message .text or message .caption
                        offset ,length =(entity .offset ,entity .length )
                        break
            elif message .caption_entities :
                for entity in message .caption_entities :
                    if entity .type ==MessageEntityType .TEXT_LINK :
                        return entity .url
        if offset in (None ,):
            return None
        return text [offset :offset +length ]

    async def details (self ,link :str ,videoid :Union [bool ,str ]=None ):
        if videoid :
            link =self .base +link
        if '&'in link :
            link =link .split ('&')[0 ]
        if '?si='in link :
            link =link .split ('?si=')[0 ]
        elif '&si='in link :
            link =link .split ('&si=')[0 ]

        video_id =None
        if 'watch?v='in link :
            try :
                video_id =link .split ('watch?v=')[1 ].split ('&')[0 ]
            except :
                pass

        if YT_API_KEY and video_id :
            try :
                logger .debug (f'Trying YouTube API for details: {video_id }')
                details_url =f"https://www.googleapis.com/youtube/v3/videos?id={video_id }&part=snippet,contentDetails&key={YT_API_KEY }"
                async with aiohttp .ClientSession ()as session :
                    async with session .get (details_url ,timeout =aiohttp .ClientTimeout (total =10 ))as resp :
                        if resp .status ==200 :
                            data =await resp .json ()
                            if 'items'in data and len (data ['items'])>0 :
                                item =data ['items'][0 ]
                                title =item ['snippet']['title']
                                thumbnail =item ['snippet']['thumbnails'].get ('high',{}).get ('url','')
                                duration_iso =item ['contentDetails']['duration']

                                import re as regex
                                duration_regex =regex .compile (r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?')
                                match =duration_regex .match (duration_iso )
                                if match :
                                    hours =int (match .group (1 )or 0 )
                                    minutes =int (match .group (2 )or 0 )
                                    seconds =int (match .group (3 )or 0 )
                                    duration_sec =hours *3600 +minutes *60 +seconds
                                    duration_min =f"{hours }:{minutes :02d}:{seconds :02d}"if hours >0 else f"{minutes }:{seconds :02d}"
                                else :
                                    duration_min ="0:00"
                                    duration_sec =0

                                logger .info (f'✓ YouTube API details: {title }')
                                return (title ,duration_min ,duration_sec ,thumbnail ,video_id )
            except Exception as e :
                logger .debug (f'YouTube API details failed: {e }')

        for attempt in range (2 ):
            try :
                logger .debug (f'Trying VideosSearch details attempt {attempt +1 }/2')
                results =VideosSearch (link ,limit =1 )
                res =await results .next ()
                if res .get ('result'):
                    result =res ['result'][0 ]
                    title =result ['title']
                    title =convert_italic_unicode (title )
                    duration_min =result ['duration']
                    thumbnail =result ['thumbnails'][0 ]['url'].split ('?')[0 ]if result .get ('thumbnails')else ''
                    vidid =result ['id']
                    if str (duration_min )=='None':
                        duration_sec =0
                    else :
                        try :
                            duration_sec =int (time_to_seconds (duration_min ))
                        except :
                            duration_sec =0
                    logger .info (f'✓ VideosSearch details: {title }')
                    return (title ,duration_min ,duration_sec ,thumbnail ,vidid )
            except Exception as e :
                logger .debug (f'VideosSearch details attempt {attempt +1 }/2 failed: {e }')
                if attempt ==0 :
                    await asyncio .sleep (0.5 )

        try :
            if video_id :
                logger .warning (f'Using fallback minimal data for video_id: {video_id }')
                return (f"Video {video_id }","0:00",0 ,"",video_id )
        except :
            pass

        raise ValueError (f"Failed to fetch video details for {link }")

    async def title (self ,link :str ,videoid :Union [bool ,str ]=None ):
        if videoid :
            link =self .base +link
        if '&'in link :
            link =link .split ('&')[0 ]
        if '?si='in link :
            link =link .split ('?si=')[0 ]
        elif '&si='in link :
            link =link .split ('&si=')[0 ]

        video_id =None
        if 'watch?v='in link :
            try :
                video_id =link .split ('watch?v=')[1 ].split ('&')[0 ]
            except :
                pass

        if YT_API_KEY and video_id :
            try :
                details_url =f"https://www.googleapis.com/youtube/v3/videos?id={video_id }&part=snippet&key={YT_API_KEY }"
                async with aiohttp .ClientSession ()as session :
                    async with session .get (details_url ,timeout =aiohttp .ClientTimeout (total =10 ))as resp :
                        if resp .status ==200 :
                            data =await resp .json ()
                            if 'items'in data and len (data ['items'])>0 :
                                return data ['items'][0 ]['snippet']['title']
            except Exception as e :
                logger .debug (f'YouTube API title failed: {e }')

        for attempt in range (2 ):
            try :
                results =VideosSearch (link ,limit =1 )
                res =await results .next ()
                if res .get ('result'):
                    title =res ['result'][0 ]['title']
                    title =convert_italic_unicode (title )
                    return title
            except Exception as e :
                logger .debug (f'VideosSearch title attempt {attempt +1 }/2 failed: {e }')
                if attempt ==0 :
                    await asyncio .sleep (0.5 )

        return "Unknown Title"

    async def duration (self ,link :str ,videoid :Union [bool ,str ]=None ):
        if videoid :
            link =self .base +link
        if '&'in link :
            link =link .split ('&')[0 ]
        if '?si='in link :
            link =link .split ('?si=')[0 ]
        elif '&si='in link :
            link =link .split ('&si=')[0 ]

        video_id =None
        if 'watch?v='in link :
            try :
                video_id =link .split ('watch?v=')[1 ].split ('&')[0 ]
            except :
                pass

        if YT_API_KEY and video_id :
            try :
                details_url =f"https://www.googleapis.com/youtube/v3/videos?id={video_id }&part=contentDetails&key={YT_API_KEY }"
                async with aiohttp .ClientSession ()as session :
                    async with session .get (details_url ,timeout =aiohttp .ClientTimeout (total =10 ))as resp :
                        if resp .status ==200 :
                            data =await resp .json ()
                            if 'items'in data and len (data ['items'])>0 :
                                duration_iso =data ['items'][0 ]['contentDetails']['duration']
                                import re as regex
                                duration_regex =regex .compile (r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?')
                                match =duration_regex .match (duration_iso )
                                if match :
                                    hours =int (match .group (1 )or 0 )
                                    minutes =int (match .group (2 )or 0 )
                                    seconds =int (match .group (3 )or 0 )
                                    return f"{hours }:{minutes :02d}:{seconds :02d}"if hours >0 else f"{minutes }:{seconds :02d}"
            except Exception as e :
                logger .debug (f'YouTube API duration failed: {e }')

        for attempt in range (2 ):
            try :
                results =VideosSearch (link ,limit =1 )
                res =await results .next ()
                if res .get ('result'):
                    duration =res ['result'][0 ].get ('duration','0:00')
                    return duration
            except Exception as e :
                logger .debug (f'VideosSearch duration attempt {attempt +1 }/2 failed: {e }')
                if attempt ==0 :
                    await asyncio .sleep (0.5 )

        return "0:00"

    async def thumbnail (self ,link :str ,videoid :Union [bool ,str ]=None ):
        if videoid :
            link =self .base +link
        if '&'in link :
            link =link .split ('&')[0 ]
        if '?si='in link :
            link =link .split ('?si=')[0 ]
        elif '&si='in link :
            link =link .split ('&si=')[0 ]

        video_id =None
        if 'watch?v='in link :
            try :
                video_id =link .split ('watch?v=')[1 ].split ('&')[0 ]
            except :
                pass

        if YT_API_KEY and video_id :
            try :
                details_url =f"https://www.googleapis.com/youtube/v3/videos?id={video_id }&part=snippet&key={YT_API_KEY }"
                async with aiohttp .ClientSession ()as session :
                    async with session .get (details_url ,timeout =aiohttp .ClientTimeout (total =10 ))as resp :
                        if resp .status ==200 :
                            data =await resp .json ()
                            if 'items'in data and len (data ['items'])>0 :
                                thumbnail =data ['items'][0 ]['snippet']['thumbnails'].get ('high',{}).get ('url','')
                                if thumbnail :
                                    return thumbnail
            except Exception as e :
                logger .debug (f'YouTube API thumbnail failed: {e }')

        for attempt in range (2 ):
            try :
                results =VideosSearch (link ,limit =1 )
                res =await results .next ()
                if res .get ('result')and res ['result'][0 ].get ('thumbnails'):
                    thumbnail =res ['result'][0 ]['thumbnails'][0 ]['url'].split ('?')[0 ]
                    return thumbnail
            except Exception as e :
                logger .debug (f'VideosSearch thumbnail attempt {attempt +1 }/2 failed: {e }')
                if attempt ==0 :
                    await asyncio .sleep (0.5 )

        if video_id :
            return f"https://i.ytimg.com/vi/{video_id }/maxresdefault.jpg"

        return ""

    async def video (self ,link :str ,videoid :Union [bool ,str ]=None ):
        if videoid :
            link =self .base +link
        if '&'in link :
            link =link .split ('&')[0 ]
        if '?si='in link :
            link =link .split ('?si=')[0 ]
        elif '&si='in link :
            link =link .split ('&si=')[0 ]

        video_id =None
        if 'watch?v='in link :
            try :
                video_id =link .split ('watch?v=')[1 ].split ('&')[0 ]
            except Exception :
                video_id =None
        elif 'youtu.be/'in link :
            try :
                video_id =link .split ('youtu.be/')[1 ].split ('?')[0 ]
            except Exception :
                video_id =None

        url_to_check =f'https://www.youtube.com/watch?v={video_id or link }'
        format_options =[]
        try :
            with create_ydl ({'quiet':True ,'socket_timeout':30 })as ydl_info :
                info_local =ydl_info .extract_info (url_to_check ,download =False )
                if info_local is None :
                    formats =[]
                else :
                    formats =info_local .get ('formats',[])or []
                formats =[f for f in formats if f is not None ]
                audio_only =[f for f in formats if f and f .get ('acodec')and f .get ('acodec')!='none'and (not f .get ('vcodec')or f .get ('vcodec')=='none')]
                if audio_only :
                    audio_sorted =sorted (audio_only ,key =lambda f :float (f .get ('abr')or 0 ),reverse =True )
                    format_options .extend ([f .get ('format_id')for f in audio_sorted if f and f .get ('format_id')])
                video_fmts =[f for f in formats if f and f .get ('vcodec')and f .get ('vcodec')!='none']
                if video_fmts :
                    video_sorted =sorted (video_fmts ,key =lambda f :int (f .get ('height')or 0 ),reverse =True )
                    format_options .extend ([f .get ('format_id')for f in video_sorted if f and f .get ('format_id')])
        except Exception as info_e :
            logger .debug (f'Could not extract formats for dynamic selection: {info_e }')

        seen =set ()
        deduped =[]
        for f in format_options :
            if f and f not in seen :
                deduped .append (f )
                seen .add (f )
        deduped .extend (["bestaudio[ext=m4a]/bestaudio/best","bestaudio/best","best","18"])

        auth_blocked = False
        for fmt in deduped :
            try :
                cmd = ['yt-dlp', '-g', '-f', fmt, f'{link }']
                proxy =_choose_proxy (0 )
                if proxy :
                    cmd .extend (['--proxy',proxy ])
                if COOKIEFILE_PATH :
                    cmd .extend (['--cookies',COOKIEFILE_PATH])
                proc =await asyncio .create_subprocess_exec (*cmd ,stdout =asyncio .subprocess .PIPE ,stderr =asyncio .subprocess .PIPE )
                stdout ,stderr =await proc .communicate ()
                if stdout :
                    url =stdout .decode ().split ('\n')[0 ]
                    if url :
                        return (1 ,url )
                if stderr :
                    err_text =stderr.decode(errors='ignore')
                    if 'Sign in to confirm' in err_text:
                        logger .warning (f'yt-dlp blocked by authentication for {link }: {err_text.splitlines()[0][:120]}')
                        auth_blocked = True
                        break
            except Exception as fmt_e :
                logger .warning (f'Format {fmt } failed for video URL: {str (fmt_e )}')
                continue

        # If yt-dlp probing failed to return a direct stream URL, attempt fallbacks
        try :
            os .makedirs ('downloads',exist_ok =True )
            safe_id = video_id or re .sub (r'[^0-9A-Za-z]','',link )[:32 ]
            filepath =os .path .join ('downloads',f'{safe_id }.mp3')

            logger .info (f'No direct stream URL from yt-dlp — attempting Invidious/external fallbacks for {link }')

            # Try Invidious first (may provide direct audio stream)
            try :
                inv =await try_invidious_extraction (f'https://www.youtube.com/watch?v={video_id or link }',filepath )
                if inv and os .path .exists (filepath ):
                    logger .info (f'Invidious fallback succeeded for {link }')
                    _log_method (video_id or safe_id ,'invidious',self )
                    return (1 ,filepath )
            except Exception as inv_e :
                logger .debug (f'Invidious fallback error: {inv_e }')

            # Then try external MP3 extraction services
            try :
                ext =await try_external_mp3_extraction (f'https://www.youtube.com/watch?v={video_id or link }',filepath ,timeout =45 ,max_attempts =None )
                if ext and os .path .exists (filepath ):
                    logger .info (f'External MP3 fallback succeeded for {link }')
                    _log_method (video_id or safe_id ,'external_service',self )
                    return (1 ,filepath )
            except Exception as ext_e :
                logger .debug (f'External MP3 fallback error: {ext_e }')

        except Exception as outer_e :
            logger .debug (f'Fallback extraction failed: {outer_e }')

        return (0 ,"All format options failed")

    async def playlist (self ,link ,limit ,user_id ,videoid :Union [bool ,str ]=None ):
        if videoid :
            link =self .listbase +link
        if '&'in link :
            link =link .split ('&')[0 ]
        if '?si='in link :
            link =link .split ('?si=')[0 ]
        elif '&si='in link :
            link =link .split ('&si=')[0 ]
        if not YOUTUBE_ENABLED :
            logger .warning ('YouTube downloads disabled by configuration; skipping playlist fetch')
            return []
        cmd_str = f'yt-dlp -i --get-id --flat-playlist --playlist-end {limit } --skip-download {link }'
        if COOKIEFILE_PATH :
            cmd_str += f' --cookies "{COOKIEFILE_PATH}"'
        playlist =await shell_cmd (cmd_str)
        try :
            result =playlist .split ('\n')
            for key in result :
                if key =='':
                    result .remove (key )
        except :
            result =[]
        return result

    async def track (self ,link :str ,videoid :Union [bool ,str ]=None ):
        if videoid :
            link =self .base +link
        if '&'in link :
            link =link .split ('&')[0 ]
        if '?si='in link :
            link =link .split ('?si=')[0 ]
        elif '&si='in link :
            link =link .split ('&si=')[0 ]

        # Try to extract video ID from link
        video_id_from_link =None
        if 'watch?v='in link :
            try :
                video_id_from_link =link .split ('watch?v=')[1 ].split ('&')[0 ]
            except :
                pass
        elif 'youtu.be/'in link :
            try :
                video_id_from_link =link .split ('youtu.be/')[1 ].split ('?')[0 ]
            except :
                pass

        # Check if metadata is already cached for this video_id
        if video_id_from_link :
            try :
                from Music .utils .mongo_cache import metadata_cache
                cached_metadata =await metadata_cache .get (f'metadata_{video_id_from_link}')
                if cached_metadata :
                    logger .info (f'✓ Using cached metadata for {video_id_from_link }')
                    return (cached_metadata ,video_id_from_link )
            except Exception as e :
                logger .debug (f'Metadata cache retrieval failed: {e }')

        max_retries =2
        for attempt in range (max_retries ):
            try :
                results =VideosSearch (link ,limit =1 )
                res =await results .next ()
                results_list =res ['result']
                logger .debug (f'VideosSearch returned {len (results_list )} results for "{link }"')
                if results_list :
                    for result in results_list :
                        title =result .get ('title','Unknown Video')
                        duration_min =result .get ('duration','0:00')
                        vidid =result .get ('id','')
                        yturl =result .get ('link','')
                        thumbnails =result .get ('thumbnails',[])
                        thumbnail =thumbnails [0 ]['url'].split ('?')[0 ]if thumbnails else f'https://i.ytimg.com/vi/{vidid }/maxresdefault.jpg'

                        logger .debug (f'Extracted: title={title }, vidid={vidid }, duration={duration_min }')
                        if vidid and title and title !='Unknown Video':
                            track_details ={'title':title ,'link':yturl ,'vidid':vidid ,'duration_min':duration_min ,'thumb':thumbnail }
                            # Cache the metadata
                            try :
                                from Music .utils .mongo_cache import metadata_cache
                                await metadata_cache .set (f'metadata_{vidid }',track_details ,ttl =86400 *7 )
                                logger .info (f'✓ VideosSearch succeeded for "{link }", cached & returning: {track_details }')
                            except Exception as e :
                                logger .debug (f'Metadata cache storage failed: {e }')
                            return (track_details ,vidid )
                        else :
                            logger .warning (f'VideosSearch returned invalid data: title={title }, vidid={vidid }')
            except Exception as e :
                logger .debug (f'VideosSearch attempt {attempt +1 }/{max_retries } failed for "{link }": {e }')
                if attempt <max_retries -1 :
                    await asyncio .sleep (0.5 )

        if YOUTUBE_INVIDIOUS_INSTANCES :
            for _ in range (min (3 ,len (YOUTUBE_INVIDIOUS_INSTANCES ))):
                try :
                    inst =self ._next_invidious ()
                    search_url =f"{inst }/api/v1/search?q={link .replace (' ','+')}&type=video"
                    async with aiohttp .ClientSession ()as session :
                        async with session .get (search_url ,timeout =aiohttp .ClientTimeout (total =10 ))as resp :
                            if resp .status ==200 :
                                data =await resp .json ()
                                if data and isinstance (data ,list )and len (data )>0 :
                                    result =data [0 ]
                                    title =result .get ('title','Unknown')
                                    vid_id =result .get ('videoId','')
                                    duration =result .get ('lengthSeconds',0 )
                                    thumbnail =f"{inst }/vi/{vid_id }/maxresdefault.jpg"

                                    duration_min =f"{int (int (duration )/60 )}:{int (int (duration )%60 ):02d}"

                                    yturl =f"https://www.youtube.com/watch?v={vid_id }"
                                    track_details ={'title':title ,'link':yturl ,'vidid':vid_id ,'duration_min':duration_min ,'thumb':thumbnail }
                                    # Cache the metadata
                                    try :
                                        from Music .utils .mongo_cache import metadata_cache
                                        await metadata_cache .set (f'metadata_{vid_id }',track_details ,ttl =86400 *7 )
                                    except Exception as e :
                                        logger .debug (f'Metadata cache storage failed: {e }')
                                    logger .info (f'✓ Invidious search succeeded for "{link }" using {inst }')
                                    return (track_details ,vid_id )
                except Exception as e :
                    logger .debug (f'Invidious search failed with {inst }: {e }')
                    continue

        if YT_API_KEY :
            try :
                search_url =f"https://www.googleapis.com/youtube/v3/search?q={link .replace (' ','+')}&type=video&part=snippet&key={YT_API_KEY }&maxResults=1"
                async with aiohttp .ClientSession ()as session :
                    async with session .get (search_url ,timeout =aiohttp .ClientTimeout (total =10 ))as resp :
                        if resp .status ==200 :
                            data =await resp .json ()
                            if 'items'in data and len (data ['items'])>0 :
                                item =data ['items'][0 ]
                                vid_id =item ['id']['videoId']
                                title =item ['snippet']['title']
                                thumbnail =item ['snippet']['thumbnails'].get ('high',{}).get ('url','')

                                details_url =f"https://www.googleapis.com/youtube/v3/videos?id={vid_id }&part=contentDetails&key={YT_API_KEY }"
                                async with session .get (details_url ,timeout =aiohttp .ClientTimeout (total =10 ))as details_resp :
                                    if details_resp .status ==200 :
                                        details_data =await details_resp .json ()
                                        if 'items'in details_data and len (details_data ['items'])>0 :
                                            duration_iso =details_data ['items'][0 ]['contentDetails']['duration']

                                            import re as regex
                                            duration_regex =regex .compile (r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?')
                                            match =duration_regex .match (duration_iso )
                                            if match :
                                                hours =int (match .group (1 )or 0 )
                                                minutes =int (match .group (2 )or 0 )
                                                seconds =int (match .group (3 )or 0 )
                                                duration_min =f"{hours }:{minutes :02d}:{seconds :02d}"if hours >0 else f"{minutes }:{seconds :02d}"
                                            else :
                                                duration_min ="0:00"

                                yturl =f"https://www.youtube.com/watch?v={vid_id }"
                                track_details ={'title':title ,'link':yturl ,'vidid':vid_id ,'duration_min':duration_min ,'thumb':thumbnail }
                                # Cache the metadata
                                try :
                                    from Music .utils .mongo_cache import metadata_cache
                                    await metadata_cache .set (f'metadata_{vid_id }',track_details ,ttl =86400 *7 )
                                except Exception as e :
                                    logger .debug (f'Metadata cache storage failed: {e }')
                                logger .info (f'✓ YouTube API search succeeded for "{link }"')
                                return (track_details ,vid_id )
            except Exception as e :
                logger .debug (f'YouTube API search failed: {e }')

        # Fallback: If direct search fails but we have a video_id from the link, use generic metadata
        if video_id_from_link :
            logger .warning (f'Metadata extraction failed for {video_id_from_link }, attempting title extraction via Invidious API')
            fallback_title =f'Music {video_id_from_link }'
            fallback_thumbnail =f'https://i.ytimg.com/vi/{video_id_from_link }/maxresdefault.jpg'
            
            # Try to get title from Invidious as last resort
            if YOUTUBE_INVIDIOUS_INSTANCES :
                for _ in range (min (2 ,len (YOUTUBE_INVIDIOUS_INSTANCES ))):
                    try :
                        inst =self ._next_invidious ()
                        api_url =f"{inst }/api/v1/videos/{video_id_from_link }"
                        async with aiohttp .ClientSession ()as session :
                            async with session .get (api_url ,timeout =aiohttp .ClientTimeout (total =5 ))as resp :
                                if resp .status ==200 :
                                    data =await resp .json ()
                                    if data .get ('title'):
                                        fallback_title =data .get ('title','Unknown')
                                        logger .info (f'✓ Got title from Invidious: {fallback_title }')
                                        break
                    except Exception as e :
                        logger .debug (f'Invidious title fetch failed: {e }')
                        continue
            
            track_details ={
                'title':fallback_title ,
                'link':link ,
                'vidid':video_id_from_link ,
                'duration_min':'0:00',
                'thumb':fallback_thumbnail
            }
            # Cache the fallback metadata with shorter TTL
            try :
                from Music .utils .mongo_cache import metadata_cache
                await metadata_cache .set (f'metadata_{video_id_from_link }',track_details ,ttl =3600 )
            except Exception as e :
                logger .debug (f'Metadata cache storage failed: {e }')
            logger .warning (f'Using fallback metadata: {track_details }')
            return (track_details ,video_id_from_link )

        raise ValueError ("ꜰᴀɪʟᴇᴅ ᴛᴏ ꜰᴇᴛᴄʜ ᴛʀᴀᴄᴋ ᴅᴇᴛᴀɪʟs. ᴛʀʏ ᴘʟᴀʏɪɴɢ ᴀɴʏ ᴏᴛʜᴇʀ.")

    async def formats (self ,link :str ,videoid :Union [bool ,str ]=None ):
        if videoid :
            link =self .base +link
        if '&'in link :
            link =link .split ('&')[0 ]
        if '?si='in link :
            link =link .split ('?si=')[0 ]
        elif '&si='in link :
            link =link .split ('&si=')[0 ]
        if not YOUTUBE_ENABLED :
            logger .warning ('YouTube downloads disabled by configuration; skipping format extraction')
            return ([],link )
        ytdl_opts ={'quiet':True ,'user_agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36','http_headers':{'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36','Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8','Accept-Language':'en-us,en;q=0.5','Sec-Fetch-Mode':'navigate'},'js_runtimes':{'node':{'interpreter':'node'}},'skip_unavailable_fragments':True ,'retries':3 ,'fragment_retries':3 }
        info_opts ={
        'quiet':True ,
        'user_agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        'http_headers':{'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36','Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8','Accept-Language':'en-us,en;q=0.5','Sec-Fetch-Mode':'navigate'},
        'js_runtimes':{'node':{'interpreter':'node'}},
        'socket_timeout':30
        }
        ydl_info =create_ydl (info_opts )
        with ydl_info :
            formats_available =[]
            try :
                r =ydl_info .extract_info (link ,download =False )
            except Exception as e :
                logger .warning (f'Failed to extract info for {link }: {e }')
                return ([],link )
            if not r or 'formats'not in r :
                return ([],link )
            for format in r ['formats']:
                try :
                    str (format ['format'])
                except :
                    continue
                if not 'dash'in str (format ['format']).lower ():
                    try :
                        format ['format']
                        format ['filesize']
                        format ['format_id']
                        format ['ext']
                        format ['format_note']
                    except :
                        continue
                    formats_available .append ({'format':format ['format'],'filesize':format ['filesize'],'format_id':format ['format_id'],'ext':format ['ext'],'format_note':format ['format_note'],'yturl':link })
        return (formats_available ,link )

    async def slider (self ,link :str ,query_type :int ,videoid :Union [bool ,str ]=None ):
        if videoid :
            link =self .base +link
        if '&'in link :
            link =link .split ('&')[0 ]
        if '?si='in link :
            link =link .split ('?si=')[0 ]
        elif '&si='in link :
            link =link .split ('&si=')[0 ]
        try :
            results =[]
            search =VideosSearch (link ,limit =10 )
            search_results =(await search .next ()).get ('result',[])
            for result in search_results :
                duration_str =result .get ('duration','0:00')
                try :
                    parts =duration_str .split (':')
                    duration_secs =0
                    if len (parts )==3 :
                        duration_secs =int (parts [0 ])*3600 +int (parts [1 ])*60 +int (parts [2 ])
                    elif len (parts )==2 :
                        duration_secs =int (parts [0 ])*60 +int (parts [1 ])
                    if duration_secs <=3600 :
                        results .append (result )
                except (ValueError ,IndexError ):
                    continue
            if not results or query_type >=len (results ):
                raise ValueError ('No suitable videos found within duration limit')
            selected =results [query_type ]
            title =selected .get ('title','Unknown')
            duration =selected .get ('duration','0:00')
            vid =selected .get ('id','')
            thumbs =selected .get ('thumbnails') or []
            thumb_url =''
            try :
                if isinstance (thumbs,list )and len (thumbs )>0 :
                    thumb_url =thumbs [0 ].get ('url','')
                    if thumb_url :
                        thumb_url =thumb_url.split ('?')[0 ]
            except Exception :
                thumb_url =''

            return (title,duration,thumb_url,vid)
        except Exception as e :
            LOGGER (__name__ ).error (f'Error in slider: {str (e )}')
            raise ValueError ('Failed to fetch video details')

    async def download (self ,link :str ,mystic ,video :Union [bool ,str ]=None ,videoid :Union [bool ,str ]=None ,songaudio :Union [bool ,str ]=None ,songvideo :Union [bool ,str ]=None ,format_id :Union [bool ,str ]=None ,title :Union [bool ,str ]=None )->str :
        if videoid :
            vid_id =link
            link =self .base +link
        loop =asyncio .get_running_loop ()

        if not YOUTUBE_ENABLED :
            logger .warning (f'YouTube downloads disabled by configuration; skipping download for {link }')
            return None

        def create_session ():
            session =requests .Session ()
            retries =Retry (total =10 ,backoff_factor =0.1 )
            session .mount ('http://',HTTPAdapter (max_retries =retries ))
            session .mount ('https://',HTTPAdapter (max_retries =retries ))
            return session

        async def download_with_requests (url ,filepath ,headers =None ):
            try :
                session =create_session ()
                response =session .get (url ,headers =headers ,stream =True ,timeout =60 ,allow_redirects =True )
                response .raise_for_status ()
                total_size =int (response .headers .get ('content-length',0 ))
                downloaded =0
                chunk_size =1024 *1024
                with open (filepath ,'wb')as file :
                    for chunk in response .iter_content (chunk_size =chunk_size ):
                        if chunk :
                            file .write (chunk )
                            downloaded +=len (chunk )
                return filepath
            except Exception as e :
                logger .error (f'Requests download failed: {str (e )}')
                if os .path .exists (filepath ):
                    os .remove (filepath )
                return None
            finally :
                session .close ()

        async def audio_dl (vid_id ,search_title :str =None ,_recursion_depth :int =0 ):
            try :
                filepath =os .path .join ('downloads',f'{vid_id }.mp3')
                os .makedirs (os .path .dirname (filepath ),exist_ok =True )
                if os .path .exists (filepath ):
                    return filepath

                logger .info (f'🎵 [DOWNLOAD] Downloading audio for {vid_id }...')
                
                # METHOD 1: External MP3 services (PRIMARY - no YouTube authentication needed)
                logger .info (f'   → [1/3] External top-1 service...')
                try :
                    ext_result =await try_external_mp3_extraction (f'https://www.youtube.com/watch?v={vid_id }',filepath ,max_attempts =1 )
                    if ext_result and os .path .exists (filepath ):
                        logger .info (f'✅ [1/3] External service success!')
                        _log_method (vid_id ,'external_service',self )
                        return filepath
                except Exception as ext_e :
                    logger .debug (f'External top-1 service failed: {str (ext_e )[:60 ]}')

                # METHOD 1B: Fallback - External services (all remaining)
                logger .info (f'   → [1B/3] External all services (fallback)...')
                try :
                    ext_result =await try_external_mp3_extraction (f'https://www.youtube.com/watch?v={vid_id }',filepath ,max_attempts =None )
                    if ext_result and os .path .exists (filepath ):
                        logger .info (f'✅ [1B/3] External services success!')
                        _log_method (vid_id ,'external_service',self )
                        return filepath
                except Exception as ext_e :
                    logger .debug (f'All external services failed: {str (ext_e )[:60 ]}')

                # METHOD 2: Invidious (YouTube proxy - fallback)
                if YOUTUBE_INVIDIOUS_INSTANCES :
                    logger .info (f'   → [2/3] Invidious proxy instances...')
                    for _ in range (len (YOUTUBE_INVIDIOUS_INSTANCES )):
                        inst =self ._next_invidious ()
                        if not inst :
                            break
                        try :
                            invid_url =f"{inst .rstrip ('/')}/watch?v={vid_id }"
                            ydl_fallback ={'format':'bestaudio[ext=m4a]/bestaudio/best','outtmpl':os .path .join ('downloads',f'{vid_id }'),'postprocessors':[{'key':'FFmpegExtractAudio','preferredcodec':'mp3','preferredquality':'320'}],'quiet':True ,'no_warnings':True ,'retries':5 ,'fragment_retries':5 ,'skip_unavailable_fragments':True ,'js_runtimes':{'node':{'interpreter':'node'}},'socket_timeout':30 }
                            if YOUTUBE_PROXY and 'proxy'not in ydl_fallback :
                                ydl_fallback ['proxy']=YOUTUBE_PROXY
                            loop =asyncio .get_running_loop ()
                            with ThreadPoolExecutor (max_workers =2 )as executor :
                                await loop .run_in_executor (executor ,lambda :create_ydl (ydl_fallback ).download ([invid_url ]))
                            if os .path .exists (filepath ):
                                logger .info (f'✅ [2/2] Invidious succeeded via {inst }')
                                _log_method (vid_id ,'invidious',self )
                                return filepath
                        except Exception as inv_e :
                            logger .debug (f'Invidious {inst }: {type (inv_e ).__name__ }')
                            continue

                # No direct YouTube download attempts - External + Invidious only!
                logger .error (f'❌ Download FAILED for {vid_id }')
                logger .warning (f'   All external services & Invidious exhausted')
                logger .warning (f'   ⚠️  Direct YouTube download DISABLED (anti-bot protection)')
                return None
            except Exception as e :
                logger .error (f'audio_dl error for {vid_id }: {str (e )}')
                return None
        async def video_dl (vid_id ,search_title :str =None ,_recursion_depth :int =0 ):
            try :
                filepath =os .path .join ('downloads',f'{vid_id }.mp4')
                os .makedirs (os .path .dirname (filepath ),exist_ok =True )
                if os .path .exists (filepath ):
                    return filepath

                logger .info (f'🎬 [SMART FALLBACK] Attempting best extraction services for {vid_id }...')
                is_available =False
                requires_auth =False

                if not is_available and not requires_auth and search_title and _recursion_depth <2 :
                    logger .info (f'Video {vid_id } is unavailable (attempt {_recursion_depth +1 }) - searching for alternative video using title: {search_title }...')
                    try :
                        search =VideosSearch (search_title ,limit =3 )
                        results =(await search .next ()).get ('result',[])
                        logger .info (f'Alternative search found {len (results )} results for "{search_title }"')
                        attempted =0
                        for i ,r in enumerate (results ):
                            alt_vid =r .get ('id')
                            if alt_vid and alt_vid !=vid_id :
                                attempted +=1
                                alt_title =r .get ('title','unknown')[:50 ]
                                logger .info (f'  [{attempted }] Trying alternative: {alt_vid } (title: {alt_title })')
                                alt_res =await video_dl (alt_vid ,search_title ,_recursion_depth +1 )
                                if alt_res :
                                    logger .info (f'✓ Successfully downloaded alternative video {alt_vid }')
                                    return alt_res
                        if attempted ==0 :
                            logger .warning (f'No valid alternatives found (all {len (results )} results had same ID or missing)')
                    except Exception as s_e :
                        logger .warning (f'Alternative video search failed: {type (s_e ).__name__ }: {s_e }')
                elif not is_available and _recursion_depth >=2 :
                    logger .warning (f'Max recursion depth reached for alternatives ({_recursion_depth }), skipping to fallback methods')

                url_to_check =f'https://www.youtube.com/watch?v={vid_id }'
                format_options =[]

                logger .info (f'   → Attempting external video extraction services (primary - top-1)...')
                try :
                    ext_result =await try_external_mp3_extraction (f'https://www.youtube.com/watch?v={vid_id }',filepath ,max_attempts =1 )
                    if ext_result and os .path .exists (filepath ):
                        logger .info (f'✅ External service video succeeded for {vid_id } (primary)')
                        _log_method (vid_id ,'external_service_video',self )
                        return filepath
                except Exception as ext_e :
                    logger .debug (f'External video services (primary) failed: {str (ext_e )[:80 ]}')

                logger .info (f'   → Attempting external video extraction services (fallback - all)...')
                try :
                    ext_result =await try_external_mp3_extraction (f'https://www.youtube.com/watch?v={vid_id }',filepath ,max_attempts =None )
                    if ext_result and os .path .exists (filepath ):
                        logger .info (f'✅ External service video succeeded for {vid_id } (fallback)')
                        _log_method (vid_id ,'external_service_video',self )
                        return filepath
                except Exception as ext_e :
                    logger .debug (f'External video services (fallback) failed: {str (ext_e )[:80 ]}')

                logger .info (f'   → Attempting Invidious (public proxy)...')
                if YOUTUBE_INVIDIOUS_INSTANCES :
                    for _ in range (len (YOUTUBE_INVIDIOUS_INSTANCES )):
                        inst =self ._next_invidious ()
                        if not inst :
                            break
                        try :
                            invid_url =f"{inst .rstrip ('/')}/watch?v={vid_id }"
                            ydl_fallback ={'format':'best','outtmpl':filepath ,'quiet':True ,'no_warnings':True ,'retries':3 ,'fragment_retries':3 ,'skip_unavailable_fragments':True ,'js_runtimes':{'node':{'interpreter':'node'}}}
                            if YOUTUBE_PROXY :
                                ydl_fallback ['proxy']=YOUTUBE_PROXY
                            loop =asyncio .get_running_loop ()
                            old_stderr =sys .stderr
                            old_stdout =sys .stdout
                            sys .stderr =io .StringIO ()
                            sys .stdout =io .StringIO ()
                            try :
                                with ThreadPoolExecutor (max_workers =2 )as executor :
                                    await loop .run_in_executor (executor ,lambda :create_ydl (ydl_fallback ).download ([invid_url ]))
                            finally :
                                sys .stderr =old_stderr
                                sys .stdout =old_stdout
                            if os .path .exists (filepath ):
                                logger .info (f'\u2705 Invidious video succeeded for {vid_id }')
                                _log_method (vid_id ,'invidious_video',self )
                                return filepath
                        except Exception as inv_e :
                            logger .debug (f'Invidious {inst }: {str (inv_e )[:80 ]}')
                            continue

                logger .info (f'   → Attempting external video extraction services...')
                try :
                    ext_result =await try_external_mp3_extraction (f'https://www.youtube.com/watch?v={vid_id }',filepath ,max_attempts =1 )
                    if ext_result and os .path .exists (filepath ):
                        logger .info (f'External service video succeeded for {vid_id }')
                        _log_method (vid_id ,'external_service_video',self )
                        return filepath
                except Exception as ext_e :
                    logger .debug (f'External video services (primary) failed: {str (ext_e )[:80 ]}')

                try :
                    ext_result =await try_external_mp3_extraction (f'https://www.youtube.com/watch?v={vid_id }',filepath ,max_attempts =None )
                    if ext_result and os .path .exists (filepath ):
                        logger .info (f'External service video succeeded for {vid_id } (fallback)')
                        _log_method (vid_id ,'external_service_video',self )
                        return filepath
                except Exception as ext_e :
                    logger .debug (f'External video services (fallback) failed: {str (ext_e )[:80 ]}')

                logger .info (f'   → Attempting YouTube yt-dlp (final fallback)...')

                format_options =[]
                try :
                    with create_ydl ({'quiet':True ,'socket_timeout':30 })as ydl_info :
                        info_local =ydl_info .extract_info (f'https://www.youtube.com/watch?v={vid_id }',download =False )
                        if info_local is None :
                            formats =[]
                        else :
                            formats =info_local .get ('formats',[])or []
                        formats =[f for f in formats if f is not None ]
                        audio_only =[f for f in formats if f and f .get ('acodec')and f .get ('acodec')!='none'and (not f .get ('vcodec')or f .get ('vcodec')=='none')]
                        if audio_only :
                            audio_sorted =sorted (audio_only ,key =lambda f :float (f .get ('abr')or 0 ),reverse =True )
                            format_options .extend ([f .get ('format_id')for f in audio_sorted if f and f .get ('format_id')])
                        video_fmts =[f for f in formats if f and f .get ('vcodec')and f .get ('vcodec')!='none']
                        if video_fmts :
                            video_sorted =sorted (video_fmts ,key =lambda f :int (f .get ('height')or 0 ),reverse =True )
                            format_options .extend ([f .get ('format_id')for f in video_sorted if f and f .get ('format_id')])
                except Exception :
                    format_options =[]

                seen =set ()
                deduped =[]
                for f in format_options :
                    if f and f not in seen :
                        deduped .append (f )
                        seen .add (f )

                if len (deduped )>10 :
                    deduped =["bestaudio/best","bestvideo[ext=mp4]+bestaudio[ext=m4a]/best","best","18"]
                else :
                    deduped .extend (["bestaudio[ext=m4a]/best","bestaudio[ext=m4a]/bestaudio","bestaudio/best","best","18"])

                for fmt in deduped :
                    try :
                        ydl_opts ={
                        'format':fmt ,
                        'outtmpl':filepath ,
                        'quiet':True ,
                        'no_warnings':True ,
                        'retries':3 ,
                        'fragment_retries':3 ,
                        'skip_unavailable_fragments':True ,
                        'socket_timeout':10 ,
                        'js_runtimes':{'node':{'interpreter':'node'}},
                        'http_headers':{
                        'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
                        'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                        'Accept-Language':'en-us,en;q=0.5',
                        'Sec-Fetch-Mode':'navigate',
                        'Sec-Fetch-Dest':'document',
                        'Sec-Fetch-Site':'none',
                        'Sec-Fetch-User':'?1',
                        'Upgrade-Insecure-Requests':'1'
                        },
                        'extractor_args':{'youtube':{'player_client':['web'],'innertube_client':'web'}}
                        }
                        if YOUTUBE_PROXY :
                            ydl_opts ['proxy']=YOUTUBE_PROXY
                        loop =asyncio .get_running_loop ()
                        with ThreadPoolExecutor (max_workers =2 )as executor :
                            err =await loop .run_in_executor (executor ,functools .partial (_run_ydl_suppressed ,ydl_opts ,[f'https://www.youtube.com/watch?v={vid_id }']))
                            if err and 'Sign in to confirm' in err:
                                logger .debug (f'Invidious {inst }: auth blocked: {err.splitlines()[0][:120]}')
                                continue
                        if os .path .exists (filepath ):
                            logger .info (f'\u2705 YouTube yt-dlp format {fmt } succeeded (final fallback)')
                            _log_method (vid_id ,'yt_dlp',self )
                            return filepath
                    except Exception as fmt_e :
                        error_msg =str (fmt_e )if fmt_e else 'Unknown error'
                        if 'Sign in to confirm'in error_msg :
                            logger .debug (f'Format {fmt }: Requires auth')
                            break
                        elif 'HTTP Error 429'in error_msg or 'Too Many Requests'in error_msg :
                            logger .debug (f'Format {fmt }: Rate limited')
                            break
                        logger .debug (f'Format {fmt }: Failed')
                        if os .path .exists (filepath ):
                            os .remove (filepath )
                        continue

                logger .error (f'❌ All extraction methods failed for video {vid_id }')
                return None
            except Exception as e :
                logger .error (f'video_dl error for {vid_id }: {str (e )}')
                return None

        async def song_video_dl ():
            try :
                filepath =f'downloads/{title }.mp4'
                os .makedirs (os .path .dirname (filepath ),exist_ok =True )
                if os .path .exists (filepath ):
                    return filepath

                logger .info (f'🎬 [SMART FALLBACK] Attempting best extraction services for song video {vid_id }...')
                url_to_check =f'https://www.youtube.com/watch?v={vid_id }'
                format_options =[]
                try :
                    with create_ydl ({'quiet':True ,'socket_timeout':30 })as ydl_info :
                        info_local =ydl_info .extract_info (url_to_check ,download =False )
                        if info_local is None :
                            formats =[]
                        else :
                            formats =info_local .get ('formats',[])or []
                        formats =[f for f in formats if f is not None ]
                        audio_only =[f for f in formats if f and f .get ('acodec')and f .get ('acodec')!='none'and (not f .get ('vcodec')or f .get ('vcodec')=='none')]
                        if audio_only :
                            audio_sorted =sorted (audio_only ,key =lambda f :float (f .get ('abr')or 0 ),reverse =True )
                            format_options .extend ([f .get ('format_id')for f in audio_sorted if f and f .get ('format_id')])
                        video_fmts =[f for f in formats if f and f .get ('vcodec')and f .get ('vcodec')!='none']
                        if video_fmts :
                            video_sorted =sorted (video_fmts ,key =lambda f :int (f .get ('height')or 0 ),reverse =True )
                            format_options .extend ([f .get ('format_id')for f in video_sorted if f and f .get ('format_id')])
                except Exception as info_e :
                    logger .debug (f'Could not extract formats for dynamic selection: {info_e }')

                seen =set ()
                deduped =[]
                for f in format_options :
                    if f and f not in seen :
                        deduped .append (f )
                        seen .add (f )
                deduped .extend (["bestaudio[ext=m4a]/bestaudio/best","bestaudio/best","best","18/best"])

                for fmt in deduped :
                    try :
                        ydl_opts ={
                        'format':fmt ,
                        'outtmpl':filepath ,
                        'quiet':True ,
                        'no_warnings':True ,
                        'retries':10 ,
                        'fragment_retries':10 ,
                        'skip_unavailable_fragments':True ,
                        'socket_timeout':30 ,
                        'js_runtimes':{'node':{'interpreter':'node'}},
                        'http_headers':{
                        'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
                        'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                        'Accept-Language':'en-us,en;q=0.5',
                        'Sec-Fetch-Mode':'navigate',
                        'Sec-Fetch-Dest':'document',
                        'Sec-Fetch-Site':'none',
                        'Sec-Fetch-User':'?1',
                        'Upgrade-Insecure-Requests':'1'
                        },
                        'extractor_args':{
                        'youtube':{
                        'player_client':['web'],
                        'innertube_client':'web'
                        }
                        }
                        }
                        if YOUTUBE_PROXY :
                            ydl_opts ['proxy']=YOUTUBE_PROXY
                        loop =asyncio .get_running_loop ()
                        with ThreadPoolExecutor (max_workers =2 )as executor :
                            await loop .run_in_executor (executor ,lambda :create_ydl (ydl_opts ).download ([url_to_check ]))
                        if os .path .exists (filepath ):
                            logger .info (f'Song video download succeeded with format: {fmt }')
                            _log_method (vid_id ,'yt_dlp',self )
                            return filepath
                    except Exception as fmt_e :
                        error_msg =str (fmt_e )

                        if 'Sign in to confirm'in error_msg :
                            logger .warning (f'Format {fmt } requires authentication (skipping): {error_msg }')
                            break
                        else :
                            logger .warning (f'Format {fmt } failed for song video {vid_id }: {error_msg }')

                        try :
                            if 'Requested format is not available'in error_msg or 'page needs to be reloaded'in error_msg :
                                fb_opts =ydl_opts .copy ()
                                fb_opts .pop ('format',None )
                                loop =asyncio .get_running_loop ()
                                with ThreadPoolExecutor (max_workers =2 )as executor :
                                    await loop .run_in_executor (executor ,lambda :create_ydl (fb_opts ).download ([url_to_check ]))
                                if os .path .exists (filepath ):
                                    logger .info (f'Fallback without explicit format succeeded for song video {vid_id }')
                                    return filepath
                        except Exception :
                            pass

                        if os .path .exists (filepath ):
                            os .remove (filepath )
                        continue

                logger .error ('All song video format options failed')

                if title :
                    logger .info (f'All song video formats failed for {vid_id } - searching for alternative video using title: {title }...')
                    try :
                        search =VideosSearch (title ,limit =5 )
                        results =(await search .next ()).get ('result',[])
                        for r in results :
                            alt_vid =r .get ('id')
                            if alt_vid and alt_vid !=vid_id :
                                logger .info (f'Trying alternative video {alt_vid } (song video format fallback)')
                                alt_res =await song_video_dl ()
                                if alt_res :
                                    logger .info (f'✓ Successfully downloaded alternative song video via fallback')
                                    return alt_res
                    except Exception as s_e :
                        logger .warning (f'Song video format fallback search failed: {s_e }')

                logger .info (f'Entering fallback extraction chain for {vid_id }...')

                try :
                    logger .info (f'Attempting Invidious video extraction for {vid_id }...')
                    youtube_url =f'https://www.youtube.com/watch?v={vid_id }'
                    invidious_result =await try_invidious_extraction (youtube_url ,filepath ,timeout =90 )
                    if invidious_result and os .path .exists (filepath ):
                        logger .info (f'✓ Invidious video extraction succeeded for {vid_id }')
                        _log_method (vid_id ,'invidious',self )
                        return filepath
                except Exception as inv_e :
                    logger .debug (f'Invidious video extraction failed: {type (inv_e ).__name__ }: {inv_e }')

                try :
                    logger .info (f'Attempting external extraction fallback for {vid_id } (top-1)...')
                    youtube_url =f'https://www.youtube.com/watch?v={vid_id }'
                    external_result =await try_external_mp3_extraction (youtube_url ,filepath ,timeout =45 ,max_attempts =1 )
                    if external_result and os .path .exists (filepath ):
                        logger .info (f'External extraction succeeded for {vid_id }')
                        _log_method (vid_id ,'external_service',self )
                        return filepath
                except Exception as ext_e :
                    logger .debug (f'External extraction (top-1) failed: {type (ext_e ).__name__ }: {ext_e }')

                try :
                    logger .info (f'Attempting external extraction fallback for {vid_id } (all)...')
                    youtube_url =f'https://www.youtube.com/watch?v={vid_id }'
                    external_result =await try_external_mp3_extraction (youtube_url ,filepath ,timeout =45 ,max_attempts =None )
                    if external_result and os .path .exists (filepath ):
                        logger .info (f'External extraction succeeded for {vid_id } (fallback)')
                        _log_method (vid_id ,'external_service',self )
                        return filepath
                except Exception as ext_e :
                    logger .debug (f'External extraction (fallback) also failed: {type (ext_e ).__name__ }: {ext_e }')

                logger .error (f'All extraction methods failed for song video {vid_id }. The video may be unavailable, region-restricted, or removed.')
                return None
            except Exception as e :
                logger .error (f'yt_dlp song video download failed for {vid_id }: {str (e )}')
                return None

        async def song_audio_dl ():
            try :
                filepath =f'downloads/{title }.mp3'
                os .makedirs (os .path .dirname (filepath ),exist_ok =True )
                if os .path .exists (filepath ):
                    return filepath

                if YOUTUBE_INVIDIOUS_INSTANCES :
                    for _ in range (len (YOUTUBE_INVIDIOUS_INSTANCES )):
                        inst =self ._next_invidious ()
                        if not inst :
                            break
                        try :
                            invid_url =f"{inst .rstrip ('/')}/watch?v={vid_id }"
                            ydl_fallback ={'format':'bestaudio/best','outtmpl':f'downloads/{title }','postprocessors':[{'key':'FFmpegExtractAudio','preferredcodec':'mp3','preferredquality':'192'}],'quiet':True ,'no_warnings':True ,'retries':5 ,'fragment_retries':5 ,'skip_unavailable_fragments':True ,'js_runtimes':{'node':{'interpreter':'node'}}}
                            if YOUTUBE_PROXY :
                                ydl_fallback ['proxy']=YOUTUBE_PROXY
                            loop =asyncio .get_running_loop ()
                            with ThreadPoolExecutor (max_workers =2 )as executor :
                                await loop .run_in_executor (executor ,lambda :create_ydl (ydl_fallback ).download ([invid_url ]))
                            if os .path .exists (filepath ):
                                logger .info (f'Invidious song download succeeded with {inst }')
                                return filepath
                        except Exception as e :
                            logger .warning (f'Invidious {inst } failed for song {vid_id }: {e }')

                if YOUTUBE_USE_PYTUBE :
                    try :
                        from pytube import YouTube as PyTube
                        yt_obj =PyTube (f'https://www.youtube.com/watch?v={vid_id }')
                        stream =yt_obj .streams .filter (only_audio =True ).order_by ('abr').desc ().first ()
                        if stream :
                            out =stream .download (output_path ='downloads',filename =f'{title }_pytube')

                            mp3path =filepath
                            try :
                                subprocess .run (['ffmpeg','-y','-i',out ,'-vn','-ab','192k',mp3path ],check =True )
                                if os .path .exists (mp3path ):
                                    logger .info ('pytube song download succeeded and converted to mp3')

                                    if os .path .exists (out )and out !=mp3path :
                                        os .remove (out )
                                    return mp3path
                            except Exception as conv_e :
                                logger .warning (f'ffmpeg conversion failed for song {out }: {conv_e }')
                    except Exception as py_e :
                        logger .warning (f'pytube failed for song {vid_id }: {py_e }')
                ydl_opts_list =[
                {
                'format':'bestaudio/best',
                'outtmpl':f'downloads/{title }',
                'postprocessors':[{'key':'FFmpegExtractAudio','preferredcodec':'mp3','preferredquality':'192'}],
                'quiet':True ,
                'no_warnings':True ,
                'retries':5 ,
                'fragment_retries':5 ,
                'skip_unavailable_fragments':True ,
                'socket_timeout':30 ,
                'js_runtimes':{'node':{'interpreter':'node'}},
                'http_headers':{
                'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
                'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language':'en-us,en;q=0.5',
                'Sec-Fetch-Mode':'navigate',
                'Sec-Fetch-Dest':'document',
                'Sec-Fetch-Site':'none',
                'Sec-Fetch-User':'?1',
                'Upgrade-Insecure-Requests':'1'
                },
                'extractor_args':{'youtube':{'player_client':['web'],'innertube_client':'web'}}
                },
                {
                'format':'bestaudio[ext=m4a]/bestaudio[acodec=mp4a]/bestaudio/best[ext=mp4]/best',
                'outtmpl':f'downloads/{title }',
                'postprocessors':[{'key':'FFmpegExtractAudio','preferredcodec':'mp3','preferredquality':'192'}],
                'quiet':True ,
                'no_warnings':True ,
                'retries':5 ,
                'fragment_retries':5 ,
                'skip_unavailable_fragments':True ,
                'socket_timeout':30 ,
                'js_runtimes':{'node':{'interpreter':'node'}},
                'http_headers':{
                'User-Agent':'Mozilla/5.0 (iPhone; CPU iPhone OS 17_5_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Mobile/15E148 Safari/604.1',
                'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language':'en-us,en;q=0.5',
                'Sec-Fetch-Mode':'navigate',
                'Sec-Fetch-Dest':'document',
                'Sec-Fetch-Site':'none',
                'Sec-Fetch-User':'?1',
                'Upgrade-Insecure-Requests':'1'
                },
                'extractor_args':{'youtube':{'player_client':['ios'],'innertube_client':'ios'}}
                },
                {
                'format':'bestaudio[ext=m4a]/bestaudio/best',
                'outtmpl':f'downloads/{title }',
                'postprocessors':[{'key':'FFmpegExtractAudio','preferredcodec':'mp3','preferredquality':'128'}],
                'quiet':True ,
                'no_warnings':True ,
                'retries':3 ,
                'fragment_retries':3 ,
                'skip_unavailable_fragments':True ,
                'socket_timeout':30 ,
                'js_runtimes':{'node':{'interpreter':'node'}},
                'http_headers':{
                'User-Agent':'Mozilla/5.0 (Linux; Android 10; SM-G973F) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Mobile Safari/537.36',
                'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language':'en-us,en;q=0.5',
                'Sec-Fetch-Mode':'navigate',
                'Sec-Fetch-Dest':'document',
                'Sec-Fetch-Site':'none',
                'Sec-Fetch-User':'?1',
                'Upgrade-Insecure-Requests':'1',
                'DNT':'1',
                'Sec-Ch-Ua-Mobile':'?1',
                'Sec-Ch-Ua-Platform':'"Android"'
                },
                'extractor_args':{'youtube':{'player_client':['android'],'innertube_client':'android'}}
                }
                ]
                for i ,ydl_opts in enumerate (ydl_opts_list ):
                    try :
                        logger .info (f'Trying song audio download configuration {i +1 } for {vid_id }')
                        if YOUTUBE_PROXY and 'proxy'not in ydl_opts :
                            ydl_opts ['proxy']=YOUTUBE_PROXY
                        loop =asyncio .get_running_loop ()
                        with ThreadPoolExecutor (max_workers =2 )as executor :
                            await loop .run_in_executor (executor ,lambda :create_ydl (ydl_opts ).download ([f'https://www.youtube.com/watch?v={vid_id }']))
                        if os .path .exists (filepath ):
                            return filepath
                        else :
                            logger .warning (f'Song audio download config {i +1 } completed but file not found at {filepath }')
                    except Exception as e :
                        error_msg =str (e )
                        logger .warning (f'Song audio download config {i +1 } failed for {vid_id }: {error_msg }')
                        if 'page needs to be reloaded'not in error_msg and 'Requested format is not available'not in error_msg :
                            break
                        continue
                logger .error (f'All song audio download configurations failed for {vid_id }')

                if title :
                    logger .info (f'All song audio formats failed for {vid_id } - searching for alternative video using title: {title }...')
                    try :
                        search =VideosSearch (title ,limit =5 )
                        results =(await search .next ()).get ('result',[])
                        for r in results :
                            alt_vid =r .get ('id')
                            if alt_vid and alt_vid !=vid_id :
                                logger .info (f'Trying alternative video {alt_vid } (song audio format fallback)')
                                alt_res =await song_audio_dl ()
                                if alt_res :
                                    logger .info (f'✓ Successfully downloaded alternative song audio via fallback')
                                    return alt_res
                    except Exception as s_e :
                        logger .warning (f'Song audio format fallback search failed: {s_e }')

                if YOUTUBE_INVIDIOUS_INSTANCES :
                    for _ in range (len (YOUTUBE_INVIDIOUS_INSTANCES )):
                        inst =self ._next_invidious ()
                        if not inst :
                            break
                        try :
                            invid_url =f"{inst .rstrip ('/')}/watch?v={vid_id }"
                            ydl_fallback ={'format':'bestaudio/best','outtmpl':f'downloads/{title }','postprocessors':[{'key':'FFmpegExtractAudio','preferredcodec':'mp3','preferredquality':'192'}],'quiet':True ,'no_warnings':True }
                            if YOUTUBE_PROXY :
                                ydl_fallback ['proxy']=YOUTUBE_PROXY
                            loop =asyncio .get_running_loop ()
                            with ThreadPoolExecutor (max_workers =2 )as executor :
                                await loop .run_in_executor (executor ,lambda :create_ydl (ydl_fallback ).download ([invid_url ]))
                            if os .path .exists (filepath ):
                                logger .info (f'Invidious fallback succeeded with {inst }')
                                return filepath
                        except Exception as e :
                            logger .warning (f'Invidious fallback {inst } failed for {vid_id }: {e }')

                if YOUTUBE_USE_PYTUBE :
                    try :
                        from pytube import YouTube as PyTube
                        stream =PyTube (f'https://www.youtube.com/watch?v={vid_id }').streams .filter (only_audio =True ).order_by ('abr').desc ().first ()
                        if stream :
                            out =stream .download (output_path ='downloads',filename =f'{title }_pytube')
                            mp3path =filepath
                            try :
                                subprocess .run (['ffmpeg','-y','-i',out ,'-vn','-ab','192k',mp3path ],check =True )
                                if os .path .exists (mp3path ):
                                    if os .path .exists (out )and out !=mp3path :
                                        os .remove (out )
                                    return mp3path
                            except Exception as conv_e :
                                logger .warning (f'ffmpeg conversion failed for {out }: {conv_e }')
                    except Exception as py_e :
                        logger .warning (f'pytube fallback failed for {vid_id }: {py_e }')

                try :
                    cmd =['yt-dlp','--format','bestaudio/best','-g',f'https://www.youtube.com/watch?v={vid_id }']
                    if JS_RUNTIME_CLI :
                        cmd [1 :1 ]=JS_RUNTIME_CLI
                    if YOUTUBE_PROXY :
                        cmd .extend (['--proxy',YOUTUBE_PROXY ])
                    if COOKIEFILE_PATH :
                        cmd.extend(['--cookies', COOKIEFILE_PATH])
                    proc =await asyncio .create_subprocess_exec (*cmd ,stdout =asyncio .subprocess .PIPE ,stderr =asyncio .subprocess .PIPE )
                    stdout ,stderr =await proc .communicate ()
                    if proc .returncode ==0 and stdout :
                        urls =stdout .decode ().splitlines ()
                        for u in urls :
                            res =await download_with_requests (u ,filepath )
                            if res :
                                logger .info ('Direct stream fallback succeeded')
                                return res
                except Exception as ds_e :
                    logger .warning (f'Direct-stream fallback failed: {ds_e }')

                try :
                    if title :
                        search =VideosSearch (title ,limit =self .fallback_search_limit )
                        results =(await search .next ()).get ('result',[])
                        for r in results :
                            alt_vid =r .get ('id')
                            if alt_vid and alt_vid !=vid_id :
                                logger .info (f'Trying alternative video {alt_vid } for title match')
                                alt_res =await song_audio_dl (alt_vid )
                                if alt_res :
                                    return alt_res
                except Exception as s_e :
                    logger .warning (f'Fallback search failed: {s_e }')

                try :
                    logger .info (f'Attempting Invidious extraction fallback for {vid_id }...')
                    youtube_url =f'https://www.youtube.com/watch?v={vid_id }'
                    invidious_result =await try_invidious_extraction (youtube_url ,filepath ,timeout =90 )
                    if invidious_result and os .path .exists (filepath ):
                        logger .info (f'✓ Invidious extraction succeeded for {vid_id }')
                        return filepath
                except Exception as inv_e :
                    logger .debug (f'Invidious extraction also failed: {type (inv_e ).__name__ }: {inv_e }')

                try :
                    logger .info (f'Attempting external extraction fallback for {vid_id } (top-1)...')
                    youtube_url =f'https://www.youtube.com/watch?v={vid_id }'
                    external_result =await try_external_mp3_extraction (youtube_url ,filepath ,timeout =45 ,max_attempts =1 )
                    if external_result and os .path .exists (filepath ):
                        logger .info (f'External extraction succeeded for {vid_id }')
                        return filepath
                except Exception as ext_e :
                    logger .debug (f'External extraction (top-1) failed: {type (ext_e ).__name__ }: {ext_e }')

                try :
                    logger .info (f'Attempting external extraction fallback for {vid_id } (all)...')
                    youtube_url =f'https://www.youtube.com/watch?v={vid_id }'
                    external_result =await try_external_mp3_extraction (youtube_url ,filepath ,timeout =45 ,max_attempts=None )
                    if external_result and os .path .exists (filepath ):
                        logger .info (f'External extraction succeeded for {vid_id } (fallback)')
                        return filepath
                except Exception as ext_e :
                    logger .debug (f'External extraction (fallback) also failed: {type (ext_e ).__name__ }: {ext_e }')

                return None
            except Exception as e :
                logger .error (f'yt_dlp song audio download failed for {vid_id }: {str (e )}')
                return None
        if songvideo :
            fpath =await song_video_dl ()
            return fpath
        elif songaudio :
            fpath =await song_audio_dl ()
            return fpath
        elif video :
            direct =True
            downloaded_file =await video_dl (vid_id ,title )
        else :
            direct =True
            downloaded_file =await audio_dl (vid_id ,title )
        return (downloaded_file ,direct )

    async def video (self ,link :str ,videoid :Union [bool ,str ]=None ):
        if videoid :
            link =self .base +link
        if '&'in link :
            link =link .split ('&')[0 ]
        if '?si='in link :
            link =link .split ('?si=')[0 ]
        elif '&si='in link :
            link =link .split ('&si=')[0 ]

        video_id =None
        if 'watch?v='in link :
            try :
                video_id =link .split ('watch?v=')[1 ].split ('&')[0 ]
            except Exception :
                video_id =None
        elif 'youtu.be/'in link :
            try :
                video_id =link .split ('youtu.be/')[1 ].split ('?')[0 ]
            except Exception :
                video_id =None

        safe_id =video_id or re .sub (r'[^0-9A-Za-z]','',link )[:32 ]or 'youtube'
        youtube_url =f'https://www.youtube.com/watch?v={video_id }'if video_id else link
        filepath =os .path .join ('downloads',f'{safe_id }.mp3')
        ext =await _download_with_external_services_only (youtube_url ,filepath ,safe_id ,self ,'external_service','stream' )
        if ext and os .path .exists (filepath ):
            return (1 ,filepath )
        return (0 ,'External services could not extract media')

    async def track (self ,link :str ,videoid :Union [bool ,str ]=None ):
        if videoid :
            link =self .base +link
        if '&'in link :
            link =link .split ('&')[0 ]
        if '?si='in link :
            link =link .split ('?si=')[0 ]
        elif '&si='in link :
            link =link .split ('&si=')[0 ]

        video_id_from_link =None
        if 'watch?v='in link :
            try :
                video_id_from_link =link .split ('watch?v=')[1 ].split ('&')[0 ]
            except Exception :
                video_id_from_link =None
        elif 'youtu.be/'in link :
            try :
                video_id_from_link =link .split ('youtu.be/')[1 ].split ('?')[0 ]
            except Exception :
                video_id_from_link =None

        if video_id_from_link :
            try :
                from Music .utils .mongo_cache import metadata_cache
                cached_metadata =await metadata_cache .get (f'metadata_{video_id_from_link}')
                if cached_metadata :
                    logger .info (f'✓ Using cached metadata for {video_id_from_link }')
                    return (cached_metadata ,video_id_from_link )
            except Exception as e :
                logger .debug (f'Metadata cache retrieval failed: {e }')

        for attempt in range (2 ):
            try :
                results =VideosSearch (link ,limit =1 )
                res =await results .next ()
                results_list =res .get ('result',[])
                logger .debug (f'VideosSearch returned {len (results_list )} results for "{link }"')
                if results_list :
                    result =results_list [0 ]
                    title =result .get ('title','Unknown Video')
                    duration_min =result .get ('duration','0:00')
                    vidid =result .get ('id','')
                    yturl =result .get ('link',f'https://www.youtube.com/watch?v={vidid }')
                    thumbnails =result .get ('thumbnails',[])
                    thumbnail =thumbnails [0 ]['url'].split ('?')[0 ]if thumbnails else f'https://i.ytimg.com/vi/{vidid }/maxresdefault.jpg'
                    if vidid and title and title !='Unknown Video':
                        track_details ={'title':title ,'link':yturl ,'vidid':vidid ,'duration_min':duration_min ,'thumb':thumbnail }
                        try :
                            from Music .utils .mongo_cache import metadata_cache
                            await metadata_cache .set (f'metadata_{vidid }',track_details ,ttl =86400 *7 )
                        except Exception as e :
                            logger .debug (f'Metadata cache storage failed: {e }')
                        logger .info (f'✓ VideosSearch succeeded for "{link }", cached & returning: {track_details }')
                        return (track_details ,vidid )
            except Exception as e :
                logger .debug (f'VideosSearch attempt {attempt +1 }/2 failed for "{link }": {e }')
                if attempt <1 :
                    await asyncio .sleep (0.5 )

        if YT_API_KEY :
            try :
                search_url =f"https://www.googleapis.com/youtube/v3/search?q={link .replace (' ','+')}&type=video&part=snippet&key={YT_API_KEY }&maxResults=1"
                async with aiohttp .ClientSession ()as session :
                    async with session .get (search_url ,timeout =aiohttp .ClientTimeout (total =10 ))as resp :
                        if resp .status ==200 :
                            data =await resp .json ()
                            if 'items'in data and len (data ['items'])>0 :
                                item =data ['items'][0 ]
                                vid_id =item ['id']['videoId']
                                title =item ['snippet']['title']
                                thumbnail =item ['snippet']['thumbnails'].get ('high',{}).get ('url','')
                                duration_min ='0:00'
                                details_url =f"https://www.googleapis.com/youtube/v3/videos?id={vid_id }&part=contentDetails&key={YT_API_KEY }"
                                async with session .get (details_url ,timeout =aiohttp .ClientTimeout (total =10 ))as details_resp :
                                    if details_resp .status ==200 :
                                        details_data =await details_resp .json ()
                                        if 'items'in details_data and len (details_data ['items'])>0 :
                                            duration_iso =details_data ['items'][0 ]['contentDetails']['duration']
                                            import re as regex
                                            duration_regex =regex .compile (r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?')
                                            match =duration_regex .match (duration_iso )
                                            if match :
                                                hours =int (match .group (1 )or 0 )
                                                minutes =int (match .group (2 )or 0 )
                                                seconds =int (match .group (3 )or 0 )
                                                duration_min =f"{hours }:{minutes :02d}:{seconds :02d}"if hours >0 else f"{minutes }:{seconds :02d}"
                                yturl =f"https://www.youtube.com/watch?v={vid_id }"
                                track_details ={'title':title ,'link':yturl ,'vidid':vid_id ,'duration_min':duration_min ,'thumb':thumbnail }
                                try :
                                    from Music .utils .mongo_cache import metadata_cache
                                    await metadata_cache .set (f'metadata_{vid_id }',track_details ,ttl =86400 *7 )
                                except Exception as e :
                                    logger .debug (f'Metadata cache storage failed: {e }')
                                logger .info (f'✓ YouTube API search succeeded for "{link }"')
                                return (track_details ,vid_id )
            except Exception as e :
                logger .debug (f'YouTube API search failed: {e }')

        if video_id_from_link :
            logger .warning (f'YouTube metadata extraction failed for {video_id_from_link }, using generic fallback metadata')
            track_details ={
            'title':f'Music {video_id_from_link }',
            'link':link ,
            'vidid':video_id_from_link ,
            'duration_min':'0:00',
            'thumb':f'https://i.ytimg.com/vi/{video_id_from_link }/maxresdefault.jpg'
            }
            try :
                from Music .utils .mongo_cache import metadata_cache
                await metadata_cache .set (f'metadata_{video_id_from_link }',track_details ,ttl =3600 )
            except Exception as e :
                logger .debug (f'Metadata cache storage failed: {e }')
            return (track_details ,video_id_from_link )

        raise ValueError ('ꜰᴀɪʟᴇᴅ ᴛᴏ ꜰᴇᴛᴄʜ ᴛʀᴀᴄᴋ ᴅᴇᴛᴀɪʟs. ᴛʀʏ ᴘʟᴀʏɪɴɢ ᴀɴʏ ᴏᴛʜᴇʀ.')

    async def download (self ,link :str ,mystic ,video :Union [bool ,str ]=None ,videoid :Union [bool ,str ]=None ,songaudio :Union [bool ,str ]=None ,songvideo :Union [bool ,str ]=None ,format_id :Union [bool ,str ]=None ,title :Union [bool ,str ]=None )->str :
        if videoid :
            vid_id =link
            youtube_url =self .base +link
        else :
            youtube_url =link
            vid_id =None
            if 'watch?v='in link :
                try :
                    vid_id =link .split ('watch?v=')[1 ].split ('&')[0 ]
                except Exception :
                    vid_id =None
            elif 'youtu.be/'in link :
                try :
                    vid_id =link .split ('youtu.be/')[1 ].split ('?')[0 ]
                except Exception :
                    vid_id =None

        if not YOUTUBE_ENABLED :
            logger .warning (f'YouTube downloads disabled by configuration; skipping download for {youtube_url }')
            return None

        safe_id =vid_id or re .sub (r'[^0-9A-Za-z]','',youtube_url )[:32 ]or 'youtube'
        safe_title =re .sub (r'[<>:\"/\\\\|?*]','',str (title or safe_id ))or safe_id

        async def external_only (filepath :str ,metric :str ,label :str ):
            return await _download_with_external_services_only (youtube_url ,filepath ,safe_id ,self ,metric ,label )

        if songvideo :
            return await external_only (f'downloads/{safe_title }.mp4','external_service_video','song-video' )
        if songaudio :
            return await external_only (f'downloads/{safe_title }.mp3','external_service','song-audio' )

        if video :
            downloaded_file =await external_only (os .path .join ('downloads',f'{safe_id }.mp4'),'external_service_video','video' )
        else :
            downloaded_file =await external_only (os .path .join ('downloads',f'{safe_id }.mp3'),'external_service','audio' )

        direct =True
        return (downloaded_file ,direct )
