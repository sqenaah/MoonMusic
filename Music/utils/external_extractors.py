
import asyncio
import aiohttp
from typing import Optional
import logging
import os
import random
import time
import re
from config import (
    ALLOW_INSECURE_INVIDIOUS_SSL,
    COBALT_API_KEY,
    COBALT_API_URL,
    COBALT_BEARER_TOKEN,
    ENABLE_LEGACY_EXTERNAL_SERVICES,
    EXTERNAL_SERVICES_MAX_ATTEMPT,
)

logger =logging .getLogger (__name__ )

INVIDIOUS_INSTANCES =['https://yewtu.be','https://invidious.snopyta.org','https://invidious.kavin.rocks','https://iv.ggtyler.dev','https://invidious.sethforprivacy.com','https://inv.riverside.rocks','https://invidio.us','https://inv.nadeko.net','https://vid.puffyan.us','https://invidious.nerdvpn.de','https://invidious.slipfox.xyz','https://inv.vern.cc']

EXTERNAL_SERVICES =[
{
'name':'cobalt',
'api':COBALT_API_URL,
'method':'POST_JSON',
'timeout':60
},
]

LEGACY_EXTERNAL_SERVICES =[
{
'name':'mdown-youtube',
'api':'https://mdown.com/api/v1/youtube',
'method':'POST',
'url_param':'url',
'timeout':60
},
{
'name':'downloader-io',
'api':'https://downloader.io/api/youtube',
'method':'POST',
'url_param':'url',
'timeout':60
},
{
'name':'y2download-api',
'api':'https://y2download.net/api/v1/convert',
'method':'POST',
'url_param':'url',
'timeout':60
},
{
'name':'tuneto-mp3',
'api':'https://tuneto.net/api/convert',
'method':'POST',
'url_param':'url',
'timeout':60
},
{
'name':'snap-youtube',
'api':'https://snaptik.pro/api/convert',
'method':'POST',
'url_param':'url',
'timeout':60
},
{
'name':'mp3-convert1',
'api':'https://mp3-convert.com/api/download',
'method':'POST',
'url_param':'url',
'timeout':60
},
{
'name':'getfbstuff-mp3',
'api':'https://getfbstuff.com/api/youtube/info',
'method':'POST',
'url_param':'url',
'timeout':60
},
{
'name':'veadotube-mp3',
'api':'https://www.veadotube.com/api/convert',
'method':'POST',
'url_param':'url',
'timeout':60
},
{
'name':'youtube-downloader-plus',
'api':'https://youtube-downloader.art/api/convert',
'method':'POST',
'url_param':'url',
'timeout':60
},
{
'name':'clip2audio-direct',
'api':'https://clip2audio.com/api/download',
'method':'POST',
'url_param':'url',
'timeout':60
},
{
'name':'mixkit-free',
'api':'https://mixkit.co/api/v1/sounds/',
'method':'GET',
'url_param':'search',
'timeout':60
},
{
'name':'freesound-api',
'api':'https://freesound.org/api/v2/search/text/',
'method':'GET',
'url_param':'query',
'timeout':60
},
]

def _normalize_service_url (url :str )->Optional [str ]:
    if not url :
        return None
    return url .rstrip ('/')+'/'

def _build_external_services ():
    services =[]
    cobalt_url =_normalize_service_url (COBALT_API_URL )
    if cobalt_url :
        cobalt_headers ={
        'Accept':'application/json',
        'Content-Type':'application/json',
        'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'
        }
        if COBALT_API_KEY :
            cobalt_headers ['Authorization']=f'Api-Key {COBALT_API_KEY }'
        elif COBALT_BEARER_TOKEN :
            cobalt_headers ['Authorization']=f'Bearer {COBALT_BEARER_TOKEN }'
        services .append ({
        **EXTERNAL_SERVICES [0 ],
        'api':cobalt_url ,
        'headers':cobalt_headers ,
        'service_type':'cobalt'
        })
    if ENABLE_LEGACY_EXTERNAL_SERVICES :
        services .extend (LEGACY_EXTERNAL_SERVICES )
    return services

async def _save_download_to_file (session ,download_url :str ,filepath :str ,timeout ,headers :dict =None )->Optional [str ]:
    try :
        async with session .get (download_url ,timeout =timeout ,headers =headers or {})as dl :
            if dl .status !=200 :
                return None
            content =await dl .read ()
            if len (content )<=50000 :
                return None
            os .makedirs (os .path .dirname (filepath )or '.',exist_ok =True )
            with open (filepath ,'wb')as f :
                f .write (content )
            return filepath
    except Exception :
        return None

def _extract_download_url_from_json (data :dict )->Optional [str ]:
    if not isinstance (data ,dict ):
        return None
    return data .get ('url')or data .get ('downloadLink')or data .get ('link')

async def _try_cobalt_extraction (session ,service :dict ,video_url :str ,filepath :str ,service_timeout ,idx :int ,total :int )->Optional [str ]:
    payload ={
    'url':video_url ,
    'downloadMode':'audio',
    'audioFormat':'mp3',
    'audioBitrate':'128',
    'alwaysProxy':True ,
    'filenameStyle':'basic',
    'youtubeBetterAudio':True
    }
    headers =service .get ('headers',{})
    try :
        async with session .post (service ['api'],json =payload ,headers =headers )as resp :
            response_text =await resp .text ()
            try :
                data =await resp .json (content_type =None )
            except Exception :
                data =None

            if resp .status !=200 :
                error_code =None
                if isinstance (data ,dict ):
                    error_code =((data .get ('error')or {}).get ('code'))
                if error_code =='error.api.auth.jwt.missing':
                    logger .error ('❌ cobalt requires authentication. Set COBALT_BEARER_TOKEN or use a self-hosted cobalt instance in COBALT_API_URL.')
                elif error_code =='error.api.auth.key.missing':
                    logger .error ('❌ cobalt requires an API key. Set COBALT_API_KEY or use a self-hosted cobalt instance.')
                elif error_code =='error.api.auth.key.invalid':
                    logger .error ('❌ cobalt API key is invalid. Update COBALT_API_KEY or switch to a valid instance.')
                else :
                    snippet =(response_text or '')[:180 ].replace ('\n',' ')
                    logger .warning (f'cobalt request failed with HTTP {resp .status }: {snippet }')
                return None

            if not isinstance (data ,dict ):
                snippet =(response_text or '')[:180 ].replace ('\n',' ')
                logger .warning (f'cobalt returned a non-JSON response: {snippet }')
                return None

            status =data .get ('status')
            download_url =None
            if status in ('redirect','tunnel'):
                download_url =data .get ('url')
            elif status =='local-processing':
                tunnel_urls =data .get ('tunnel')or []
                if tunnel_urls :
                    download_url =tunnel_urls [0 ]
            elif status =='picker':
                download_url =data .get ('audio')
            elif status =='error':
                error_code =((data .get ('error')or {}).get ('code','unknown'))
                logger .warning (f'cobalt returned API error: {error_code }')
                return None

            if not download_url :
                logger .warning (f'cobalt did not return a downloadable audio URL (status={status })')
                return None

            saved =await _save_download_to_file (session ,download_url ,filepath ,service_timeout ,headers =headers )
            if saved :
                size_kb =max (1 ,os .path .getsize (filepath )//1024 )
                logger .info (f'✅ [{idx}/{total }] cobalt succeeded ({size_kb }KB)')
                return saved
            logger .warning ('cobalt returned a download URL but the file fetch did not produce usable audio')
            return None
    except asyncio .TimeoutError :
        logger .debug ('cobalt: API call timeout')
        return None
    except Exception as e :
        logger .debug (f'cobalt API error: {type (e ).__name__ } {e }')
        return None

async def try_invidious_extraction (video_url :str ,filepath :str ,timeout :int =60 )->Optional [str ]:

    try :

        vid_match =re .search (r'(?:youtube\.com\/watch\?v=|youtu\.be\/)([^&\n?#]+)',video_url )
        if not vid_match :
            logger .warning ('Could not extract video ID from URL for Invidious')
            return None

        vid_id =vid_match .group (1 )
        instances =INVIDIOUS_INSTANCES .copy ()
        random .shuffle (instances )

        os .makedirs (os .path .dirname (filepath )or '.',exist_ok =True )

        failed_instances =[]
        for instance in instances :
            invidious_url =f'{instance .rstrip ("/")}/api/v1/videos/{vid_id }'
            logger .debug (f'Trying Invidious instance: {instance }')

            for attempt in range (2 ):
                try :
                    async with aiohttp .ClientSession (timeout =aiohttp .ClientTimeout (total =timeout ))as session :
                        headers ={
                        'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                        'Accept':'*/*'
                        }

                        async with session .get (invidious_url ,headers =headers ,ssl =False if ALLOW_INSECURE_INVIDIOUS_SSL else None )as resp :
                            if resp .status ==200 :
                                data =await resp .json (content_type =None )

                                formats =[]
                                for key in ('audioStreams','adaptiveFormats','formatStreams'):
                                    items =data .get (key ,[])or []
                                    if isinstance (items ,list ):
                                        formats .extend (items )

                                audio_formats =[
                                f for f in formats
                                if isinstance (f ,dict )and f .get ('url')and (
                                    str (f .get ('type','')).lower ().startswith ('audio')
                                    or 'audio'in str (f .get ('type','')).lower ()
                                    or 'audio'in str (f .get ('mimeType','')).lower ()
                                )
                                ]

                                if audio_formats :
                                    audio_format =audio_formats [0 ]
                                    audio_url =audio_format .get ('url')

                                    if audio_url :
                                        async with session .get (audio_url ,headers =headers ,timeout =aiohttp .ClientTimeout (total =timeout ),ssl =False if ALLOW_INSECURE_INVIDIOUS_SSL else None )as audio_resp :
                                            if audio_resp .status ==200 :
                                                content =await audio_resp .read ()
                                                if len (content )>50000 :
                                                    with open (filepath ,'wb')as f :
                                                        f .write (content )
                                                    logger .info (f'✓ Invidious extraction succeeded ({instance }): {len (content )//1024 }KB')
                                                    return filepath

                                hls_url =data .get ('hlsUrl')
                                if hls_url :
                                    proc =await asyncio .create_subprocess_exec (
                                    'ffmpeg',
                                    '-y',
                                    '-i',
                                    hls_url ,
                                    '-vn',
                                    '-acodec',
                                    'libmp3lame',
                                    '-b:a',
                                    '128k',
                                    filepath ,
                                    stdout =asyncio .subprocess .PIPE ,
                                    stderr =asyncio .subprocess .PIPE ,
                                    )
                                    _stdout ,stderr =await proc .communicate ()
                                    if proc .returncode ==0 and os .path .exists (filepath )and os .path .getsize (filepath )>50000 :
                                        logger .info (f'✓ Invidious HLS extraction succeeded ({instance })')
                                        return filepath
                                    logger .debug (f'Invidious HLS extraction failed on {instance }: {stderr .decode ("utf-8","ignore")[:160 ]}')
                except asyncio .TimeoutError :
                    logger .debug (f'Invidious {instance } timeout on attempt {attempt +1 }')
                except Exception as e :
                    logger .debug (f'Invidious {instance } error on attempt {attempt +1 }: {type (e ).__name__ }')

                await asyncio .sleep (0.5 *(attempt +1 ))

            logger .debug (f'Invidious instance {instance } failed after retries')
            failed_instances .append (instance )

        logger .warning (f'All Invidious instances exhausted ({len (instances )} tried)')
        return None
    except Exception as e :
        logger .debug (f'Invidious extraction fatal error: {type (e ).__name__ }: {e }')
        return None

async def try_external_mp3_extraction (video_url :str ,filepath :str ,timeout :int =90 ,max_attempts :int =None )->Optional [str ]:

    try :

        services =_build_external_services ()
        if not services :
            logger .error ('❌ No supported external extractor is configured. Set COBALT_API_URL and auth, or enable ENABLE_LEGACY_EXTERNAL_SERVICES=1 for legacy public endpoints.')
            return None

        if max_attempts is None :
            max_attempts = EXTERNAL_SERVICES_MAX_ATTEMPT

        services_to_try =services [:max_attempts ]if max_attempts >0 else services

        start =time .monotonic ()
        failed_services =[]

        for idx ,service in enumerate (services_to_try ,1 ):

            elapsed =time .monotonic ()-start
            if elapsed >=timeout :
                logger .warning ("External extraction overall timeout reached")
                break

            service_name =service .get ('name','unknown')
            # smaller per-service timeout to speed up retries
            service_timeout =aiohttp .ClientTimeout (total =min (12 ,service .get ('timeout',15 )))

            method =service .get ('method','POST').upper ()
            url_param =service .get ('url_param','url')
            
            # Show which service is being tried (higher visibility)
            logger .info (f'   Trying [{idx}/{len (services )}] {service_name }...')

            try :
                async with aiohttp .ClientSession (timeout =service_timeout )as session :
                    headers =service .get ('headers')or {
                    'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
                    'Accept':'*/*',
                    }

                    try :
                        if service .get ('service_type')=='cobalt':
                            cobalt_result =await _try_cobalt_extraction (session ,service ,video_url ,filepath ,service_timeout ,idx ,len (services_to_try ))
                            if cobalt_result :
                                return cobalt_result
                            failed_services .append (service_name )
                            continue
                        if method =='GET':
                            async with session .get (service ['api'],params ={url_param :video_url },headers =headers )as resp :
                                if resp .status ==200 :
                                    logger .debug (f"{service_name } API success")

                                    mp3_url =None
                                    try :
                                        data =await resp .json ()
                                        mp3_url =data .get ('url')or data .get ('downloadLink')or data .get ('link')
                                    except Exception :

                                        try :
                                            content_type =resp .headers .get ('Content-Type','')
                                            if 'audio'in content_type :
                                                content =await resp .read ()
                                                if len (content )>50000 :
                                                    os .makedirs (os .path .dirname (filepath )or '.',exist_ok =True )
                                                    with open (filepath ,'wb')as f :
                                                        f .write (content )
                                                    logger .info (f'✅ [{idx}] {service_name } succeeded (direct stream {len (content )//1024 }KB)')
                                                    return filepath
                                        except Exception :
                                            logger .debug (f'{service_name }: failed to read direct stream')

                                    if mp3_url :
                                        try :
                                            async with session .get (mp3_url ,timeout =service_timeout )as dl :
                                                logger .debug (f"{service_name } download response: {dl .status }")
                                                if dl .status ==200 :
                                                    content =await dl .read ()
                                                    if len (content )>50000 :
                                                        os .makedirs (os .path .dirname (filepath )or '.',exist_ok =True )
                                                        with open (filepath ,'wb')as f :
                                                            f .write (content )
                                                        logger .info (f'✅ [{idx}] {service_name } succeeded ({len (content )//1024 }KB)')
                                                        return filepath
                                        except asyncio .TimeoutError :
                                            logger .debug (f'{service_name }: download timeout')
                                        except Exception as dl_e :
                                            logger .debug (f'{service_name }: download failed: {type (dl_e ).__name__ } {dl_e }')
                        else :
                            async with session .post (service ['api'],data ={url_param :video_url },headers =headers )as resp :
                                if resp .status ==200 :
                                    mp3_url =None
                                    try :
                                        data =await resp .json ()
                                        mp3_url =data .get ('url')or data .get ('downloadLink')or data .get ('link')
                                    except Exception :
                                        mp3_url =None

                                    if mp3_url :
                                        try :
                                            async with session .get (mp3_url ,timeout =service_timeout )as dl :
                                                logger .debug (f"{service_name } download response: {dl .status }")
                                                if dl .status ==200 :
                                                    content =await dl .read ()
                                                    if len (content )>50000 :
                                                        os .makedirs (os .path .dirname (filepath )or '.',exist_ok =True )
                                                        with open (filepath ,'wb')as f :
                                                            f .write (content )
                                                        logger .info (f'✅ [{idx}] {service_name } succeeded ({len (content )//1024 }KB)')
                                                        return filepath
                                        except asyncio .TimeoutError :
                                            logger .debug (f'{service_name }: download timeout')
                                        except Exception as dl_e :
                                            logger .debug (f'{service_name }: download failed: {type (dl_e ).__name__ } {dl_e }')
                                else :
                                    logger .debug (f'{service_name } POST: HTTP {resp .status }')
                    except asyncio .TimeoutError :
                        logger .debug (f'{service_name }: API call timeout')
                    except Exception as e :
                        logger .debug (f'{service_name }: API error: {type (e ).__name__ } {e }')

            except asyncio .TimeoutError :
                logger .debug (f'{service_name }: session timeout')
                failed_services .append (f'{service_name } (timeout)')
            except Exception as service_error :
                logger .debug (f'{service_name }: connection error: {type (service_error ).__name__ } {service_error }')
                failed_services .append (f'{service_name } ({type (service_error ).__name__ })')

            await asyncio .sleep (0.15 )

        services_str =', '.join ([s .get ('name','unknown')for s in services_to_try ])or 'none'
        if len (services_to_try )<len (services ):
            logger .warning (f'⚠️  External service limit reached ({len (services_to_try )}/{len(services)} tried). Services attempted: {services_str }. Increase EXTERNAL_SERVICES_MAX_ATTEMPT to try more configured services.')
        else :
            logger .warning (f'⚠️  All configured external services were exhausted. Services attempted: {services_str }.')
        if failed_services :
            logger .debug (f'Failed services: {", ".join (failed_services [:5 ])}')

        logger .error ('❌ All configured external extraction services failed. The downloader will fall back to any remaining third-party proxies handled by the caller.')
        return None
    except Exception as outer_e :
        logger .error (f"External extraction fatal error: {type (outer_e ).__name__ }: {outer_e }")
        return None

async def retry_with_backoff (func ,max_retries =3 ,base_delay =2 ):

    for attempt in range (max_retries ):
        try :
            return await func ()
        except Exception as e :
            if attempt ==max_retries -1 :
                logger .warning (f"Retry failed after {max_retries } attempts")
                raise

            delay =base_delay **(attempt +1 )
            logger .debug (f"Retry backoff: waiting {delay }s before attempt {attempt +2 }/{max_retries }")
            await asyncio .sleep (delay )
    return None
