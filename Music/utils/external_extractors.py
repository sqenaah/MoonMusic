
import asyncio
import aiohttp
from typing import Optional
import logging
import os
import random
import time
import re
from config import EXTERNAL_SERVICES_MAX_ATTEMPT

logger =logging .getLogger (__name__ )

INVIDIOUS_INSTANCES =['https://yewtu.be','https://invidious.snopyta.org','https://invidious.kavin.rocks','https://iv.ggtyler.dev','https://invidious.sethforprivacy.com','https://inv.riverside.rocks','https://invidio.us','https://inv.nadeko.net','https://vid.puffyan.us','https://invidious.nerdvpn.de','https://invidious.slipfox.xyz','https://inv.vern.cc']

EXTERNAL_SERVICES =[
{
'name':'cobalt-audio',
'api':'https://api.cobalt.tools/api/json',
'extractor':'youtube',
'format_param':'audio',
'timeout':60
},
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

                        async with session .get (invidious_url ,headers =headers )as resp :
                            if resp .status ==200 :
                                data =await resp .json ()

                                formats =data .get ('formatStreams',[])or []
                                audio_formats =[f for f in formats if f .get ('type','').startswith ('audio')]

                                if audio_formats :
                                    audio_format =audio_formats [0 ]
                                    audio_url =audio_format .get ('url')

                                    if audio_url :
                                        async with session .get (audio_url ,headers =headers ,timeout =aiohttp .ClientTimeout (total =timeout ))as audio_resp :
                                            if audio_resp .status ==200 :
                                                content =await audio_resp .read ()
                                                if len (content )>50000 :
                                                    with open (filepath ,'wb')as f :
                                                        f .write (content )
                                                    logger .info (f'✓ Invidious extraction succeeded ({instance }): {len (content )//1024 }KB')
                                                    return filepath
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

        if max_attempts is None :
            max_attempts = EXTERNAL_SERVICES_MAX_ATTEMPT

        services =EXTERNAL_SERVICES .copy ()
        # DO NOT SHUFFLE - maintain priority order of services
        # Limit to top N services by default for speed
        services_to_try = services [:max_attempts]

        start =time .monotonic ()
        failed_services =[]
        working_services =[]

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
                    headers ={
                    'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
                    'Accept':'*/*',
                    }

                    try :
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

        services_str =', '.join ([s .get ('name','unknown')for s in services_to_try ])
        logger .warning (f'⚠️  External service limit reached ({len (services_to_try )}/{len(services)} tried). Services attempted: {services_str }. To try all services, set EXTERNAL_SERVICES_MAX_ATTEMPT=13 or higher.')
        if working_services :
            logger .info (f'Working services: {", ".join (working_services )}')
        if failed_services :
            logger .debug (f'Failed services: {", ".join (failed_services [:5 ])}')

        logger .error ('❌ All configured external extraction services failed. Direct YouTube, Invidious, and pytube download paths are disabled in this build.')
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
