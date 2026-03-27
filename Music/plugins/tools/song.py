import os
import re
import asyncio
import aiohttp
from concurrent .futures import ThreadPoolExecutor
import yt_dlp
import subprocess
import json
import sys
import io
import functools
from pyrogram import filters
from pyrogram .types import Message
from pyrogram .errors import MessageNotModified
import logging
from Music import app
from Music .utils .decorators .urls import no_preview_filter
from Music .utils .external_extractors import try_external_mp3_extraction
from config import BANNED_USERS ,YOUTUBE_PROXY ,YOUTUBE_ENABLED
from Music import LOGGER
logger =LOGGER (__name__ )

def _run_yt_dlp_suppressed (ydl_opts ,urls ):

    old_stderr =sys .stderr
    old_stdout =sys .stdout
    sys .stderr =io .StringIO ()
    sys .stdout =io .StringIO ()
    try :
        with yt_dlp .YoutubeDL (ydl_opts )as ydl :
            ydl .download (urls )
        return None
    except Exception as e :
        return str (e )
    finally :
        sys .stderr =old_stderr
        sys .stdout =old_stdout

def _extract_info_suppressed (url ):

    old_stdout =sys .stdout
    old_stderr =sys .stderr
    sys .stdout =io .StringIO ()
    sys .stderr =io .StringIO ()
    try :
        ydl_opts ={
        'quiet':True ,
        'no_warnings':True ,
        'skip_download':True ,
        'socket_timeout':15 ,
        }
        with yt_dlp .YoutubeDL (ydl_opts )as ydl :
            info =ydl .extract_info (url ,download =False )
            return info
    except Exception as e :
        err =str (e )
        if 'Sign in to confirm'in err :
            logger .debug ('Info extraction requires authentication (skipping)')
            return None
        logger .debug (f'Info extraction failed: {err [:200 ]}')
        return None
    finally :
        sys .stdout =old_stdout
        sys .stderr =old_stderr

def is_youtube_url (url :str )->bool :
    youtube_regex ='(https?://)?(www\\.)?(youtube|youtu|youtube-nocookie)\\.(com|be)/(watch\\?v=|embed/|v/|.+\\?v=)?([^&=%\\?]{11})'
    return bool (re .match (youtube_regex ,url ))

async def search_youtube (query :str ,limit :int =1 )->list :

    try :
        loop =asyncio .get_event_loop ()
        with ThreadPoolExecutor ()as executor :
            def _search ():
                ydl_opts ={
                'quiet':True ,
                'no_warnings':True ,
                'default_search':'ytsearch',
                'extract_flat':True ,
                }
                with yt_dlp .YoutubeDL (ydl_opts )as ydl :
                    results =ydl .extract_info (f'ytsearch{limit }:{query }',download =False )
                    return results .get ('entries',[])

            results =await loop .run_in_executor (None ,_search )
            return results
    except Exception as e :
        logger .error (f'YouTube search error: {type (e ).__name__ }: {e }')
        return []

def _format_views (views ):

    if not views :
        return "0"
    try :
        views_int =int (views )
        if views_int >=1000000 :
            return f"{views_int //1000000 }M"
        elif views_int >=1000 :
            return f"{views_int //1000 }K"
        else :
            return str (views_int )
    except (ValueError ,TypeError ):
        return str (views )

async def download_thumbnail (url :str ,filename :str )->str :
    try :
        async with aiohttp .ClientSession ()as session :
            async with session .get (url )as resp :
                if resp .status ==200 :
                    data =await resp .read ()
                    os .makedirs ('downloads',exist_ok =True )
                    filepath =f'downloads/{filename }'
                    with open (filepath ,'wb')as f :
                        f .write (data )
                    return filepath
    except Exception as e :
        logger .error (f'Thumbnail download failed: {e }')
    return None

@app .on_message (filters .command (['song'])&~BANNED_USERS &no_preview_filter )
async def song_download (client ,message :Message ):
    if not YOUTUBE_ENABLED :
        await message .reply_text ('YouTube downloading is disabled by configuration.',quote =True )
        return
    if len (message .command )<2 :
        return await message .reply_text ('Please provide a song name or YouTube URL.\n\nExample: `/song Believer` or `/song https://www.youtube.com/watch?v=7wtfhZwyrcc`',disable_web_page_preview =True )
    query =message .text .split (None ,1 )[1 ].strip ()
    if is_youtube_url (query ):
        video_url =query
    else :
        try :
            results =await search_youtube (query ,limit =1 )
            if not results :
                return await message .reply_text ('No results found for this song.')
            video =results [0 ]
            video_url =f"https://www.youtube.com/watch?v={video ['id']}"
        except Exception as e :
            logger .error (f'Search failed: {e }')
            return await message .reply_text ('Failed to search for the song.')
    processing_msg =await message .reply_text ('Downloading...')
    try :
        safe_title =re .sub ('[<>:"/\\\\|?*]','','Unknown - Unknown')
        filepath =f'downloads/{safe_title }.mp3'

        logger .info (f'Song request for: {query }')

        if not is_youtube_url (query ):
            try :
                results =await search_youtube (query ,limit =1 )
                if not results :
                    await processing_msg .delete ()
                    return await message .reply_text ('❌ No results found for this song.')
                video =results [0 ]
                video_url =f"https://www.youtube.com/watch?v={video ['id']}"
                title =video .get ('title','Unknown')

                uploader =video .get ('channel')or video .get ('uploader')or video .get ('artist')
                views_count =video .get ('view_count',0 )or 0
                upload_date =video .get ('upload_date','Unknown')

                if not uploader and isinstance (title ,str )and ' - 'in title :
                    parts =title .split (' - ',1 )
                    maybe_artist =parts [0 ].strip ()
                    maybe_title =parts [1 ].strip ()if len (parts )>1 else title
                    if maybe_artist and maybe_title :
                        uploader =maybe_artist
                        title =maybe_title
                if not uploader :
                    uploader ='Unknown'

                if uploader =='Unknown'or views_count ==0 :
                    try :
                        full_info =_extract_info_suppressed (video_url )
                        if full_info :
                            if uploader =='Unknown':
                                uploader =full_info .get ('uploader')or full_info .get ('channel')or 'Unknown'
                            if views_count ==0 :
                                views_count =full_info .get ('view_count',0 )or 0
                            if upload_date =='Unknown':
                                upload_date =full_info .get ('upload_date','Unknown')
                    except Exception :
                        pass

                duration =video .get ('duration',0 )or 0
                thumbnail_url =video .get ('thumbnails',[{}])[0 ].get ('url','')
                logger .info (f'Found video: {title } by {uploader }')
            except Exception as e :
                logger .error (f'Search failed: {e }')
                await processing_msg .delete ()
                return await message .reply_text ('❌ Failed to search for the song.')
        else :
            video_url =query
            title ='Unknown'
            uploader ='Unknown'
            duration =0
            thumbnail_url =''
            views_count =0
            upload_date ='Unknown'

            try :
                info =_extract_info_suppressed (video_url )
                if info :
                    title =info .get ('title',title )
                    uploader =info .get ('uploader')or info .get ('channel')or uploader
                    duration =info .get ('duration',duration )or duration
                    thumbnail_url =info .get ('thumbnails',[{}])[0 ].get ('url',thumbnail_url )
                    views_count =info .get ('view_count',0 )or 0
                    upload_date =info .get ('upload_date','Unknown')
            except Exception :
                pass

        logger .debug (f'Skipping metadata extraction to avoid YouTube authentication errors')

        safe_title =re .sub ('[<>:"/\\\\|?*]','',f'{title } - {uploader }')
        filepath =f'downloads/{safe_title }.mp3'

        download_success =False

        logger .info (f'[External-only] Trying third-party services for: {video_url }')
        try :
            result =await try_external_mp3_extraction (video_url ,filepath )
            if result and os .path .exists (filepath ):
                file_size =os .path .getsize (filepath )
                if file_size >10000 :
                    download_success =True
                    logger .info (f'✓ External service succeeded ({file_size } bytes)')
        except Exception as e :
            logger .debug (f'External extraction failed: {type (e ).__name__ }: {e }')

        if not download_success :
            try :
                await processing_msg .edit_text ('❌ Download failed. External services could not fetch this track.')
            except MessageNotModified :
                pass
            except Exception as e :
                logger .debug (f'Edit message failed: {e }')
            logger .error (f'All download attempts failed for: {title }')
            return

        thumb_path =None
        try :
            if thumbnail_url :
                try :
                    async with aiohttp .ClientSession ()as session :
                        async with session .get (thumbnail_url ,timeout =aiohttp .ClientTimeout (total =10 ))as resp :
                            if resp .status ==200 :
                                thumb_dir ='downloads'
                                os .makedirs (thumb_dir ,exist_ok =True )
                                thumb_filename =f'{re .sub ("[^a-zA-Z0-9]","",safe_title )}_thumb.jpg'
                                thumb_path =os .path .join (thumb_dir ,thumb_filename )
                                with open (thumb_path ,'wb')as f :
                                    f .write (await resp .read ())
                except Exception as e :
                    logger .debug (f'Thumbnail download failed: {e }')

            if os .path .exists (filepath ):
                file_size =os .path .getsize (filepath )
                logger .info (f'Sending audio file: {filepath } ({file_size } bytes)')

                upload_date_str ='Unknown'
                if upload_date and upload_date !='Unknown':
                    try :
                        import datetime
                        dt =datetime .datetime .strptime (str (upload_date ),'%Y%m%d')
                        upload_date_str =dt .strftime ('%Y-%m-%d')
                    except :
                        upload_date_str =upload_date

                formatted_views =_format_views (views_count )

                try :
                    channel_link =f"https://www.youtube.com/c/{uploader .replace (' ','')}"if uploader and uploader !='Unknown'else ""
                    track_info =f"😲 <b>ᴛʀᴀᴄᴋ ɪɴғᴏʀᴍᴀᴛɪᴏɴ</b> 😲\n\n📌 <b>ᴛɪᴛʟᴇ :</b> {title }\n\n⏳ <b>ᴅᴜʀᴀᴛɪᴏɴ :</b> {int (duration //60 )} ᴍɪɴᴜᴛᴇs\n👀 <b>ᴠɪᴇᴡs :</b> <code>{formatted_views }</code>\n⏰ <b>ᴩᴜʙʟɪsʜᴇᴅ ᴏɴ :</b> {upload_date_str }\n📎 <b>ᴄʜᴀɴɴᴇʟ :</b> {uploader }\n\n<u><b>🥀 sᴇᴀʀᴄʜ ᴩᴼᴡᴇʀᴇᴅ ʙʏ MoonMuzzBot</b></u>"
                    if thumb_path and os .path .exists (thumb_path ):
                        await message .reply_photo (
                        photo =thumb_path ,
                        caption =track_info ,
                        parse_mode ='html'
                        )
                    else :
                        await message .reply_text (
                        track_info ,
                        parse_mode ='html'
                        )
                except Exception as info_e :
                    logger .debug (f'Failed to send track info: {info_e }')

                await message .reply_audio (
                audio =filepath ,
                title =title ,
                performer =uploader ,
                duration =int (duration )if duration else 0 ,
                thumb =thumb_path if thumb_path and os .path .exists (thumb_path )else None ,
                caption ='@MoonMuzzBot'
                )

                await processing_msg .delete ()
            else :
                try :
                    await processing_msg .edit_text ('❌ File not found after download.')
                except MessageNotModified :
                    pass
                except Exception as e :
                    logger .debug (f'Edit message failed: {e }')

        except Exception as e :
            logger .error (f'Failed to send audio: {e }')
            try :
                await processing_msg .edit_text (f'❌ Failed to send audio file.')
            except MessageNotModified :
                pass
            except Exception as e :
                logger .debug (f'Edit message failed: {e }')

        finally :

            try :
                if os .path .exists (filepath ):
                    os .remove (filepath )
                    logger .debug (f'Cleaned up: {filepath }')
            except :
                pass
            try :
                if thumb_path and os .path .exists (thumb_path ):
                    os .remove (thumb_path )
                    logger .debug (f'Cleaned up: {thumb_path }')
            except :
                pass

    except Exception as e :
        logger .error (f'Song download error: {e }')
        try :
            try :
                await processing_msg .edit_text (f'❌ Error: {str (e )[:50 ]}')
            except MessageNotModified :
                pass
            except Exception :
                pass
        except :
            pass
