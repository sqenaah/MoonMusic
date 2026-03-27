import asyncio
import importlib
import sys
from pyrogram import idle
from pyrogram import errors as pyrogram_errors
from pyrogram.errors import FloodWait ,UnknownError

if not hasattr (pyrogram_errors ,'GroupcallForbidden'):
    from pyrogram.errors import RPCError
    class GroupcallForbidden (RPCError ):

        CODE =400
        NAME ="GROUPCALL_FORBIDDEN"
    pyrogram_errors .GroupcallForbidden =GroupcallForbidden

from pytgcalls.exceptions import NoActiveGroupCall
from ntgcalls import TelegramServerError
import config
from Music import LOGGER ,app ,userbot
from Music.core.call import Anony
from Music.misc import sudo
from Music.plugins import ALL_MODULES
from Music.utils.database import get_banned_users ,get_gbanned
from Music.utils.error_handler import ErrorHandler ,handle_unknown_constructor
from config import BANNED_USERS

RECONNECT_ATTEMPTS =0
MAX_RECONNECT_ATTEMPTS =5

async def init ():
    if not config .STRING1 and (not config .STRING2 )and (not config .STRING3 )and (not config .STRING4 )and (not config .STRING5 ):
        LOGGER (__name__ ).error ('Assistant client variables not defined, exiting...')
        exit ()

    try :
        users =await get_banned_users ()
        for user_id in users :
            BANNED_USERS .add (user_id )
    except Exception as e :
        LOGGER (__name__ ).warning (f"Failed to load banned users: {e }")

    try :
        await app .start ()
        LOGGER (__name__ ).info ("✓ Bot started successfully")
    except FloodWait as e :
        LOGGER (__name__ ).warning (f"FloodWait: waiting {e .value }s before starting bot...")
        await asyncio .sleep (e .value )
        await app .start ()
    except (UnknownError ,TelegramServerError )as e :
        error_hint =handle_unknown_constructor (str (e ))
        if error_hint :
            LOGGER (__name__ ).error (error_hint )
        LOGGER (__name__ ).warning ("Retrying bot start after server error...")
        await asyncio .sleep (3 )
        await app .start ()
    except Exception as e :
        LOGGER (__name__ ).error (f"Failed to start bot: {type (e ).__name__ }: {e }")
        raise

    for all_module in ALL_MODULES :
        try :
            importlib .import_module ('Music.plugins'+all_module )
        except Exception as e :
            LOGGER (__name__ ).error (f"Failed to import plugin {all_module }: {e }")

    LOGGER ('Music.plugins').info ('Successfully Imported Modules...')

    try :
        await userbot .start ()
        LOGGER (__name__ ).info ("✓ Userbot started successfully")
    except FloodWait as e :
        LOGGER (__name__ ).warning (f"FloodWait for userbot: waiting {e .value }s...")
        await asyncio .sleep (e .value )
        await userbot .start ()
    except Exception as e :
        LOGGER (__name__ ).error (f"Failed to start userbot: {type (e ).__name__ }: {e }")

    try :
        await Anony .start ()
        LOGGER (__name__ ).info ("✓ PyTgCalls started successfully")
    except Exception as e :
        LOGGER (__name__ ).error (f"Failed to start PyTgCalls: {type (e ).__name__ }: {e }")
        raise

    try :
        await app .get_chat (config .LOGGER_ID )
        await Anony .stream_call ('https://image2url.com/r2/default/videos/1769268795930-72965ce5-60f7-4bf2-bdcd-e5a49cce8ad4.mp4')
    except NoActiveGroupCall :
        LOGGER ('Music').error ('Please enable videochat in your log group/channel.\n\nStopping Bot...')
        exit ()
    except (pyrogram_errors .PeerIdInvalid ,pyrogram_errors .ChannelInvalid ):
        LOGGER (__name__ ).warning (f"Skipping log-group stream test because LOGGER_ID {config .LOGGER_ID } is not accessible to the bot.")
    except Exception as e :
        LOGGER (__name__ ).warning (f"Stream test failed (non-critical): {type (e ).__name__ }: {e }")

    try :
        await Anony .decorators ()
    except Exception as e :
        LOGGER (__name__ ).warning (f"Failed to setup decorators: {e }")

    LOGGER (__name__ ).info ("✓ All systems initialized successfully!")
    await idle ()

    await app .stop ()
    LOGGER ('Music').info ('Stopping Music Bot...')

def _handle_loop_exception (loop ,context ):

    try :
        error =context .get ('exception')
        msg =context .get ('message','Unknown error')

        if error :

            error_str =str (error )
            if "unknown constructor"in error_str .lower ()or "0x"in error_str :
                LOGGER (__name__ ).warning (f"TL Schema error (transient): {error_str }")

                return

            if isinstance (error ,TelegramServerError ):
                LOGGER (__name__ ).warning (f"Telegram server error (transient): {error }")
                return

            if isinstance (error ,UnknownError ):
                LOGGER (__name__ ).warning (f"Telegram API error (may be transient): {error }")
                return

            if "Peer id invalid"in error_str :
                LOGGER (__name__ ).warning (f"Peer error handled non-fatally: {error_str }")
                return

            LOGGER (__name__ ).error (f'Unhandled exception in event loop: {error }',exc_info =True )
        else :
            LOGGER (__name__ ).error (f'Event loop error: {msg }',exc_info =True )
    except Exception as inner_error :
        LOGGER (__name__ ).error (f'Error in exception handler itself: {inner_error }')

def _install_signal_handlers (loop ):

    import signal

    shutdown_flag ={'triggered':False }

    def _shutdown (signame ):
        if shutdown_flag ['triggered']:
            LOGGER (__name__ ).warning (f"Force shutdown triggered (signal {signame } received again)")
            sys .exit (1 )

        shutdown_flag ['triggered']=True
        LOGGER (__name__ ).info (f"Received signal {signame }; starting graceful shutdown...")

        async def cleanup ():
            try :
                if app .is_connected :
                    LOGGER (__name__ ).info ("Stopping main bot...")
                    await app .stop ()
            except Exception as e :
                LOGGER (__name__ ).warning (f"Error stopping bot: {e }")

            try :
                if userbot .one .is_connected :
                    LOGGER (__name__ ).info ("Stopping assistant clients...")
                    await userbot .one .stop ()
            except :
                pass

            try :
                if Anony .one .is_started ():
                    LOGGER (__name__ ).info ("Stopping PyTgCalls...")
                    await Anony .one .stop ()
            except :
                pass

        asyncio .ensure_future (cleanup ())

    for sig_name in ('SIGINT','SIGTERM'):
        try :
            loop .add_signal_handler (
            getattr (signal ,sig_name ),
            lambda s =sig_name :_shutdown (s )
            )
            LOGGER (__name__ ).debug (f"Signal handler registered for {sig_name }")
        except (NotImplementedError ,ValueError ):

            LOGGER (__name__ ).debug (f"Signal handler {sig_name } not supported on this platform")

if __name__ =='__main__':
    loop =asyncio .get_event_loop ()
    loop .set_exception_handler (_handle_loop_exception )
    _install_signal_handlers (loop )

    async def _safe_init ():
        global RECONNECT_ATTEMPTS
        wait_time =5

        while True :
            try :
                RECONNECT_ATTEMPTS =0
                await init ()
                break
            except KeyboardInterrupt :
                LOGGER (__name__ ).info ("Shutdown signal received")
                try :
                    if app .is_connected :
                        await app .stop ()
                except :
                    pass
                break
            except Exception as e :
                error_msg =str (e )
                error_type =type (e ).__name__

                LOGGER (__name__ ).error (f'Failed in init(): {error_type }: {error_msg }')

                if error_type =="AuthKeyDuplicated":
                    LOGGER (__name__ ).critical ("Session conflict detected (AUTH_KEY_DUPLICATED). Container will exit for clean restart.")
                    try :
                        if app .is_connected :
                            await app .stop ()
                    except :
                        pass
                    sys .exit (1 )

                if "unknown constructor"in error_msg .lower ():
                    LOGGER (__name__ ).warning ("TL schema mismatch detected - bot will recover automatically")

                try :
                    if app .is_connected :
                        await app .stop ()
                except :
                    pass

                RECONNECT_ATTEMPTS +=1
                if RECONNECT_ATTEMPTS >MAX_RECONNECT_ATTEMPTS :
                    LOGGER (__name__ ).critical (f"Max reconnection attempts ({MAX_RECONNECT_ATTEMPTS }) reached. Exiting.")
                    sys .exit (1 )

                wait_time =min (5 *(2 **RECONNECT_ATTEMPTS ),300 )
                LOGGER (__name__ ).info (f"Reconnecting in {wait_time }s (attempt {RECONNECT_ATTEMPTS }/{MAX_RECONNECT_ATTEMPTS })...")
                await asyncio .sleep (wait_time )

    try :
        loop .run_until_complete (_safe_init ())
    except KeyboardInterrupt :
        LOGGER (__name__ ).info ("Bot shutdown complete")
    except Exception as e :
        LOGGER (__name__ ).critical (f"Fatal error: {type (e ).__name__ }: {e }",exc_info =True )
        sys .exit (1 )
    finally :
        try :
            loop .run_until_complete (loop .shutdown_asyncgens ())
        except Exception :
            pass
        loop .close ()
