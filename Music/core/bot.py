from pyrogram import Client ,errors
from pyrogram.enums import ChatMemberStatus ,ParseMode
from pyrogram.types import BotCommand ,BotCommandScopeDefault ,BotCommandScopeAllGroupChats
import config
from ..logging import LOGGER

class Anony (Client ):

    def __init__ (self ):
        LOGGER (__name__ ).info (f'Starting Bot...')
        super ().__init__ (name ='MusicBot',api_id =config .API_ID ,api_hash =config .API_HASH ,bot_token =config .BOT_TOKEN ,in_memory =True ,parse_mode =ParseMode .HTML ,max_concurrent_transmissions =7 )

    async def start (self ):
        await super ().start ()
        self .id =self .me .id
        self .name =self .me .first_name +' '+(self .me .last_name or '')
        self .username =self .me .username
        self .mention =self .me .mention

        private_commands =[
        BotCommand ('start','Start the bot'),
        BotCommand ('song','Download a song'),
        BotCommand ('help','Show help message'),
        ]

        group_commands =[
        BotCommand ('play','Play music'),
        BotCommand ('song','Download a song'),
        BotCommand ('queue','Show queue'),
        BotCommand ('pause','Pause music'),
        BotCommand ('resume','Resume music'),
        BotCommand ('skip','Skip current song'),
        BotCommand ('stop','Stop music'),
        BotCommand ('shuffle','Shuffle queue'),
        BotCommand ('settings','Bot settings'),
        ]

        try :
            await self .set_bot_commands (
            commands =private_commands ,
            scope =BotCommandScopeDefault ()
            )
            await self .set_bot_commands (
            commands =group_commands ,
            scope =BotCommandScopeAllGroupChats ()
            )
            LOGGER (__name__ ).info ('Bot commands configured successfully')
        except Exception as e :
            LOGGER (__name__ ).warning (f'Failed to set bot commands: {e }')

        log_chat_accessible =False
        try :
            LOGGER (__name__ ).info (f'Attempting to access log group/channel: {config .LOGGER_ID }')
            await self .send_message (chat_id =config .LOGGER_ID ,text =f'<u><b>» {self .mention } ʙᴏᴛ sᴛᴀʀᴛᴇᴅ :</b></u>\n\nɪᴅ : <code>{self .id }</code>\nɴᴀᴍᴇ : {self .name }\nᴜsᴇʀɴᴀᴍᴇ : @{self .username }')
            LOGGER (__name__ ).info ('✓ Startup message sent to log group')
            log_chat_accessible =True
        except (errors .ChannelInvalid ,errors .PeerIdInvalid ):
            LOGGER (__name__ ).warning (f'Cannot access log group/channel {config .LOGGER_ID }. Bot will continue running but startup notifications will not be sent. Make sure bot is added to the group and has admin permissions.')
        except (errors .BadRequest ,errors .Forbidden ):
            LOGGER (__name__ ).warning (f'Bot does not have permission to access log group/channel {config .LOGGER_ID }. Bot will continue running.')
        except Exception as ex :
            error_str =str (ex )
            LOGGER (__name__ ).warning (f'Could not access log group/channel {config .LOGGER_ID }: {type (ex ).__name__ }: {error_str }')

        if log_chat_accessible :
            try :
                a =await self .get_chat_member (config .LOGGER_ID ,self .id )
                if a .status !=ChatMemberStatus .ADMINISTRATOR :
                    LOGGER (__name__ ).warning (f'Bot is not an admin in log group/channel {config .LOGGER_ID }. Some functions may be limited.')
            except Exception as ex :
                LOGGER (__name__ ).warning (f'Could not verify admin status in log group: {type (ex ).__name__ }')

        LOGGER (__name__ ).info (f'Music Bot Started as {self .name }')

    async def stop (self ):
        await super ().stop ()
