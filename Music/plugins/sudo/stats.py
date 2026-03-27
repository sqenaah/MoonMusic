

from pyrogram import Client ,filters
from pyrogram .types import Message
from Music import LOGGER ,app

logger =LOGGER (__name__ )

@Client .on_message (filters .command ("stats")&filters .group )
async def show_stats (client :Client ,message :Message ):

    try :

        from Music .platforms .Youtube import YouTubeAPI

        stats_text ="""
**Extraction Method Statistics**

✓ This build uses YouTube for search/metadata and third-party services for media extraction.

• **external_service:** Successful extraction via configured third-party services
• **external_service_video:** Same policy for video-oriented requests

Direct YouTube downloading, Invidious fallbacks, and pytube fallbacks are disabled.

To see live stats, check logs for "MethodUsed:" entries.

**Log Grep for Current Session:**
```
grep "MethodUsed:" <log_file>
```

**Recent Extraction Failure Pattern:**
- External services exhausted for a requested media item
- Search/metadata succeeded, but third-party extraction failed
    """

        await message .reply_text (stats_text ,quote =True )

    except Exception as e :
        logger .error (f"Error in stats command: {e }")
        await message .reply_text (f"❌ Stats command error: {e }",quote =True )
