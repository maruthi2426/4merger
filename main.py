import os
import logging
from fastapi import FastAPI, Request
from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    filters,
)
from dotenv import load_dotenv
from handlers.start import start_command
from handlers.callback_handler import handle_callback_query
from handlers.video_handlers import (
    merge_videos,
    extract_audio,
    trim_video,
    convert_video,
)
from handlers.audio_handlers import swap_audio, combine_video_audio
from handlers.media_handlers import (
    add_watermark,
    add_subtitle,
    compress_video,
    remove_stream,
    sync_subtitle,
    rename_file,
)
from handlers.file_handler import handle_files
from utils.logger import setup_logging
from utils.file_manager import FileManager

load_dotenv()
setup_logging()
logger = logging.getLogger(__name__)

# Conversation states
(
    MERGE_VIDEOS,
    EXTRACT_AUDIO,
    TRIM_VIDEO,
    CONVERT_VIDEO,
    SWAP_AUDIO,
    COMBINE_MEDIA,
    ADD_WATERMARK,
    ADD_SUBTITLE,
    COMPRESS_VIDEO,
    REMOVE_STREAM,
    SYNC_SUBTITLE,
    RENAME_FILE,
) = range(12)

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
WEBHOOK_URL = os.getenv("WEBHOOK_URL")
PORT = int(os.getenv("PORT", 10000))

if not BOT_TOKEN:
    raise ValueError("TELEGRAM_BOT_TOKEN not found in environment variables")
if not WEBHOOK_URL:
    raise ValueError("WEBHOOK_URL not found in environment variables")

app = FastAPI()
application = None

@app.on_event("startup")
async def on_startup():
    """Initialize bot and set webhook."""
    global application
    application = Application.builder().token(BOT_TOKEN).build()
    
    # Create temp folder for files
    FileManager.create_temp_folder()
    
    # Add command handlers
    application.add_handler(CommandHandler("start", start_command))
    
    application.add_handler(CallbackQueryHandler(handle_callback_query))
    
    # Add legacy command handlers for backward compatibility
    application.add_handler(CommandHandler("merge", lambda u, c: merge_videos(u, c, MERGE_VIDEOS)))
    application.add_handler(CommandHandler("extract", lambda u, c: extract_audio(u, c, EXTRACT_AUDIO)))
    application.add_handler(CommandHandler("trim", lambda u, c: trim_video(u, c, TRIM_VIDEO)))
    application.add_handler(CommandHandler("convert", lambda u, c: convert_video(u, c, CONVERT_VIDEO)))
    application.add_handler(CommandHandler("swap_audio", lambda u, c: swap_audio(u, c, SWAP_AUDIO)))
    application.add_handler(CommandHandler("combine", lambda u, c: combine_video_audio(u, c, COMBINE_MEDIA)))
    application.add_handler(CommandHandler("watermark", lambda u, c: add_watermark(u, c, ADD_WATERMARK)))
    application.add_handler(CommandHandler("subtitle", lambda u, c: add_subtitle(u, c, ADD_SUBTITLE)))
    application.add_handler(CommandHandler("compress", lambda u, c: compress_video(u, c, COMPRESS_VIDEO)))
    application.add_handler(CommandHandler("remove_stream", lambda u, c: remove_stream(u, c, REMOVE_STREAM)))
    application.add_handler(CommandHandler("sync_sub", lambda u, c: sync_subtitle(u, c, SYNC_SUBTITLE)))
    application.add_handler(CommandHandler("rename", lambda u, c: rename_file(u, c, RENAME_FILE)))
    
    # Add file handler for all documents, videos, and audio
    application.add_handler(
        MessageHandler(filters.Document.ALL | filters.VIDEO | filters.AUDIO, handle_files)
    )
    
    application.add_handler(
        MessageHandler(filters.TEXT & ~filters.COMMAND, handle_files)
    )
    
    async def error_handler(update, context):
        """Log errors caused by Updates."""
        logger.error(f"Exception while handling an update: {context.error}", exc_info=context.error)
        
        if update and update.effective_chat:
            try:
                await context.bot.send_message(
                    chat_id=update.effective_chat.id,
                    text=f"❌ An error occurred. Please try again.\nError: {str(context.error)[:100]}"
                )
            except:
                pass
    
    application.add_error_handler(error_handler)
    
    await application.initialize()
    await application.start()
    
    # Set webhook
    await application.bot.set_webhook(url=f"{WEBHOOK_URL}/webhook")
    logger.info(f"✅ Bot started - Webhook set to {WEBHOOK_URL}/webhook")

@app.on_event("shutdown")
async def on_shutdown():
    """Shutdown bot gracefully."""
    global application
    await application.stop()
    await application.shutdown()
    logger.info("Bot stopped")

@app.post("/webhook")
async def telegram_webhook(request: Request):
    """Handle incoming Telegram updates via webhook."""
    try:
        update = Update.de_json(await request.json(), application.bot)
        await application.process_update(update)
    except Exception as e:
        logger.error(f"Error processing webhook update: {e}")
    return {"ok": True}

@app.get("/")
def health_check():
    """Health check endpoint for Render."""
    return {"status": "ok", "service": "Video Merger Bot"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=PORT, log_level="info")
