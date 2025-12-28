import logging
from telegram import Update
from telegram.ext import ContextTypes

logger = logging.getLogger(__name__)

async def add_watermark(update: Update, context: ContextTypes.DEFAULT_TYPE, state: int) -> int:
    """Handle watermark addition."""
    await update.message.reply_text(
        "📹 Send the video file\n"
        "Then send the watermark image (PNG recommended)",
        reply_to_message_id=update.message.message_id
    )
    context.user_data["operation"] = "watermark"
    return state

async def add_subtitle(update: Update, context: ContextTypes.DEFAULT_TYPE, state: int) -> int:
    """Handle subtitle addition."""
    await update.message.reply_text(
        "📹 Send the video file\n"
        "Then send the subtitle file (.srt, .ass, .vtt)",
        reply_to_message_id=update.message.message_id
    )
    context.user_data["operation"] = "subtitle"
    return state

async def compress_video(update: Update, context: ContextTypes.DEFAULT_TYPE, state: int) -> int:
    """Handle video compression."""
    await update.message.reply_text(
        "📹 Send the video file to compress\n"
        "Quality: 0-51 (lower = better, default 28)",
        reply_to_message_id=update.message.message_id
    )
    context.user_data["operation"] = "compress"
    return state

async def remove_stream(update: Update, context: ContextTypes.DEFAULT_TYPE, state: int) -> int:
    """Handle stream removal (audio/video)."""
    await update.message.reply_text(
        "📹 Send the video file\n"
        "Then specify: 'audio' or 'video' to remove",
        reply_to_message_id=update.message.message_id
    )
    context.user_data["operation"] = "remove_stream"
    return state

async def sync_subtitle(update: Update, context: ContextTypes.DEFAULT_TYPE, state: int) -> int:
    """Handle subtitle synchronization."""
    await update.message.reply_text(
        "📄 Send the subtitle file to sync\n"
        "Then provide delay in seconds (e.g., 2.5 or -1.5)",
        reply_to_message_id=update.message.message_id
    )
    context.user_data["operation"] = "sync_sub"
    return state

async def rename_file(update: Update, context: ContextTypes.DEFAULT_TYPE, state: int) -> int:
    """Handle file renaming."""
    await update.message.reply_text(
        "📁 Send the file to rename\n"
        "Then send the new filename",
        reply_to_message_id=update.message.message_id
    )
    context.user_data["operation"] = "rename"
    return state
