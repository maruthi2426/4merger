import logging
import os
from telegram import Update
from telegram.ext import ContextTypes
from utils.ffmpeg_processor import FFmpegProcessor
from utils.file_manager import FileManager

logger = logging.getLogger(__name__)
processor = FFmpegProcessor()
file_manager = FileManager()

async def merge_videos(update: Update, context: ContextTypes.DEFAULT_TYPE, state: int) -> int:
    """Handle video merge operation."""
    await update.message.reply_text(
        "📹 Send video files to merge (send 2 or more videos)",
        reply_to_message_id=update.message.message_id
    )
    context.user_data["operation"] = "merge"
    return state

async def extract_audio(update: Update, context: ContextTypes.DEFAULT_TYPE, state: int) -> int:
    """Handle audio extraction."""
    await update.message.reply_text(
        "📹 Send video file to extract audio from",
        reply_to_message_id=update.message.message_id
    )
    context.user_data["operation"] = "extract"
    return state

async def trim_video(update: Update, context: ContextTypes.DEFAULT_TYPE, state: int) -> int:
    """Handle video trimming."""
    await update.message.reply_text(
        "📹 Send video file to trim",
        reply_to_message_id=update.message.message_id
    )
    context.user_data["operation"] = "trim"
    return state

async def convert_video(update: Update, context: ContextTypes.DEFAULT_TYPE, state: int) -> int:
    """Handle video format conversion."""
    formats = "mp4, mkv, avi, mov, webm, flv, wmv, m3u8"
    await update.message.reply_text(
        f"📹 Send video file to convert\n"
        f"Supported formats: {formats}",
        reply_to_message_id=update.message.message_id
    )
    context.user_data["operation"] = "convert"
    return state
