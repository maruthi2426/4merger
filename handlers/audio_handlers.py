import logging
from telegram import Update
from telegram.ext import ContextTypes

logger = logging.getLogger(__name__)

async def swap_audio(update: Update, context: ContextTypes.DEFAULT_TYPE, state: int) -> int:
    """Handle audio swap operation."""
    await update.message.reply_text(
        "🎬 Send the video file whose audio you want to replace",
        reply_to_message_id=update.message.message_id
    )
    context.user_data["operation"] = "swap_audio"
    return state

async def combine_video_audio(update: Update, context: ContextTypes.DEFAULT_TYPE, state: int) -> int:
    """Handle video + audio combination."""
    await update.message.reply_text(
        "📹 Send the video file\n"
        "Then send the audio file to combine",
        reply_to_message_id=update.message.message_id
    )
    context.user_data["operation"] = "combine"
    return state
