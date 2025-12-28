"""Start command handler with user information display."""
import logging
from telegram import Update
from telegram.ext import ContextTypes
from keyboards.main_keyboard import get_main_keyboard

logger = logging.getLogger(__name__)


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Handle /start command and show main menu with user information.
    Added default Telegram upload mode on start

    Args:
        update: Telegram update object
        context: Callback context
    """
    user = update.effective_user

    THUMBNAIL_URL = "https://wallpapercave.com/wp/wp13949768.jpg"
    thumbnail_status = "Exist ✅"

    user_info = (
        f"📌 USER SETTINGS\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"🖼 Thumbnail: {thumbnail_status}\n"
        f"🔗 Thumbnail URL:\n{THUMBNAIL_URL}\n\n"
        f"👤 Username: @{user.username or 'Not Set'}\n"
        f"🆔 ID: {user.id}\n"
        f"👁️ First Name: {user.first_name or 'Not Set'}\n"
        f"📛 Last Name: {user.last_name or 'Not Set'}\n"
        f"🤖 Is Bot: {'Yes' if user.is_bot else 'No'}\n"
        f"━━━━━━━━━━━━━━━━━━\n\n"
    )

    welcome_text = (
        f"{user_info}"
        f"🎬 Welcome to Video Merger Bot!\n\n"
        f"I can help you with:\n"
        f"• ➕ Merging videos\n"
        f"• 🔊 Extracting/adding audio\n"
        f"• 🌊 Watermarks & subtitles\n"
        f"• ✅ Video compression & conversion\n"
        f"• And much more!\n\n"
        f"Select a category below to get started:"
    )

    if "upload_mode" not in context.user_data:
        context.user_data["upload_mode"] = {
            "engine": "telegram",
            "format": "video"  # Default format for Telegram
        }

    await update.message.reply_text(
        welcome_text,
        reply_markup=get_main_keyboard(context.user_data.get("upload_mode")),
        disable_web_page_preview=False,
    )

    logger.info(f"User {user.id} (@{user.username}) started bot - Upload mode: {context.user_data.get('upload_mode', {}).get('engine', 'telegram')}")
