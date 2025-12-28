"""Callback handlers for video merge operations."""
import logging
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
from handlers.video_merge_manager import get_or_create_queue, show_merge_menu
from keyboards.main_keyboard import get_telegram_format_keyboard

logger = logging.getLogger(__name__)


async def handle_merge_callbacks(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Main handler for video merge callbacks."""
    query = update.callback_query
    callback_data = query.data
    user_id = update.effective_user.id
    
    try:
        if callback_data == "video_merge":
            await show_merge_menu(update, context, edit=True)
        
        elif callback_data == "merge_menu":
            await show_merge_menu(update, context, edit=True)
        
        elif callback_data == "merge_add_video":
            context.user_data["operation"] = "merge_add"
            context.user_data["merge_mode"] = True  # Flag to indicate we're in merge mode
            
            await query.edit_message_text(
                text="📹 Send video file to add to queue\n\n"
                     "Supported formats: mp4, mkv, mov, webm\n"
                     "Max file size: 4GB\n\n"
                     "Type /start to cancel",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("⬅️ Back", callback_data="merge_menu")
                ]])
            )
        
        elif callback_data == "merge_clear":
            queue = get_or_create_queue(user_id)
            queue.clear_all()
            await query.answer("Queue cleared", show_alert=False)
            await show_merge_menu(update, context, edit=True)
        
        elif callback_data == "merge_confirm":
            queue = get_or_create_queue(user_id)
            if len(queue.videos) < 2:
                await query.answer("Need at least 2 videos!", show_alert=True)
                return
            
            upload_mode = context.user_data.get("upload_mode")
            if not upload_mode:
                await query.answer("❌ Please select Upload Mode first!", show_alert=True)
                logger.warning(f"User {user_id} attempted merge without selecting upload mode")
                return
            
            if upload_mode.get("engine") == "telegram":
                await query.edit_message_text(
                    text="📱 TELEGRAM UPLOAD FORMAT\n━━━━━━━━━━━━━━━━━━\n"
                         "Choose how to upload merged video:\n\n"
                         "🎥 Video: Sends as playable video file\n"
                         "📁 Document: Sends as generic file\n\n"
                         "Select your preferred format:",
                    reply_markup=get_telegram_format_keyboard()
                )
                context.user_data["awaiting_merge_format"] = True
                logger.info(f"User {user_id} shown format selection for merge")
                return
            elif upload_mode.get("engine") == "rclone":
                await _show_rename_options(query, user_id)
                return
        
        elif callback_data == "merge_use_default":
            """User chose to use default filename (merged_video.mp4)"""
            context.user_data["merged_filename"] = "merged_video.mp4"
            context.user_data["awaiting_merge_filename"] = False
            
            # Start merge with default filename
            from handlers.video_merge_processor import execute_smart_merge
            await execute_smart_merge(update, context)
        
        elif callback_data == "merge_ask_rename":
            """User chose to rename - ask for custom filename"""
            context.user_data["awaiting_merge_filename"] = True
            
            await query.edit_message_text(
                text="📝 CUSTOM FILENAME\n━━━━━━━━━━━━━━━━━━\n\n"
                     "Send the desired filename for merged video.\n\n"
                     "Examples:\n"
                     "• my_video.mp4\n"
                     "• my_video (extension auto-added)\n"
                     "• birthday_celebration.mp4\n\n"
                     "Don't worry about the extension, we'll handle it!",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("❌ Cancel", callback_data="merge_confirm_back")
                ]])
            )
            logger.info(f"User {user_id} started rename process")
        
        elif callback_data == "merge_confirm_back":
            """User cancelled rename, go back to rename options"""
            context.user_data["awaiting_merge_filename"] = False
            await _show_rename_options(query, user_id)
        
        elif callback_data == "merge_cancel":
            await show_merge_menu(update, context, edit=True)
        
        elif callback_data == "telegram_format_video":
            """User selected Video format for Telegram merge"""
            context.user_data["upload_mode"]["format"] = "video"
            context.user_data["awaiting_merge_format"] = False
            
            # Show rename options after format selection
            await _show_rename_options(query, user_id)
            logger.info(f"User {user_id} selected Video format for merge")
        
        elif callback_data == "telegram_format_document":
            """User selected Document format for Telegram merge"""
            context.user_data["upload_mode"]["format"] = "document"
            context.user_data["awaiting_merge_format"] = False
            
            # Show rename options after format selection
            await _show_rename_options(query, user_id)
            logger.info(f"User {user_id} selected Document format for merge")
        
    except Exception as e:
        logger.error(f"Error in merge callback: {e}")
        await query.answer(f"Error: {str(e)}", show_alert=True)


async def _show_rename_options(query, user_id):
    """Show Default/Rename buttons with Back and Cancel for file naming."""
    await query.edit_message_text(
        text="📝 FILENAME SELECTION\n━━━━━━━━━━━━━━━━━━\n\n"
             "How would you like to name the merged file?\n\n"
             "📌 Default: merged_video.mp4\n"
             "✏️ Rename: Choose a custom name",
        reply_markup=InlineKeyboardMarkup([
            [
                InlineKeyboardButton("📌 Default", callback_data="merge_use_default"),
                InlineKeyboardButton("✏️ Rename", callback_data="merge_ask_rename")
            ],
            [
                InlineKeyboardButton("⬅️ Back", callback_data="merge_menu"),
                InlineKeyboardButton("❌ Cancel", callback_data="merge_menu")
            ]
        ])
    )
    logger.info(f"User {user_id} shown rename options")
