"""Handle file uploads including rclone config file detection."""
import logging
import os
from telegram import Update
from telegram.ext import ContextTypes
from utils.file_manager import FileManager
from utils.ffmpeg_processor import FFmpegProcessor
from handlers.media_processor import (
    process_extract, process_trim, 
    process_convert, process_compress, process_remove_stream,
    process_swap_audio, process_combine, process_watermark, 
    process_subtitle
)
from handlers.video_merge_processor import process_merge_video

logger = logging.getLogger(__name__)
file_manager = FileManager()
processor = FFmpegProcessor()

async def handle_files(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle all file uploads and process based on operation."""
    try:
        user_id = update.effective_user.id
        
        if context.user_data.get("awaiting_rclone_config"):
            file = update.message.document
            if not file:
                await update.message.reply_text(
                    "❌ Please send a file (document format only)",
                    reply_to_message_id=update.message.message_id
                )
                return
            
            filename = file.file_name or "rclone.conf"
            
            # Only accept .conf files
            if not filename.endswith(".conf"):
                await update.message.reply_text(
                    "❌ Invalid file! Please send the rclone.conf file only",
                    reply_to_message_id=update.message.message_id
                )
                return
            
            # Create userdata directory
            user_dir = f"./userdata/{user_id}"
            os.makedirs(user_dir, exist_ok=True)
            
            # Save the config file
            conf_path = os.path.join(user_dir, "rclone.conf")
            file_obj = await context.bot.get_file(file.file_id)
            await file_obj.download_to_drive(conf_path)
            
            # Validate rclone config
            try:
                with open(conf_path, 'r') as f:
                    content = f.read()
                    if '[' not in content or ']' not in content:
                        raise ValueError("Invalid rclone config format")
                
                # Config is valid, set rclone mode
                context.user_data["upload_mode"] = {
                    "engine": "rclone",
                    "configured": True
                }
                context.user_data.pop("awaiting_rclone_config", None)
                
                logger.info(f"User {user_id} successfully uploaded rclone.conf")
                
                from keyboards.main_keyboard import get_main_keyboard
                
                await update.message.reply_text(
                    "✅ RCLONE CONFIGURED\n━━━━━━━━━━━━━━━━━━\n\n"
                    "Your rclone configuration has been successfully saved!\n"
                    "You can now use Rclone upload mode.\n\n"
                    "Returning to main menu...",
                    reply_to_message_id=update.message.message_id
                )
                
                # Send main menu with rclone checkmark
                await update.message.reply_text(
                    "🎬 Welcome to Video Merger Bot!\n\nSelect a category:",
                    reply_markup=get_main_keyboard(context.user_data.get("upload_mode"))
                )
                return
            except Exception as e:
                logger.error(f"Invalid rclone config from user {user_id}: {e}")
                await update.message.reply_text(
                    f"❌ Invalid rclone config file!\n\n"
                    f"Error: {str(e)}\n\n"
                    f"Please send a valid rclone.conf file",
                    reply_to_message_id=update.message.message_id
                )
                os.remove(conf_path)
                return
        
        operation = context.user_data.get("operation")
        
        if not operation:
            await update.message.reply_text(
                "❌ No operation selected. Use /start to choose an operation",
                reply_to_message_id=update.message.message_id
            )
            return
        
        file = update.message.document or update.message.video or update.message.audio
        if not file:
            return
        
        file_obj = await context.bot.get_file(file.file_id)
        filename = file.file_name or f"file_{file.file_id[:8]}"
        filepath = os.path.join(file_manager.TEMP_FOLDER, filename)
        
        # Create temp folder
        file_manager.create_temp_folder()
        
        if operation in ["merge", "merge_add"]:
            download_msg = await update.message.reply_text(
                "📥 Downloading video...\n"
                "Progress: 0%",
                reply_to_message_id=update.message.message_id
            )
            
            await file_obj.download_to_drive(filepath)
            file_size = file_manager.get_file_size(filepath) / (1024*1024)
            
            try:
                await download_msg.delete()
            except:
                pass
            
            await process_merge_video(update, context, filepath)
        else:
            await file_obj.download_to_drive(filepath)
            file_size = file_manager.get_file_size(filepath) / (1024*1024)
            await update.message.reply_text(
                f"📥 Downloaded: {filename} ({file_size:.2f} MB)",
                reply_to_message_id=update.message.message_id
            )
            
            if "files" not in context.user_data:
                context.user_data["files"] = []
            context.user_data["files"].append(filepath)
            
            if operation == "extract":
                await process_extract(update, context, filepath)
            elif operation == "trim":
                await process_trim(update, context, filepath)
            elif operation == "convert":
                await process_convert(update, context, filepath)
            elif operation == "compress":
                await process_compress(update, context, filepath)
            elif operation == "remove_stream":
                await process_remove_stream(update, context, filepath)
            elif operation == "swap_audio":
                await process_swap_audio(update, context, filepath)
            elif operation == "combine":
                await process_combine(update, context, filepath)
            elif operation == "watermark":
                await process_watermark(update, context, filepath)
            elif operation == "subtitle":
                await process_subtitle(update, context, filepath)
        
    except Exception as e:
        logger.error(f"Error handling file: {e}")
        await update.message.reply_text(
            f"❌ Error: {str(e)}",
            reply_to_message_id=update.message.message_id
        )
