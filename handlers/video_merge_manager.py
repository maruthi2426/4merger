"""Advanced video merge queue management system with real-time validation and settings."""
import logging
import os
from datetime import datetime
from typing import Optional, Dict, List, Any
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
from utils.file_manager import FileManager
from utils.ffmpeg_processor import FFmpegProcessor
import json
import subprocess

logger = logging.getLogger(__name__)
file_manager = FileManager()
processor = FFmpegProcessor()

# Queue database - stores merge state per user with message ID for editing
MERGE_QUEUE_DB: Dict[int, Dict[str, Any]] = {}


class VideoMetadata:
    """Store video metadata for queue management."""
    
    def __init__(self, msg_id: int, file_name: str, file_path: str):
        self.msg_id = msg_id
        self.file_name = file_name
        self.file_path = file_path
        self.size = file_manager.get_file_size(file_path)
        self.duration = processor.get_video_duration(file_path)
        
        self.resolution = self._get_resolution()
        self.fps = self._get_fps()
        self.codec = self._get_codec()
        self.has_audio = self._check_audio()
        self.added_time = datetime.now()
        
    def _get_resolution(self) -> tuple:
        """Extract resolution using ffprobe."""
        try:
            cmd = [
                "ffprobe", "-v", "error", "-select_streams", "v:0",
                "-show_entries", "stream=width,height",
                "-of", "json", self.file_path
            ]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
            data = json.loads(result.stdout)
            if data.get("streams"):
                stream = data["streams"][0]
                width = stream.get("width", 1920)
                height = stream.get("height", 1080)
                return (width, height)
            return (1920, 1080)
        except Exception as e:
            logger.warning(f"Could not get resolution: {e}")
            return (1920, 1080)
    
    def _get_fps(self) -> float:
        """Extract FPS using ffprobe."""
        try:
            cmd = [
                "ffprobe", "-v", "error", "-select_streams", "v:0",
                "-show_entries", "stream=r_frame_rate",
                "-of", "json", self.file_path
            ]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
            data = json.loads(result.stdout)
            if data.get("streams"):
                fps_str = data["streams"][0].get("r_frame_rate", "30/1")
                if "/" in fps_str:
                    num, den = fps_str.split("/")
                    return float(num) / float(den)
                return float(fps_str)
            return 30.0
        except Exception as e:
            logger.warning(f"Could not get FPS: {e}")
            return 30.0
    
    def _get_codec(self) -> str:
        """Extract video codec using ffprobe."""
        try:
            cmd = [
                "ffprobe", "-v", "error", "-select_streams", "v:0",
                "-show_entries", "stream=codec_name",
                "-of", "json", self.file_path
            ]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
            data = json.loads(result.stdout)
            if data.get("streams"):
                return data["streams"][0].get("codec_name", "h264")
            return "h264"
        except Exception as e:
            logger.warning(f"Could not get codec: {e}")
            return "h264"
    
    def _check_audio(self) -> bool:
        """Check if video has audio stream."""
        try:
            cmd = [
                "ffprobe", "-v", "error", "-select_streams", "a:0",
                "-show_entries", "stream=codec_type",
                "-of", "json", self.file_path
            ]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
            data = json.loads(result.stdout)
            return len(data.get("streams", [])) > 0
        except Exception as e:
            logger.warning(f"Could not check audio: {e}")
            return True
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for storage."""
        return {
            "msg_id": self.msg_id,
            "file_name": self.file_name,
            "file_path": self.file_path,
            "size": self.size,
            "duration": self.duration,
            "resolution": self.resolution,
            "fps": self.fps,
            "codec": self.codec,
            "has_audio": self.has_audio,
            "added_time": self.added_time.isoformat(),
        }
    
    @staticmethod
    def from_dict(data: Dict) -> 'VideoMetadata':
        """Restore from dictionary."""
        meta = VideoMetadata(data["msg_id"], data["file_name"], data["file_path"])
        meta.resolution = tuple(data.get("resolution", (0, 0)))
        meta.fps = data.get("fps", 0.0)
        meta.codec = data.get("codec", "unknown")
        meta.has_audio = data.get("has_audio", False)
        meta.size = data.get("size", 0)
        meta.duration = data.get("duration", 0)
        meta.added_time = datetime.fromisoformat(data.get("added_time", datetime.now().isoformat()))
        return meta


class MergeQueue:
    """Manage video merge queue for a user with single message tracking."""
    
    def __init__(self, user_id: int):
        self.user_id = user_id
        self.videos: List[VideoMetadata] = []
        self.queue_message_id: Optional[int] = None  # Track single message ID for editing
        self.settings = {
            "merge_mode": "smart",  # smart, fast, safe
            "resolution": "auto",    # auto, 720, 1080, 4k
            "fps": "auto",
            "audio": "keep_all",     # keep_all, keep_first, normalize
        }
    
    def add_video(self, metadata: VideoMetadata) -> bool:
        """Add video to queue with validation."""
        # Validation
        if len(self.videos) >= 20:
            logger.warning(f"User {self.user_id} exceeded max videos")
            return False
        
        if metadata.duration == 0:
            logger.warning(f"Video {metadata.file_name} has invalid duration")
            return False
        
        self.videos.append(metadata)
        return True
    
    def remove_video(self, index: int) -> bool:
        """Remove video at index."""
        if 0 <= index < len(self.videos):
            video = self.videos.pop(index)
            file_manager.delete_file(video.file_path)
            return True
        return False
    
    def move_video(self, from_idx: int, to_idx: int) -> bool:
        """Move video in queue."""
        if 0 <= from_idx < len(self.videos) and 0 <= to_idx < len(self.videos):
            self.videos[from_idx], self.videos[to_idx] = self.videos[to_idx], self.videos[from_idx]
            return True
        return False
    
    def clear_all(self) -> None:
        """Clear entire queue and cleanup files."""
        for video in self.videos:
            file_manager.delete_file(video.file_path)
        self.videos = []
        self.queue_message_id = None
    
    def get_total_size(self) -> float:
        """Get total size in GB."""
        total_bytes = sum(v.size for v in self.videos)
        return total_bytes / (1024 * 1024 * 1024)
    
    def get_total_duration(self) -> float:
        """Get total duration in seconds."""
        return sum(v.duration for v in self.videos)
    
    def get_validation_warnings(self) -> List[str]:
        """Check for codec/resolution mismatches."""
        warnings = []
        
        if len(self.videos) < 2:
            return warnings
        
        # Check codec mismatch
        codecs = set(v.codec for v in self.videos)
        if len(codecs) > 1:
            warnings.append(f"• Different codecs detected ({', '.join(codecs)})")
        
        # Check resolution mismatch
        resolutions = set(v.resolution for v in self.videos)
        if len(resolutions) > 1:
            warnings.append(f"• Different resolutions detected")
        
        # Check FPS mismatch
        fps_values = set(v.fps for v in self.videos)
        if len(fps_values) > 1:
            warnings.append(f"• Different FPS detected ({', '.join(str(f) for f in fps_values)})")
        
        # Check audio mismatch
        has_audio = set(v.has_audio for v in self.videos)
        if len(has_audio) > 1:
            warnings.append("• Some videos missing audio")
        
        return warnings
    
    def format_queue_message(self) -> str:
        """Format consolidated queue message."""
        if not self.videos:
            return "📂 Queue (0 videos)\n\nNo videos added yet."
        
        lines = [f"📂 Queue ({len(self.videos)} videos)\n"]
        
        for idx, video in enumerate(self.videos, 1):
            duration_str = self._format_duration(video.duration)
            size_mb = video.size / (1024 * 1024)
            res_str = f"{video.resolution[0]}x{video.resolution[1]}"
            
            lines.append(f"{idx}️⃣ {video.file_name}")
            lines.append(f"   ⏱️ {duration_str} | 🎬 {res_str} | 📁 {size_mb:.1f}MB")
        
        total_duration = self.get_total_duration()
        total_size = self.get_total_size()
        
        lines.append(f"\n📦 Total Size: {total_size:.2f}GB")
        lines.append(f"⏱️ Total Duration: {self._format_duration(total_duration)}")
        
        return "\n".join(lines)
    
    @staticmethod
    def _format_duration(seconds: float) -> str:
        """Format duration as HH:MM:SS."""
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        secs = int(seconds % 60)
        return f"{hours:02d}:{minutes:02d}:{secs:02d}"


def get_or_create_queue(user_id: int) -> MergeQueue:
    """Get or create merge queue for user."""
    if user_id not in MERGE_QUEUE_DB:
        MERGE_QUEUE_DB[user_id] = MergeQueue(user_id)
    return MERGE_QUEUE_DB[user_id]


async def show_merge_menu(update, context, edit=True):
    """Show video merger main menu."""
    queue = get_or_create_queue(update.effective_user.id)
    
    text = "🎬 VIDEO + VIDEO MERGER\n━━━━━━━━━━━━━━━━━━\n"
    text += "Add videos in order you want to merge.\n"
    text += "Supported: mp4, mkv, mov, webm\n\n"
    
    if queue.videos:
        text += queue.format_queue_message() + "\n\n"
    
    keyboard = [
        [InlineKeyboardButton("➕ Add Video", callback_data="merge_add_video")],
    ]
    
    if len(queue.videos) >= 2:
        keyboard.append([InlineKeyboardButton("▶️ Start Merge", callback_data="merge_confirm")])
    
    keyboard.extend([
        [
            InlineKeyboardButton("🧹 Clear Queue", callback_data="merge_clear"),
            InlineKeyboardButton("🔙 Back", callback_data="back_main"),
        ],
    ])
    
    if edit and update.callback_query:
        await update.callback_query.edit_message_text(
            text=text,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    elif update.message:
        await update.message.reply_text(
            text=text,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )


async def show_merge_queue(update, context):
    """Display detailed queue with move/remove options."""
    query = update.callback_query
    user_id = update.effective_user.id
    queue = get_or_create_queue(user_id)
    
    if not queue.videos:
        await query.answer("Queue is empty!", show_alert=False)
        return
    
    text = queue.format_queue_message()
    
    # Show warnings
    warnings = queue.get_validation_warnings()
    if warnings:
        text += "\n\n⚠️ Detected Issues:\n"
        text += "\n".join(warnings)
    
    # Create move/remove keyboard
    keyboard = []
    for idx in range(len(queue.videos)):
        keyboard.append([
            InlineKeyboardButton(f"⬆️ {idx+1}", callback_data=f"merge_move_up_{idx}"),
            InlineKeyboardButton(f"❌ {idx+1}", callback_data=f"merge_remove_{idx}"),
            InlineKeyboardButton(f"⬇️ {idx+1}", callback_data=f"merge_move_down_{idx}"),
        ])
    
    keyboard.append([InlineKeyboardButton("⬅️ Back to Menu", callback_data="merge_menu")])
    
    await query.edit_message_text(
        text=text,
        reply_markup=InlineKeyboardMarkup(keyboard)
    )


async def show_merge_settings(update, context):
    """Show merge settings menu."""
    query = update.callback_query
    user_id = update.effective_user.id
    queue = get_or_create_queue(user_id)
    settings = queue.settings
    
    text = "⚙️ MERGE SETTINGS\n━━━━━━━━━━━━━━━━━━\n\n"
    text += f"🔀 Merge Mode: {settings['merge_mode'].upper()}\n"
    text += "  Fast: No re-encode (fastest)\n"
    text += "  Smart: Auto decide (recommended)\n"
    text += "  Safe: Always re-encode (safest)\n\n"
    text += f"🎞 Resolution: {settings['resolution'].upper()}\n"
    text += "  Auto: Keep first video\n"
    text += "  720, 1080, 4k: Scale to size\n\n"
    text += f"🎥 FPS: {settings['fps'].upper()}\n\n"
    text += f"🔊 Audio: {settings['audio'].replace('_', ' ').upper()}\n"
    
    keyboard = [
        [
            InlineKeyboardButton("🔀 Merge Mode", callback_data="merge_set_mode"),
            InlineKeyboardButton("🎞 Resolution", callback_data="merge_set_resolution"),
        ],
        [
            InlineKeyboardButton("🎥 FPS", callback_data="merge_set_fps"),
            InlineKeyboardButton("🔊 Audio", callback_data="merge_set_audio"),
        ],
        [InlineKeyboardButton("⬅️ Back", callback_data="merge_menu")],
    ]
    
    await query.edit_message_text(
        text=text,
        reply_markup=InlineKeyboardMarkup(keyboard)
    )


async def show_pre_merge_validation(update, context):
    """Show validation warnings before merge."""
    query = update.callback_query
    user_id = update.effective_user.id
    queue = get_or_create_queue(user_id)
    
    text = "⚠️ PRE-MERGE VALIDATION\n━━━━━━━━━━━━━━━━━━\n\n"
    text += f"📦 Videos: {len(queue.videos)}\n"
    text += f"⏱ Total Duration: {MergeQueue._format_duration(queue.get_total_duration())}\n"
    text += f"📁 Total Size: {queue.get_total_size():.2f}GB\n\n"
    
    warnings = queue.get_validation_warnings()
    if warnings:
        text += "⚠️ Differences Detected:\n"
        text += "\n".join(warnings)
        text += "\n\nMerge will require re-encoding.\n"
        text += "Estimated time: 5-30 minutes\n"
    else:
        text += "✅ All videos compatible!\n"
        text += "Fast merge possible.\n"
    
    # Size warning for large files
    if queue.get_total_size() > 2:
        text += f"\n⚠️ Large file size: {queue.get_total_size():.2f}GB\n"
        text += "Upload may take longer\n"
    
    keyboard = [
        [InlineKeyboardButton("✅ Start Merge", callback_data="merge_start_now")],
        [InlineKeyboardButton("❌ Cancel", callback_data="merge_menu")],
    ]
    
    await query.edit_message_text(
        text=text,
        reply_markup=InlineKeyboardMarkup(keyboard)
    )
