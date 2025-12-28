import logging
import time
from telegram import Update
from telegram.ext import ContextTypes

logger = logging.getLogger(__name__)

class ProgressTracker:
    """Track and display progress in real-time."""
    
    def __init__(self, update: Update, context: ContextTypes.DEFAULT_TYPE, status_msg, operation: str):
        self.update = update
        self.context = context
        self.status_msg = status_msg
        self.operation = operation
        self.start_time = time.time()
        self.last_update = time.time()
        
    async def update_progress(self, current: int, total: int, filename: str = "", speed: str = ""):
        """Update progress bar and stats."""
        elapsed = time.time() - self.start_time
        
        # Only update every 2 seconds to avoid rate limiting
        if time.time() - self.last_update < 2:
            return
        
        self.last_update = time.time()
        
        try:
            # Calculate percentage
            if total > 0:
                percentage = (current / total) * 100
            else:
                percentage = 0
            
            # Progress bar
            bar_length = 20
            filled = int(bar_length * percentage / 100)
            bar = "█" * filled + "░" * (bar_length - filled)
            
            # Calculate ETA
            if speed and current > 0:
                try:
                    speed_num = float(speed.split()[0])
                    remaining_mb = (total - current) / (1024 * 1024)
                    eta_seconds = remaining_mb / speed_num if speed_num > 0 else 0
                    eta_str = self._format_time(eta_seconds)
                except:
                    eta_str = "calculating..."
            else:
                eta_str = "calculating..."
            
            text = (
                f"⏳ {self.operation}...\n\n"
                f"📊 Progress: {percentage:.1f}%\n"
                f"[{bar}]\n\n"
                f"📁 File: {filename}\n"
                f"⚡ Speed: {speed}\n"
                f"⏱️ Elapsed: {self._format_time(elapsed)}\n"
                f"⏳ ETA: {eta_str}"
            )
            
            await self.status_msg.edit_text(text)
        except Exception as e:
            logger.debug(f"Error updating progress: {e}")
    
    async def final_status(self, message: str):
        """Update final status message."""
        try:
            elapsed = time.time() - self.start_time
            text = f"{message}\n⏱️ Total time: {self._format_time(elapsed)}"
            await self.status_msg.edit_text(text)
        except Exception as e:
            logger.debug(f"Error updating final status: {e}")
    
    @staticmethod
    def _format_time(seconds: float) -> str:
        """Format seconds to human readable format."""
        if seconds < 60:
            return f"{int(seconds)}s"
        elif seconds < 3600:
            return f"{int(seconds // 60)}m {int(seconds % 60)}s"
        else:
            hours = int(seconds // 3600)
            minutes = int((seconds % 3600) // 60)
            return f"{hours}h {minutes}m"
