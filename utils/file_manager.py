import os
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

class FileManager:
    """Manage file operations."""

    ALLOWED_VIDEO_FORMATS = {
        "mp4", "mkv", "avi", "mov", "webm", "flv", "wmv", "m3u8",
        "m4v", "mpg", "mpeg", "3gp", "ogv", "ts", "vob"
    }
    
    ALLOWED_AUDIO_FORMATS = {
        "mp3", "m4a", "aac", "wav", "flac", "opus", "ogg", "wma"
    }

    TEMP_FOLDER = "temp_files"

    @staticmethod
    def create_temp_folder():
        """Create temporary files folder."""
        if not os.path.exists(FileManager.TEMP_FOLDER):
            os.makedirs(FileManager.TEMP_FOLDER)
            logger.info(f"Created temp folder: {FileManager.TEMP_FOLDER}")

    @staticmethod
    def is_valid_video(filename: str) -> bool:
        """Check if file is valid video format."""
        ext = Path(filename).suffix.lower().lstrip(".")
        return ext in FileManager.ALLOWED_VIDEO_FORMATS

    @staticmethod
    def is_valid_audio(filename: str) -> bool:
        """Check if file is valid audio format."""
        ext = Path(filename).suffix.lower().lstrip(".")
        return ext in FileManager.ALLOWED_AUDIO_FORMATS

    @staticmethod
    def get_file_extension(filename: str) -> str:
        """Get file extension."""
        return Path(filename).suffix.lower()

    @staticmethod
    def rename_file(old_path: str, new_name: str) -> str:
        """Rename a file and return new path."""
        try:
            directory = os.path.dirname(old_path)
            new_path = os.path.join(directory, new_name)
            os.rename(old_path, new_path)
            logger.info(f"File renamed: {old_path} -> {new_path}")
            return new_path
        except Exception as e:
            logger.error(f"Error renaming file: {e}")
            return None

    @staticmethod
    def delete_file(file_path: str) -> bool:
        """Delete a file."""
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
                logger.info(f"File deleted: {file_path}")
                return True
            return False
        except Exception as e:
            logger.error(f"Error deleting file: {e}")
            return False

    @staticmethod
    def get_file_size(file_path: str) -> int:
        """Get file size in bytes."""
        try:
            return os.path.getsize(file_path)
        except Exception as e:
            logger.error(f"Error getting file size: {e}")
            return 0

    @staticmethod
    def cleanup_temp_files():
        """Delete all temporary files."""
        try:
            if os.path.exists(FileManager.TEMP_FOLDER):
                for file in os.listdir(FileManager.TEMP_FOLDER):
                    file_path = os.path.join(FileManager.TEMP_FOLDER, file)
                    if os.path.isfile(file_path):
                        os.remove(file_path)
                logger.info("Temp files cleaned up")
                return True
        except Exception as e:
            logger.error(f"Error cleaning temp files: {e}")
            return False
