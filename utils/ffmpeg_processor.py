import subprocess
import logging
import os
import re
import asyncio
from pathlib import Path

logger = logging.getLogger(__name__)

class FFmpegProcessor:
    """Handle all FFmpeg operations."""

    @staticmethod
    def check_ffmpeg_installed():
        """Check if FFmpeg is installed."""
        try:
            subprocess.run(["ffmpeg", "-version"], capture_output=True, check=True)
            return True
        except (FileNotFoundError, subprocess.CalledProcessError):
            return False

    @staticmethod
    def get_video_duration(video_path: str) -> float:
        """Get video duration in seconds."""
        try:
            cmd = ["ffprobe", "-v", "error", "-show_entries", "format=duration", "-of", "default=noprint_wrappers=1:nokey=1:nokey=1", video_path]
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            return float(result.stdout.strip())
        except Exception as e:
            logger.error(f"Error getting duration: {e}")
            return 0

    @staticmethod
    def merge_videos(video_paths: list, output_path: str, progress=None) -> bool:
        """Merge multiple videos using FFmpeg concat demuxer with production-ready flags."""
        try:
            concat_file = "concat.txt"
            with open(concat_file, "w", encoding="utf-8") as f:
                for video in video_paths:
                    # Convert to absolute POSIX path (forward slashes)
                    abs_path = os.path.abspath(video).replace("\\", "/")
                    f.write(f"file '{abs_path}'\n")

            # -fflags +genpts: Fixes broken timestamps on multi-file concat
            # -map 0:v:0 -map 0:a?: Properly maps video and optional audio streams
            # -movflags +faststart: Optimizes for streaming/Telegram
            cmd = [
                "ffmpeg",
                "-y",
                "-hide_banner",
                "-loglevel", "error",
                "-f", "concat",
                "-safe", "0",
                "-fflags", "+genpts",
                "-i", concat_file,
                "-map", "0:v:0",
                "-map", "0:a?",
                "-c", "copy",
                "-movflags", "+faststart",
                output_path
            ]
            
            process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            
            # Track progress
            total_duration = sum(FFmpegProcessor.get_video_duration(v) for v in video_paths)
            
            if progress and total_duration > 0:
                for line in process.stderr:
                    match = re.search(r'time=(\d+):(\d+):(\d+\.\d+)', line)
                    if match:
                        hours, minutes, seconds = match.groups()
                        current_time = int(hours) * 3600 + int(minutes) * 60 + float(seconds)
                        current_bytes = os.path.getsize(output_path) if os.path.exists(output_path) else 0
                        
                        # Use synchronous approach instead of async
                        try:
                            if asyncio.iscoroutinefunction(progress.update_progress):
                                # Schedule coroutine properly
                                loop = asyncio.get_event_loop()
                                loop.create_task(progress.update_progress(
                                    current=int(current_time * 1000),
                                    total=int(total_duration * 1000),
                                    filename=os.path.basename(output_path),
                                    speed=f"{current_bytes / (1024*1024) / max(1, current_time):.2f} MB/s"
                                ))
                        except RuntimeError:
                            # Event loop not available, skip progress update
                            pass
            else:
                process.wait()
            
            process.wait()
            if os.path.exists(concat_file):
                os.remove(concat_file)
            logger.info(f"Videos merged successfully: {output_path}")
            return True
        except Exception as e:
            logger.error(f"Error merging videos: {e}")
            return False

    @staticmethod
    def extract_audio(video_path: str, output_path: str, progress=None) -> bool:
        """Extract audio from video without re-encoding."""
        try:
            cmd = [
                "ffmpeg",
                "-i", video_path,
                "-vn",
                "-acodec", "copy",
                output_path
            ]
            
            process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            total_duration = FFmpegProcessor.get_video_duration(video_path)
            
            if progress and total_duration > 0:
                for line in process.stderr:
                    match = re.search(r'time=(\d+):(\d+):(\d+\.\d+)', line)
                    if match:
                        hours, minutes, seconds = match.groups()
                        current_time = int(hours) * 3600 + int(minutes) * 60 + float(seconds)
                        current_bytes = os.path.getsize(output_path) if os.path.exists(output_path) else 0
                        
                        try:
                            if asyncio.iscoroutinefunction(progress.update_progress):
                                loop = asyncio.get_event_loop()
                                loop.create_task(progress.update_progress(
                                    current=int(current_time * 1000),
                                    total=int(total_duration * 1000),
                                    filename=os.path.basename(output_path),
                                    speed=f"{current_bytes / (1024*1024) / max(1, current_time):.2f} MB/s"
                                ))
                        except RuntimeError:
                            pass
            
            process.wait()
            logger.info(f"Audio extracted: {output_path}")
            return True
        except Exception as e:
            logger.error(f"Error extracting audio: {e}")
            return False

    @staticmethod
    def trim_video(video_path: str, start_time: str, end_time: str, output_path: str, progress=None) -> bool:
        """Trim video to specified duration."""
        try:
            cmd = [
                "ffmpeg",
                "-i", video_path,
                "-ss", start_time,
                "-to", end_time,
                "-c", "copy",
                output_path
            ]
            
            process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            total_duration = FFmpegProcessor.get_video_duration(video_path)
            
            if progress and total_duration > 0:
                for line in process.stderr:
                    match = re.search(r'time=(\d+):(\d+):(\d+\.\d+)', line)
                    if match:
                        hours, minutes, seconds = match.groups()
                        current_time = int(hours) * 3600 + int(minutes) * 60 + float(seconds)
                        current_bytes = os.path.getsize(output_path) if os.path.exists(output_path) else 0
                        
                        try:
                            if asyncio.iscoroutinefunction(progress.update_progress):
                                loop = asyncio.get_event_loop()
                                loop.create_task(progress.update_progress(
                                    current=int(current_time * 1000),
                                    total=int(total_duration * 1000),
                                    filename=os.path.basename(output_path),
                                    speed=f"{current_bytes / (1024*1024) / max(1, current_time):.2f} MB/s"
                                ))
                        except RuntimeError:
                            pass
            
            process.wait()
            logger.info(f"Video trimmed: {output_path}")
            return True
        except Exception as e:
            logger.error(f"Error trimming video: {e}")
            return False

    @staticmethod
    def convert_video(video_path: str, output_format: str, output_path: str = None, progress=None) -> bool:
        """Convert video to different format."""
        try:
            if output_path is None:
                base = Path(video_path).stem
                output_path = f"{base}_converted.{output_format}"

            cmd = [
                "ffmpeg",
                "-i", video_path,
                "-c:v", "libx264",
                "-c:a", "aac",
                output_path
            ]
            
            process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            total_duration = FFmpegProcessor.get_video_duration(video_path)
            
            if progress and total_duration > 0:
                for line in process.stderr:
                    match = re.search(r'time=(\d+):(\d+):(\d+\.\d+)', line)
                    if match:
                        hours, minutes, seconds = match.groups()
                        current_time = int(hours) * 3600 + int(minutes) * 60 + float(seconds)
                        current_bytes = os.path.getsize(output_path) if os.path.exists(output_path) else 0
                        
                        try:
                            if asyncio.iscoroutinefunction(progress.update_progress):
                                loop = asyncio.get_event_loop()
                                loop.create_task(progress.update_progress(
                                    current=int(current_time * 1000),
                                    total=int(total_duration * 1000),
                                    filename=os.path.basename(output_path),
                                    speed=f"{current_bytes / (1024*1024) / max(1, current_time):.2f} MB/s"
                                ))
                        except RuntimeError:
                            pass
            
            process.wait()
            logger.info(f"Video converted: {output_path}")
            return True
        except Exception as e:
            logger.error(f"Error converting video: {e}")
            return False

    @staticmethod
    def compress_video(video_path: str, crf: int, output_path: str, progress=None) -> bool:
        """Compress video with specified quality (CRF 0-51, default 28)."""
        try:
            crf = max(0, min(51, crf))
            cmd = [
                "ffmpeg",
                "-i", video_path,
                "-vcodec", "libx264",
                "-crf", str(crf),
                "-c:a", "aac",
                output_path
            ]
            
            process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            total_duration = FFmpegProcessor.get_video_duration(video_path)
            
            if progress and total_duration > 0:
                for line in process.stderr:
                    match = re.search(r'time=(\d+):(\d+):(\d+\.\d+)', line)
                    if match:
                        hours, minutes, seconds = match.groups()
                        current_time = int(hours) * 3600 + int(minutes) * 60 + float(seconds)
                        current_bytes = os.path.getsize(output_path) if os.path.exists(output_path) else 0
                        
                        try:
                            if asyncio.iscoroutinefunction(progress.update_progress):
                                loop = asyncio.get_event_loop()
                                loop.create_task(progress.update_progress(
                                    current=int(current_time * 1000),
                                    total=int(total_duration * 1000),
                                    filename=os.path.basename(output_path),
                                    speed=f"{current_bytes / (1024*1024) / max(1, current_time):.2f} MB/s"
                                ))
                        except RuntimeError:
                            pass
            
            process.wait()
            logger.info(f"Video compressed: {output_path}")
            return True
        except Exception as e:
            logger.error(f"Error compressing video: {e}")
            return False

    @staticmethod
    def combine_video_audio(video_path: str, audio_path: str, output_path: str, progress=None) -> bool:
        """Combine video and audio files."""
        try:
            cmd = [
                "ffmpeg",
                "-i", video_path,
                "-i", audio_path,
                "-c:v", "copy",
                "-c:a", "aac",
                "-map", "0:v:0",
                "-map", "1:a:0",
                output_path
            ]
            
            process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            total_duration = FFmpegProcessor.get_video_duration(video_path)
            
            if progress and total_duration > 0:
                for line in process.stderr:
                    match = re.search(r'time=(\d+):(\d+):(\d+\.\d+)', line)
                    if match:
                        hours, minutes, seconds = match.groups()
                        current_time = int(hours) * 3600 + int(minutes) * 60 + float(seconds)
                        current_bytes = os.path.getsize(output_path) if os.path.exists(output_path) else 0
                        
                        try:
                            if asyncio.iscoroutinefunction(progress.update_progress):
                                loop = asyncio.get_event_loop()
                                loop.create_task(progress.update_progress(
                                    current=int(current_time * 1000),
                                    total=int(total_duration * 1000),
                                    filename=os.path.basename(output_path),
                                    speed=f"{current_bytes / (1024*1024) / max(1, current_time):.2f} MB/s"
                                ))
                        except RuntimeError:
                            pass
            
            process.wait()
            logger.info(f"Video + Audio combined: {output_path}")
            return True
        except Exception as e:
            logger.error(f"Error combining video and audio: {e}")
            return False

    @staticmethod
    def add_watermark(video_path: str, watermark_path: str, output_path: str, progress=None) -> bool:
        """Add watermark image to video."""
        try:
            cmd = [
                "ffmpeg",
                "-i", video_path,
                "-i", watermark_path,
                "-filter_complex", "[0:v][1:v]overlay=W-w-10:H-h-10",
                "-c:a", "copy",
                output_path
            ]
            
            process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            total_duration = FFmpegProcessor.get_video_duration(video_path)
            
            if progress and total_duration > 0:
                for line in process.stderr:
                    match = re.search(r'time=(\d+):(\d+):(\d+\.\d+)', line)
                    if match:
                        hours, minutes, seconds = match.groups()
                        current_time = int(hours) * 3600 + int(minutes) * 60 + float(seconds)
                        current_bytes = os.path.getsize(output_path) if os.path.exists(output_path) else 0
                        
                        try:
                            if asyncio.iscoroutinefunction(progress.update_progress):
                                loop = asyncio.get_event_loop()
                                loop.create_task(progress.update_progress(
                                    current=int(current_time * 1000),
                                    total=int(total_duration * 1000),
                                    filename=os.path.basename(output_path),
                                    speed=f"{current_bytes / (1024*1024) / max(1, current_time):.2f} MB/s"
                                ))
                        except RuntimeError:
                            pass
            
            process.wait()
            logger.info(f"Watermark added: {output_path}")
            return True
        except Exception as e:
            logger.error(f"Error adding watermark: {e}")
            return False

    @staticmethod
    def add_subtitle(video_path: str, subtitle_path: str, output_path: str, progress=None) -> bool:
        """Add subtitle to video."""
        try:
            cmd = [
                "ffmpeg",
                "-i", video_path,
                "-i", subtitle_path,
                "-c", "copy",
                "-c:s", "mov_text",
                output_path
            ]
            
            process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            total_duration = FFmpegProcessor.get_video_duration(video_path)
            
            if progress and total_duration > 0:
                for line in process.stderr:
                    match = re.search(r'time=(\d+):(\d+):(\d+\.\d+)', line)
                    if match:
                        hours, minutes, seconds = match.groups()
                        current_time = int(hours) * 3600 + int(minutes) * 60 + float(seconds)
                        current_bytes = os.path.getsize(output_path) if os.path.exists(output_path) else 0
                        
                        try:
                            if asyncio.iscoroutinefunction(progress.update_progress):
                                loop = asyncio.get_event_loop()
                                loop.create_task(progress.update_progress(
                                    current=int(current_time * 1000),
                                    total=int(total_duration * 1000),
                                    filename=os.path.basename(output_path),
                                    speed=f"{current_bytes / (1024*1024) / max(1, current_time):.2f} MB/s"
                                ))
                        except RuntimeError:
                            pass
            
            process.wait()
            logger.info(f"Subtitle added: {output_path}")
            return True
        except Exception as e:
            logger.error(f"Error adding subtitle: {e}")
            return False

    @staticmethod
    def remove_stream(video_path: str, stream_type: str, output_path: str, progress=None) -> bool:
        """Remove audio or video stream."""
        try:
            if stream_type.lower() == "audio":
                cmd = ["ffmpeg", "-i", video_path, "-c:v", "copy", "-an", output_path]
            elif stream_type.lower() == "video":
                cmd = ["ffmpeg", "-i", video_path, "-c:a", "copy", "-vn", output_path]
            else:
                return False

            process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            total_duration = FFmpegProcessor.get_video_duration(video_path)
            
            if progress and total_duration > 0:
                for line in process.stderr:
                    match = re.search(r'time=(\d+):(\d+):(\d+\.\d+)', line)
                    if match:
                        hours, minutes, seconds = match.groups()
                        current_time = int(hours) * 3600 + int(minutes) * 60 + float(seconds)
                        current_bytes = os.path.getsize(output_path) if os.path.exists(output_path) else 0
                        
                        try:
                            if asyncio.iscoroutinefunction(progress.update_progress):
                                loop = asyncio.get_event_loop()
                                loop.create_task(progress.update_progress(
                                    current=int(current_time * 1000),
                                    total=int(total_duration * 1000),
                                    filename=os.path.basename(output_path),
                                    speed=f"{current_bytes / (1024*1024) / max(1, current_time):.2f} MB/s"
                                ))
                        except RuntimeError:
                            pass
            
            process.wait()
            logger.info(f"Stream removed ({stream_type}): {output_path}")
            return True
        except Exception as e:
            logger.error(f"Error removing stream: {e}")
            return False

    @staticmethod
    def sync_subtitle(subtitle_path: str, delay: float, output_path: str) -> bool:
        """Sync subtitle by adding delay."""
        try:
            delay_ms = int(delay * 1000)
            cmd = [
                "ffmpeg",
                "-i", subtitle_path,
                "-c:s", "srt",
                "-time_delta", str(delay_ms),
                output_path
            ]
            subprocess.run(cmd, check=True, capture_output=True)
            logger.info(f"Subtitle synced: {output_path}")
            return True
        except Exception as e:
            logger.error(f"Error syncing subtitle: {e}")
            return False

    @staticmethod
    def calculate_duration(start_time: str, end_time: str) -> float:
        """Calculate duration from start and end times."""
        def time_to_seconds(time_str):
            parts = time_str.split(":")
            return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])

        return time_to_seconds(end_time) - time_to_seconds(start_time)
