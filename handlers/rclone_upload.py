"""Rclone upload handler for cloud storage integration."""
import os
import re
import subprocess
import time
import asyncio
import json
import traceback
import logging

logger = logging.getLogger(__name__)


def check_rclone_installed():
    """Check if rclone is installed and accessible."""
    try:
        result = subprocess.run(
            ["which", "rclone"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=5
        )
        return result.returncode == 0
    except Exception as e:
        logger.debug(f"Rclone check failed: {e}")
        return False


class Status:
    """Status tracking for rclone uploads."""
    Tasks = []

    def __init__(self):
        self._task_id = len(self.Tasks) + 1

    def refresh_info(self):
        raise NotImplementedError

    def update_message(self):
        raise NotImplementedError

    def is_active(self):
        raise NotImplementedError

    def set_inactive(self):
        raise NotImplementedError


class RCUploadTask(Status):
    """Rclone upload task status tracker."""
    
    def __init__(self):
        super().__init__()
        self.Tasks.append(self)
        self._active = True
        self._upmsg = ""
        self._prev_cont = ""
        self._message = None
        self._error = ""
        self.cancel = False

    async def set_message(self, message):
        self._message = message

    async def refresh_info(self, msg):
        self._upmsg = msg

    async def create_message(self):
        """Create formatted progress message from rclone output."""
        mat = re.findall(r"Transferred:.*ETA.*", self._upmsg)
        if not mat:
            return self._upmsg
        
        nstr = mat[0].replace("Transferred:", "").strip().split(",")
        if len(nstr) >= 4:
            prg = nstr[1].strip("% ").strip()
            prg_bar = self.progress_bar(prg)
            progress = (
                f"<b>Uploaded: {nstr[0]}\n"
                f"Progress: {prg_bar} - {prg}%\n"
                f"Speed: {nstr[2]}\n"
                f"ETA: {nstr[3].replace('ETA', '').strip()}</b>\n\n"
                f"<b>Using Engine: </b><code>RCLONE</code>"
            )
            return progress
        return self._upmsg

    @staticmethod
    def progress_bar(percentage):
        """Create visual progress bar."""
        comp = "●"
        ncomp = "○"
        pr = ""
        
        try:
            percentage = int(percentage)
        except:
            percentage = 0
        
        for i in range(1, 11):
            if i <= int(percentage / 10):
                pr += comp
            else:
                pr += ncomp
        return pr

    async def update_message(self):
        """Update telegram message with progress."""
        if not self._message:
            return
        
        progress = await self.create_message()
        if not self._prev_cont == progress:
            self._prev_cont = progress
            try:
                await self._message.edit_text(progress)
            except Exception as e:
                logger.debug(f"Could not update message: {e}")

    async def is_active(self):
        return self._active

    async def set_inactive(self, error=None):
        self._active = False
        if error:
            self._error = error


async def rclone_driver(status_msg, user_id: int, filepath: str, filename: str = None) -> dict:
    """
    Upload file to rclone configured remote drive.
    
    Args:
        status_msg: Telegram message object for status updates
        user_id: Telegram user ID
        filepath: Full path to file to upload
        filename: Optional custom filename for upload (defaults to basename of filepath)
    
    Returns:
        dict: Status with success flag and details
    """
    try:
        if not check_rclone_installed():
            logger.error("Rclone binary not installed on server")
            try:
                await status_msg.edit_text(
                    "❌ RCLONE NOT INSTALLED\n━━━━━━━━━━━━━━━━━━\n\n"
                    "Rclone is not available on this server.\n"
                    "Please contact bot administrator.\n\n"
                    "Switch to Telegram upload mode to continue."
                )
            except:
                pass
            return {"success": False, "error": "rclone not installed on server"}
        
        # Get rclone config path from user's config
        conf_path = f"./userdata/{user_id}/rclone.conf"
        
        if not os.path.exists(conf_path):
            logger.error(f"Rclone config not found at {conf_path}")
            try:
                await status_msg.edit_text(
                    "❌ RCLONE CONFIG NOT FOUND\n━━━━━━━━━━━━━━━━━━\n\n"
                    "Please upload your rclone.conf file first.\n"
                    "Go to Upload Mode → Rclone"
                )
            except:
                pass
            return {"success": False, "error": "Rclone config not found"}
        
        # Read config to get drive name
        try:
            with open(conf_path, 'r') as f:
                content = f.read()
            
            # Extract first remote name from config
            match = re.search(r'\[([^\]]+)\]', content)
            if not match:
                raise ValueError("No remotes found in rclone.conf")
            
            drive_name = match.group(1)
        except Exception as e:
            logger.error(f"Error reading rclone config: {e}")
            try:
                await status_msg.edit_text(
                    f"❌ Invalid rclone config:\n{str(e)}"
                )
            except:
                pass
            return {"success": False, "error": "Invalid rclone config"}
        
        # Check if file exists
        if not os.path.exists(filepath):
            logger.error(f"File not found: {filepath}")
            try:
                await status_msg.edit_text(f"❌ File not found: {filepath}")
            except:
                pass
            return {"success": False, "error": "File not found"}
        
        file_size_mb = os.path.getsize(filepath) / (1024 * 1024)
        upload_filename = filename if filename else os.path.basename(filepath)
        
        try:
            await status_msg.edit_text(
                f"☁️ UPLOADING TO RCLONE\n━━━━━━━━━━━━━━━━━━\n\n"
                f"📁 Remote: {drive_name}\n"
                f"📄 File: {upload_filename}\n"
                f"📊 Size: {file_size_mb:.2f}MB\n\n"
                f"⏳ Uploading... 0%"
            )
        except:
            pass
        
        # Create upload task
        ul_task = RCUploadTask()
        await ul_task.set_message(status_msg)
        
        # Run rclone upload
        result = await rclone_upload(
            filepath=filepath,
            drive_name=drive_name,
            conf_path=conf_path,
            task=ul_task,
            status_msg=status_msg,
            filename=upload_filename
        )
        
        return result
    
    except Exception as e:
        logger.error(f"Rclone driver error: {e}", exc_info=True)
        try:
            await status_msg.edit_text(f"❌ Rclone error: {str(e)}")
        except:
            pass
        return {"success": False, "error": str(e)}


async def rclone_upload(filepath: str, drive_name: str, conf_path: str, task: RCUploadTask, status_msg, filename: str = None) -> dict:
    """Execute rclone copy command."""
    try:
        upload_filename = filename if filename else os.path.basename(filepath)
        # This ensures merged files go directly to the cloud storage root
        rclone_copy_cmd = [
            "rclone",
            "copy",
            f"--config={conf_path}",
            str(filepath),
            f"{drive_name}:/",
            "-P",
            "--stats=2s"
        ]
        
        logger.info(f"Running rclone: {' '.join(rclone_copy_cmd)}")
        logger.info(f"Uploading as: {upload_filename}")
        
        # Run rclone process
        def run_process():
            return subprocess.Popen(
                rclone_copy_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1
            )
        
        process = await asyncio.to_thread(run_process)
        
        # Monitor process output
        await rclone_process_display(process, status_msg, task)
        
        process.wait(timeout=1800)  # 30 minute timeout
        
        if process.returncode == 0:
            logger.info(f"Rclone upload successful: {upload_filename}")
            
            # Don't send confirmation message, just return success
            # The merge processor will handle the final message
            return {
                "success": True,
                "remote": drive_name,
                "file": upload_filename
            }
        else:
            logger.error(f"Rclone failed with code {process.returncode}")
            try:
                await status_msg.edit_text(
                    f"❌ RCLONE UPLOAD FAILED\n━━━━━━━━━━━━━━━━━━\n\n"
                    f"Check your rclone configuration."
                )
            except:
                pass
            return {"success": False, "error": "Rclone upload failed"}
    
    except asyncio.TimeoutError:
        logger.error("Rclone upload timeout")
        try:
            await status_msg.edit_text("❌ Upload timeout - file too large")
        except:
            pass
        return {"success": False, "error": "Upload timeout"}
    
    except Exception as e:
        logger.error(f"Rclone upload exception: {e}", exc_info=True)
        try:
            await status_msg.edit_text(f"❌ Upload error: {str(e)}")
        except:
            pass
        return {"success": False, "error": str(e)}


async def rclone_process_display(process, status_msg: any, task: RCUploadTask):
    """Monitor and display rclone upload progress."""
    try:
        blank = 0
        start = time.time()
        edit_time = 5
        
        while True:
            try:
                line = process.stdout.readline()
                if not line:
                    blank += 1
                    if blank >= 20:
                        break
                    await asyncio.sleep(0.5)
                    continue
                
                data = line.strip()
                blank = 0
                
                # Look for progress lines
                if "Transferred:" in data:
                    await task.refresh_info(data)
                    
                    if time.time() - start > edit_time:
                        start = time.time()
                        await task.update_message()
                    
                    logger.debug(f"Rclone: {data}")
                
                await asyncio.sleep(0.1)
            
            except Exception as e:
                logger.debug(f"Error reading rclone output: {e}")
                break
    
    except Exception as e:
        logger.error(f"Error in process display: {e}")
