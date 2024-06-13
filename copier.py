import os
from typing import Optional
import shutil
import asyncio
import logging
from threading import Lock

from metadata import Metadata, FileRecord


def load_info(path: str) -> Optional[FileRecord]:
    if not os.path.exists(path):
        return None
    
    return FileRecord(size=os.path.getsize(path), last_write=int(os.path.getmtime(path)))


class Copier:
    metadata: Metadata
    src: str
    dst: str
    exclude: list[str]
    copy_count: int
    total_count: int
    copy_size: int
    error: bool
    lock: Lock
    logger: logging.Logger

    def __init__(self, metadata: Metadata, src: str, dst: str, exclude: list[str]) -> None:
        self.metadata = metadata
        self.dst = dst
        self.src = src
        self.exclude = exclude
        self.copy_count = 0
        self.total_count = 0
        self.copy_size = 0
        self.lock = Lock()
        self.error = False
        self.logger = logging.getLogger(__name__)

    def run(self):
        asyncio.run(self._run())

    async def _run(self):
        try:
            tasks = []

            for dirpath, dirs, filenames in os.walk(self.src):
                dirs[:] = [d for d in dirs if d not in self.exclude]

                for filename in filenames:
                    file_path = os.path.join(dirpath, filename)
                    relative_path = os.path.relpath(file_path, self.src)
                    tasks.append(self._process(relative_path))
                    self.total_count += 1

            await asyncio.gather(*tasks)
        except Exception as e:
            self.logger.error(f'Copy failed: {e}')
            with self.lock:
                self.error = True

    async def _process(self, path: str):
        src_info = load_info(os.path.join(self.src, path))
        if not src_info:
            self.logger.warning(f'Failed to read info for file: "{path}"')
            return
        
        if self._needs_copy(path, src_info):
            await self._copy(path, src_info)
        
        # Always write to handle case of empty starting metadata
        with self.lock:
            self.metadata.files[path] = src_info

    async def _copy(self, path, src_info: FileRecord):
        try:
            src_path = os.path.join(self.src, path)
            dst_path = os.path.join(self.dst, path)
            os.makedirs(os.path.dirname(dst_path), exist_ok=True)
            await asyncio.to_thread(shutil.copy2, src_path, dst_path)

            with self.lock:
                self.copy_count += 1
                self.copy_size += src_info.size
        except Exception as e:
            self.logger.error(f'Failed to copy file "{path}": {e}')
            with self.lock:
                self.error = True

    def _needs_copy(self, path: str, src_info: FileRecord) -> bool:
        dst_path = os.path.join(self.dst, path)
        entry = self.metadata.files.get(path, None)

        # If no entry we can read directly
        if not entry:
            entry = load_info(dst_path)
            if not entry:
                return True
        
        return src_info.size != entry.size or src_info.last_write != entry.last_write
