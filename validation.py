import logging
import subprocess
import os


def _validate_mountpoint(path: str) -> bool:
    try:
        subprocess.run(['sudo', 'mount', '-a'], check=True, capture_output=True)
        subprocess.run(['mountpoint', '-q', path], check=True, capture_output=True)
        return True
    except Exception:
        return False
    

def _validate_dir(path: str) -> bool:
    try:
        os.makedirs(path, exist_ok=True)
        return os.path.isdir(path)
    except Exception:
        return False


def validate(dst: str, expected: str):
    logger = logging.getLogger(__name__)

    if expected == 'dir':
        return _validate_dir(dst)
    elif expected == 'mountPoint':
        return _validate_mountpoint(dst)
    else:
        logger.error(f'Unrecognized dst_type: {expected}')
        return False
