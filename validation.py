import logging
import subprocess


def _validate_mountpoint(path: str) -> bool:
    try:
        subprocess.run(['sudo', 'mount', '-a'], check=True, capture_output=True)
        subprocess.run(['mountpoint', '-q', path], check=True, capture_output=True)
        return True
    except Exception:
        return False


def validate(dst: str, expected: str):
    logger = logging.getLogger(__name__)

    if expected == 'dir':
        return True # always pass
    elif expected == 'mountPoint':
        return _validate_mountpoint(dst)
    else:
        logger.error(f'Unrecognized dst_type: {expected}')
        return False
