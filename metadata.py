from dataclasses import dataclass, asdict
import json
import os
import logging

@dataclass
class FileRecord:
    size: int
    last_write: int

@dataclass
class Metadata:
    files: dict[str, FileRecord]


def load_metadata(dst_dir: str, filename: str) -> Metadata:
    logger = logging.getLogger(__name__)

    try:
        path = os.path.join(dst_dir, filename)
        with open(path, 'r') as handle:
            data = json.loads(handle.read())
            return Metadata(
                files={
                    path: FileRecord(**record)
                    for path, record in data['files'].items()
                }
            )
    except Exception as e:
        logger.warning(f'Failed to load metadata: {e}')
        return Metadata(files={})
    

def write_metadata(dst_dir: str, filename: str, data: Metadata):
    logger = logging.getLogger(__name__)

    try:
        with open(os.path.join(dst_dir, filename), 'w') as handle:
            handle.write(json.dumps(asdict(data)))
    except Exception as e:
        logger.error(f'Failed to write metadata: {e}')
