from typing import Callable
import logging
from dataclasses import dataclass
import subprocess

import requests

@dataclass
class Variables:
    file_count: int
    copy_count: int
    copy_size: int
    runtime: int
    runtime_ms: int

ActionHandler = Callable[[str, Variables], None]


def handle_curl(url: str, vars: Variables):
    requests.get(url.format(**vars.__dict__))


def handle_command(cmd: str, vars: Variables):
    cmd = cmd.format(**vars.__dict__)
    subprocess.run(cmd, check=True, shell=True)


ActionHandlers: dict[str, ActionHandler] = {
    'curl': handle_curl,
    'cmd': handle_command,
}


def run_action(action: str, vars: Variables):
    logger = logging.getLogger(__name__)

    parts = action.split(':')
    handler = ActionHandlers.get(parts[0], None)
    if not handler:
        logger.warning(f'Ignoring unrecognized action: {action}')
        return
    
    try:
        handler(':'.join(parts[1:]), vars)
    except Exception as e:
        logger.error(f'Action "{action}" failed: {e}')
