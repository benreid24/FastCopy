import os

import psutil


def is_process_alive(pid: int):
    try:
        process = psutil.Process(pid)
        return process.is_running()
    except psutil.NoSuchProcess:
        return False


def acquire_lock(dst: str) -> bool:
    lock_file = os.path.join(dst, 'fastcopy.lock')
    
    def write_pid():
        with open(lock_file, 'w') as handle:
            handle.write(f'{os.getpid()}')

    if not os.path.exists(lock_file):
        write_pid()
        return True
    
    with open(lock_file, 'r') as handle:
        if is_process_alive(int(handle.read())):
            return False
    
    write_pid()
    return True

def release_lock(dst: str):
    lock_file = os.path.join(dst, 'fastcopy.lock')

    try:
        os.remove(lock_file)
    except FileNotFoundError:
        pass
