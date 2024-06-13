
import argparse
import logging
import time
import math

from copier import Copier
from metadata import load_metadata, write_metadata
from actions import run_action, Variables
from validation import validate
from lockfile import acquire_lock, release_lock

ACTION_HELP = 'Format "<action>:<arg>". Action types: [curl:<url>, cmd:<command>]. Variables (use Python format string): [file_count, copy_count, copy_size, runtime, runtime_ms]'


def format_bytes(bytes_num: int) -> str:
    suffixes = ['B', 'KB', 'MB', 'GB', 'TB']
    suffix_index = 0

    while bytes_num >= 1024 and suffix_index < len(suffixes) - 1:
        bytes_num /= 1024
        suffix_index += 1

    return f"{bytes_num:.3f} {suffixes[suffix_index]}"


def format_seconds(seconds: int) -> str:
    mins = math.floor(seconds / 60)
    secs = seconds - mins * 60
    return f'{mins:02}:{secs:02}'


def setup_logging(log_file):
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s: %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )


def main():
    parser = argparse.ArgumentParser(description='Quickly copy a source folder to a destination folder using sidecar metadata')
    parser.add_argument('--log_file', type=str, help='Path to the log file', default='fastcopy.log')
    parser.add_argument('--metadata_file', type=str, help='Name of the metadata file to place in the destination directory', default='fastcopy.json')
    parser.add_argument('--src', type=str, required=True, help='Source directory')
    parser.add_argument('--dst', type=str, required=True, help='Destination directory')
    parser.add_argument('--dst_type', type=str, required=True, help='Destination type to validate. Options: [dir, mountPoint]')
    parser.add_argument('--start_action', type=str, action='append', help=f'Action(s) to run before starting. {ACTION_HELP}', default=[])
    parser.add_argument('--complete_action', type=str, action='append', help='Action(s) to run when complete. {ACTION_HELP}', default=[])
    parser.add_argument('--error_action', type=str, action='append', help='Action(s) to run if an error occurs. {ACTION_HELP}', default=[])
    parser.add_argument('--exclude', type=str, action='append', help='Exclude pattern for directories', default=[])
    args = parser.parse_args()

    setup_logging(args.log_file)
    logger = logging.getLogger(__name__)

    logger.info(f'Copying files from "{args.src}" to "{args.dst}"')

    try:
        if not acquire_lock(args.dst):
            logger.error('A copy operation is already running')
            return

        vars = Variables(file_count=0, copy_count=0, copy_size=0, runtime=0, runtime_ms=0)
        def run_actions(actions: list[str]):
            for action in actions:
                run_action(action, vars)

        if not validate(args.dst, args.dst_type):
            run_actions(args.error_action)

        logger.info('Validated destination')
        
        run_actions(args.start_action)
        logger.info('Ran startup actions')

        start_time = time.time()
        metadata = load_metadata(args.dst, args.metadata_file)
        copier = Copier(metadata, args.src, args.dst, args.exclude)
        logger.info('Loaded metadata')

        logger.info('Beginning copy...')
        copier.run()

        vars.file_count = copier.total_count
        vars.copy_count = copier.copy_count
        vars.copy_size = copier.copy_size
        runtime = time.time() - start_time
        vars.runtime = int(runtime)
        vars.runtime_ms = int(runtime * 1000)

        logger.info(
            'Finished copying:\n\tTotal files: {total}\n\tFiles copied: {copy}\n\tCopy size: {size}\n\tRuntime: {time}'.format(
                total=vars.file_count,
                copy=vars.copy_count,
                size=format_bytes(vars.copy_size),
                time=format_seconds(vars.runtime)
            )
        )

        run_actions(args.complete_action)
        logger.info('Ran completion actions')

        write_metadata(args.dst, args.metadata_file, metadata)
        logger.info('Wrote metadata')
    finally:
        release_lock(args.dst)


if __name__ == '__main__':
    main()
