import argparse
import itertools
import json
import logging
import os
import subprocess
import sys
import time
from logging.config import fileConfig
from multiprocessing import Process, Queue
from pathlib import Path
from glob import glob

import pandas as pd
import psutil

logging.basicConfig(
    stream=sys.stderr,
    level=logging.DEBUG,
    datefmt='%Y-%m-%d %H:%M:%S',
    format='%(asctime)s [%(levelname)8s] [%(filename)24s:%(lineno)4s] [%(processName)16s] [%(name)s] %(message)s'
)
logger = logging.getLogger(__file__)


def profiler_daemon(queue: Queue, target_pid: int, profiler_dir: Path):
    profiler_pid = os.getpid()
    logger.info(f'profiler daemon pid: {profiler_pid};  root pid: {target_pid}')
    queue.put_nowait(('measure', None))
    records = []
    meta = {
        'start_time(ms)': int(round(time.time() * 1000))
    }
    i = 0
    while True:
        item = queue.get(True, None)
        event, val = item
        if event in {'measure'}:
            logger.debug(f'profiler iteration: {i}')
            try:
                parent_ps = psutil.Process(target_pid)
                for ps in [parent_ps] + parent_ps.children(recursive=True):
                    if psutil.pid_exists(ps.pid) and ps.pid != profiler_pid:
                        pps = ps.parent()
                        thread_count = len(glob(f'/proc/{ps.pid}/task/*/'))
                        records.append(
                            {
                                'index': i,
                                'pid': ps.pid,
                                'parent_pid': pps.pid if pps is not None else None,
                                'measure_time': int(round(time.time() * 1000)),
                                'cpu': ps.cpu_percent(0.5),
                                'rss': ps.memory_info().rss,
                                'vms': ps.memory_info().vms,
                                'thread_count': thread_count,
                            }
                        )
            except psutil.NoSuchProcess as e:
                pass
            i += 1
            time.sleep(0.5)
            queue.put_nowait(('measure', None))
        elif event in {'terminate'}:
            meta['end_time(ms)'] = int(round(time.time() * 1000))
            meta['running_time(ms)'] = meta['end_time(ms)'] - meta['start_time(ms)']
            logger.info(f'shutting down profiler...')
            pd.DataFrame.from_records(records).to_csv(profiler_dir.joinpath('profiling_result.csv'), index=False)
            with profiler_dir.joinpath('run.json').open('wt') as run_json:
                json.dump(meta, run_json)
            break
        else:
            *prefixes, key = event.split('.')
            this_dict = meta
            for pfx in prefixes:
                if pfx not in this_dict:
                    this_dict[pfx] = {}
                this_dict = this_dict[pfx]
            this_dict[key] = val


if __name__ == "__main__":

    args = sys.argv[1:]
    profiler_args = []
    delimiter_index = len(args)
    try:
        delimiter_index = args.index('--')
    except ValueError as e:
        pass

    profiler_args, program_args = args[0:delimiter_index], args[(delimiter_index + 1):len(args)]

    parser = argparse.ArgumentParser()
    parser.add_argument('profile_dir')
    parser.add_argument('--logging-config', default=None)
    parsed_args = parser.parse_args(profiler_args)

    if parsed_args.logging_config:
        logging_config = Path(parsed_args.logging_config).expanduser()
        if logging_config.exists():
            fileConfig(logging_config, defaults=None, disable_existing_loggers=True)

    profile_dir = Path(parsed_args.profile_dir).expanduser()
    profile_dir.mkdir(parents=True, exist_ok=True)
    logger.info(f"created profile_dir: {str(profile_dir)}")

    proc = subprocess.Popen(program_args, stdout=sys.stdout, stderr=sys.stdout, stdin=sys.stdin)
    queue = Queue()
    profiler = Process(target=profiler_daemon, daemon=True, args=(queue, proc.pid, profile_dir))
    logger.info(f'spawning profiler daemon...')
    profiler.start()
    proc.wait()
    queue.put_nowait(('terminate', None))
    profiler.join()

