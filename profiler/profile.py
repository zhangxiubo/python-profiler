import argparse
import itertools
import json
import logging
import os
import sys
import time
from logging.config import fileConfig
from multiprocessing import Process, Queue
from pathlib import Path

import pandas as pd
import psutil

logging.basicConfig(
    stream=sys.stderr,
    level=logging.DEBUG,
    datefmt='%Y-%m-%d %H:%M:%S',
    format='%(asctime)s [%(levelname)8s] [%(filename)24s:%(lineno)4s] [%(processName)16s] [%(name)s] %(message)s'
)
logger = logging.getLogger(__file__)


def profiler_daemon(queue: Queue, root_pid, profiler_dir: Path):
    profiler_pid = os.getpid()
    logger.info(f'profiler daemon pid: {profiler_pid};  root pid: {root_pid}')
    queue.put_nowait(('measure', None))
    records = []
    meta = {}
    i = 0
    while True:
        item = queue.get(True, None)
        event, val = item
        if event in {'measure'}:
            logger.debug(f'profiler iteration: {i}')
            index_time = int(round(time.time() * 1000))
            try:
                parent_ps = psutil.Process(root_pid)
                for ps in itertools.chain.from_iterable([[parent_ps], parent_ps.children(recursive=True)]):
                    if psutil.pid_exists(ps.pid) and ps.pid != profiler_pid:
                        pps = ps.parent()
                        records.append(
                            {
                                'index': i,
                                'pid': ps.pid,
                                'parent_pid': pps.pid if pps is not None else None,
                                'index_time': index_time,
                                'measure_time': int(round(time.time() * 1000)),
                                'cpu': ps.cpu_percent(0.5),
                                'rss': ps.memory_info().rss,
                                'vms': ps.memory_info().vms,
                            }
                        )
            except psutil.NoSuchProcess as e:
                pass
            i += 1
            time.sleep(0.5)
            queue.put_nowait(('measure', None))
        elif event in {'terminate'}:
            logger.info(f'shutting down profiler...')
            pd.DataFrame.from_records(records).to_csv(profiler_dir.joinpath('profiling_result.csv'), index_label='row_id')
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

    parser = argparse.ArgumentParser()
    parser.add_argument('profile_dir')
    parser.add_argument('--logging-config', default=None)
    args = parser.parse_args()

    if args.logging_config:
        logging_config = Path(args.logging_config).absolute()
        if logging_config.exists():
            fileConfig(logging_config, defaults=None, disable_existing_loggers=True)

    profile_dir = Path(args.profile_dir).expanduser()
    profile_dir.mkdir(parents=True, exist_ok=True)
    debug_output_dir = profile_dir.joinpath('debug_output')
    debug_output_dir.mkdir(parents=True, exist_ok=True)

    logger.info(f"profile_dir: {str(profile_dir)}")
    logger.info(f"debug_output_dir: {str(debug_output_dir)}")

    pid = os.getpid()
    queue = Queue()
    profiler = Process(target=profiler_daemon, daemon=True, args=(queue, pid, profile_dir))
    logger.info(f'spawning profiler daemon...')
    # profiler.start()

    project_dir = Path(args.project_dir).expanduser()
    logger.info(f'requested debug directory: {project_dir}')

    queue.put_nowait(('terminate', None))
    # profiler.join()

