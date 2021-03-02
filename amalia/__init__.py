import logging

logger = logging.getLogger(__name__.split('.')[-1])

import os
import pandas as pd
from tools.OutputWriter import OutputWriter
from tools.ConfigHandler import ConfigHandler
from tools.MapReduce import open_dask, close_dask
import tools.Cache as Cache
from tools.ReportWriter import ReportWriter
from tools.EventGeneration import convert_date
from reports.ReportFactory import ReportFactory
from dataframe.DataFrame import DataFrame, load
from simulation.SimulationFactory import SimulationFactory


def _get_debug_trim(cfg: ConfigHandler):
    trim = cfg.get("debug.trim_rows", 0, int)
    if trim == 0:
        return None
    return trim


def compare(config_path):
    compare_cfg = ConfigHandler(config_path)
    results = {}

    source = compare_cfg.get('source', False)
    if source:
        logger.debug(f'Using source {source}')
        source_df = load({'source': source})['source']

        start = compare_cfg.get('source_limits.start_date', type=convert_date, default='2018-01-01 00:00:00')
        end = compare_cfg.get('source_limits.end_date', type=convert_date, default='2018-04-14 23:59:59')

        source_df = source_df[(start < source_df['nodeTime']) & (source_df['nodeTime'] < end)]
        results['source'] = source_df

    for config in compare_cfg.get('configs'):
        results[config] = run(config)


    factory = ReportFactory(compare_cfg, results)
    report = ReportWriter("Amalia Report")
    for report_name in compare_cfg.get('reports'):
        factory.get_report(report_name).write(report)

    with open(compare_cfg.get('output'), 'w') as outfile:
        outfile.write(report.finish())

    return results

def run(config_path):
    cfg = ConfigHandler(config_path)

    logging.basicConfig(level=getattr(logging, cfg.get('debug.log_level', default='DEBUG')),
                        format='%(asctime)s %(name)s %(levelname)s: %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')

    Cache.set_cache_config(cfg)
    sim_key = cfg.get('sim_type')

    n_workers = cfg.get('dask.n_workers')

    if n_workers > 1:
        memory_limit = cfg.get('dask.memory_limit')
        local_directory = cfg.get('dask.local_directory')
        if local_directory == 'default':
            local_directory = os.getcwd()
        open_dask(n_workers=n_workers, memory_limit=memory_limit, local_directory=local_directory)
        logger.info('Initializing Dask')

    data_loader = DataFrame(cfg.get('data_loader', type=dict))
    result = SimulationFactory(cfg).run_simulation(sim_key, data_loader)

    if n_workers > 1:
        close_dask()
        logger.info('Closing Dask')

    return OutputWriter(cfg).write(result)
