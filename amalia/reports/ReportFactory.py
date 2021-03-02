import logging

logger = logging.getLogger(__name__.split('.')[-1])

import importlib


class ReportFactory:

    def __init__(self, cfg, results):

        self.results = results
        self.cfg = cfg

    def get_report(self, name):
        try:
            mod = importlib.import_module(f'reports.{name}')
        except ImportError as e:
            logger.error(f'Unable to find report {name}. Skipping...')
            raise e

        try:
            return getattr(mod, name)(self.cfg, self.results)
        except AttributeError as e:
            logger.error(f'Report module {name} has no such report {name}.')
            raise e
