import logging
from typing import Dict

from reports import Report

logger = logging.getLogger(__name__.split('.')[-1])

from amalia import ReportWriter, ConfigHandler
import pandas as pd
import matplotlib.pyplot as plt

class PlatformReport(Report):

    def __init__(self, cfg: ConfigHandler, results: Dict[str, pd.DataFrame]):
        super().__init__(cfg, results)
        for config in results:
            results[config]['nodeTime'] = pd.to_datetime(results[config]['nodeTime'], unit='s')

    def _get_platforms(self):
        res = set()
        for config in self.results:
            res |= {platform for platform in self.results[config]['platform'].unique()}

        return res

    def _get_num_events(self, key):
        return {platform: self.results[key][self.results[key]['platform'] == platform]['nodeID'].nunique()
                for platform in self._get_platforms()}


    def _get_start_time(self, key):
        return self.results[key]['nodeTime'].min()

    def _get_end_time(self, key):
        return self.results[key]['nodeTime'].max()

    def _get_num_events_per_day(self, key):
        events = self._get_num_events(key)
        res = {}
        for platform in events:
            res[platform] = (events[platform] / (self._get_end_time(key) - self._get_start_time(key)).total_seconds()) * (60 * 60 * 24)

        return res

    def write(self, report: ReportWriter):
        report.section('Platforms Report')
        report.section('Total Unique Events by Platform', level=2)
        for config in self.results:
            report.section(config, level=3)
            self.results[config].groupby([pd.Grouper(key='nodeTime', freq='d'), 'platform'])['nodeID'].nunique().unstack().plot()
            report.savefig()

        table = {}
        for config in self.results:
            table[config] = self._get_num_events(config)

        report.section('Total Unique Events', level=2)
        report.table(pd.DataFrame.from_dict(table, orient='index', columns=self._get_platforms()))


        table = {}
        for config in self.results:
            table[config] = self._get_num_events_per_day(config)

        report.section('Total Unique Events Per Day', level=2)
        report.table(pd.DataFrame.from_dict(table, orient='index', columns=self._get_platforms()))


