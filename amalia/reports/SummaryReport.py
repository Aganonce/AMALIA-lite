import logging
from typing import Dict

from reports import Report

logger = logging.getLogger(__name__.split('.')[-1])

from amalia import ReportWriter, ConfigHandler
import pandas as pd
import matplotlib.pyplot as plt


class SummaryReport(Report):

    def __init__(self, cfg: ConfigHandler, results: Dict[str, pd.DataFrame]):
        super().__init__(cfg, results)
        for config in results:
            results[config]['nodeTime'] = pd.to_datetime(results[config]['nodeTime'], unit='s')

    def _get_num_events(self, key):
        return self.results[key]['nodeID'].nunique()

    def _get_num_unique_users(self, key):
        return self.results[key]['nodeUserID'].nunique()

    def _get_average_events_per_second(self, key):
        return (self.results[key]['nodeID'].nunique() /
                (self._get_end_time(key) - self._get_start_time(key)).total_seconds()) * (60 * 60 * 24)

    def _get_start_time(self, key):
        return self.results[key]['nodeTime'].min()

    def _get_end_time(self, key):
        return self.results[key]['nodeTime'].max()

    def _make_activity_vs_time_plot(self):
        group_time = self.cfg.get('summary.aggregation_freq', default='1d')
        activity_vs_time = {}
        for config in self.results:
            activity_vs_time[config] = self.results[config].groupby(pd.Grouper(key='nodeTime', freq=group_time))[
                'nodeID'].nunique()

        activity_vs_time = pd.DataFrame.from_dict(activity_vs_time, orient='columns')
        
        # print(activity_vs_time)
        activity_vs_time.plot()
        plt.ylabel("# of posts")
        plt.tight_layout()

    def write(self, writer: ReportWriter):
        writer.section("Summary")

        table = {
            "Total Events": [self._get_num_events(key) for key in self.results],
            "Unique Users": [self._get_num_unique_users(key) for key in self.results],
            "First Event": [self._get_start_time(key) for key in self.results],
            "Last Event": [self._get_end_time(key) for key in self.results],
            "Average Events Per Day": [self._get_average_events_per_second(key) for key in self.results]
        }

        table = pd.DataFrame.from_dict(table, orient='index', columns=self.results.keys())
        writer.table(table)

        writer.section("Activity vs Time", level=2)
        writer.p("Number of posts per day")
        self._make_activity_vs_time_plot()
        writer.savefig()
        plt.close()

