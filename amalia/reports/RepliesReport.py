import logging
from typing import Dict

from reports import Report

logger = logging.getLogger(__name__.split('.')[-1])

from amalia import ReportWriter, ConfigHandler
import pandas as pd
import matplotlib.pyplot as plt

class RepliesReport(Report):

    def __init__(self, cfg: ConfigHandler, results: Dict[str, pd.DataFrame]):
        super().__init__(cfg, results)
        for config in results:
            results[config]['nodeTime'] = pd.to_datetime(results[config]['nodeTime'], unit='s')


    def _make_activity_vs_time_plot(self, ax, results):
        group_time = self.cfg.get('summary.aggregation_freq', default='1d')
        activity_vs_time = {}
        for config in results:
            activity_vs_time[config] = results[config].groupby(pd.Grouper(key='nodeTime', freq=group_time))[
                'nodeID'].nunique()

        activity_vs_time = pd.DataFrame.from_dict(activity_vs_time, orient='columns')
        activity_vs_time.plot(ax = ax)
        ax.set_ylabel("# of posts")


    def write(self, report: ReportWriter):
        f, (base_ax, replies_ax) = plt.subplots(2, 1)
        base_events = {}
        for config in self.results:
            base_events[config] = self.results[config][self.results[config]['parentID'] == self.results[config]['nodeID']]

        replies = {}
        for config in self.results:
            replies[config] = self.results[config][self.results[config]['parentID'] != self.results[config]['nodeID']]

        self._make_activity_vs_time_plot(base_ax, base_events)
        base_ax.set_title('Base Events')

        self._make_activity_vs_time_plot(replies_ax, replies)
        replies_ax.set_title('Replies')

        report.section('Replies')
        report.section('Events per Day', level=2)
        f.set_size_inches(5, 5)
        plt.tight_layout()
        report.savefig()
        plt.close()

        report.section('Actions', level=2)
        for platform in self.results:
            report.section(platform, level=3)
            table = {}
            for action_type in list(self.results[platform]['actionType'].unique()):
                table[action_type] = [len(self.results[platform][self.results[platform]['actionType'] == action_type])]
            report.table(pd.DataFrame.from_dict(table, orient='columns'))



