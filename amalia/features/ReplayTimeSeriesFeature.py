import logging

from tools.Utilities import get_time_bins

logger = logging.getLogger(__name__.split('.')[-1])

from archetypes.TimeSeriesArchetype import TimeSeriesArchetype

import time
import pandas as pd
from datetime import datetime
import tools.Cache as Cache

class ReplayTimeSeriesFeature:
    '''

    Replay some config-defined segment of the time series archetype.

    Parameters
    ----------

    start date : str or int
        The start date to begin the replay segmentation. This can be
        either a string in the '%Y-%m-%d %H:%M:%S' format or a unix
        timestamp.

    end_date : str or int
        The end date to cut off the replay segmentation.

    time_delta : int
        The time range (in seconds) to bin activity together. Must
        match the value assigned in the TimeSeriesArchetype class.

    Output
    ------

    This class outputs a dictionary of dictionaries of csr matrices, where
    the first key is the platform, the second key in the information id,
    and the value is a csr sparse
    matrix that represents the binned activity time series
    associated with the platform for a config-defined segment
    of time, less than that of the time series data generated
    from the TimeSeriesArchetype class.
    '''

    def __init__(self, cfg):
        self.start_date = cfg.get('limits.start_date', type=_convert_date)
        self.end_date = cfg.get('limits.end_date', type=_convert_date)
        self.time_delta = cfg.get('limits.time_delta', type=pd.Timedelta).total_seconds()

        self.cfg = cfg

    @Cache.amalia_cache
    def compute(self, dfs):
        ts = TimeSeriesArchetype(self.cfg).compute(dfs)

        for platform, e in ts.items():
            total_base_events = e.sum()
            logger.debug(f'Total time series events for {platform} is {total_base_events}')

        res = {}
        logger.info('Replaying time series segment.')
        for platform in ts:
            min_time, max_time = dfs.get_time_range(platform)
            start = int(get_time_bins(self.start_date, min_time, self.time_delta))
            end = int(get_time_bins(self.end_date, min_time, self.time_delta))
            logger.debug(f"Got start: {start} and end: {end} for {platform}. These will index a timeseries of shape {ts[platform].shape}")
            res[platform] = ts[platform][:, start:end].tocsr()

            if res[platform].sum() == 0:
                logger.warning(f'No events replayed for {platform}')
        del ts


        # total_events = 0
        # for platform in res:
        #     total_events += res[platform].sum()

        # print('TOTAL EVENTS ', total_events)

        return res

def _convert_date(date):
    if isinstance(date, str):
        return time.mktime(datetime.strptime(date, '%Y-%m-%d %H:%M:%S').timetuple())
    else:
        return date

