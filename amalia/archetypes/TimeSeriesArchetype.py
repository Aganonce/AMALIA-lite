import logging

logger = logging.getLogger(__name__.split('.')[-1])

import sys
import itertools
import numpy as np
import pandas as pd
import scipy.sparse as ss
import tools.Cache as Cache


class TimeSeriesArchetype:
    '''

    Time series archetype generates a sparse matrix representation
    of user time series within primary dataframes. Each time series
    is created by binning activity within some time delta.

    Parameters
    ----------

    time_delta : int (default : 86400)
        The time range (in seconds) to bin activity together.

    base_action : dict (defalt : {'Twitter' : 'tweet'})
        A dictionary of base event types, where the key is
        the platform and the value is the action type that
        represents base activity within the platform.

    Output
    ------

    This class outputs a dictionary of a dictionary of csc matrices, 
    where the key is the platform, the second key if the information id
    and the value is a csc sparse matrix that represents the binned activity 
    time series associated with the platform and information id. The row 
    index can be mapped to a userID given the platform's node_map. All time
    series data that is not associated with an information id is binned in
    'None'.

    Notes
    -----

    A csc matrix is used here because it performs quicker
    column-slicing operations, which makes for faster
    replay segmentation within the ReplayTimeSeriesFeature
    specifically.
    
    '''

    def __init__(self, cfg):
        self.time_delta = cfg.get('limits.time_delta', type=pd.Timedelta).total_seconds()
        self.base_action = cfg.get('time_series_archetype.base_actions')

        self.cfg = cfg

    @Cache.amalia_cache
    def compute(self, dfs):
        logger.info('Generating base activity time series.')
        platforms = dfs.get_platforms()
        res = {}
        for platform in platforms:
            min_time, max_time = dfs.get_time_range(platform)
            time_steps = int(_get_time_bins(max_time, min_time, self.time_delta) + 1)
            node_map = dfs.get_node_map(platform)

            res[platform] = _process_function(dfs.get_df(platform), node_map, min_time, self.time_delta,
                                                    time_steps, self.base_action[platform])

        return res


def _process_function(df, node_map, min_time, time_delta, time_steps, base_action, *args, **kwargs):
    df = df[df['actionType'] == base_action]
    data = np.ones(len(df))
    row_ind = np.searchsorted(node_map, df.nodeUserID)
    col_ind = np.maximum(_get_time_bins(df.nodeTime, min_time, time_delta), np.zeros(len(df)))
    return ss.csc_matrix((data, (row_ind, col_ind)), shape=(len(node_map), time_steps), dtype=np.uint32)


def _get_time_bins(max_time, min_time, time_delta):
    return ((max_time - (max_time % time_delta)) - min_time) // time_delta