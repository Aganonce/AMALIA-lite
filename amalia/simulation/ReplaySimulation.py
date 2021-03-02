import logging

from amalia import ConfigHandler

logger = logging.getLogger(__name__.split('.')[-1])

import pandas as pd

import numpy as np

from features.ReplayTimeSeriesFeature import ReplayTimeSeriesFeature


class ReplaySimulation:
    '''

    Arbitrary replay simulation. Passes feature data forward.

    Parameters
    ----------

    No parameters

    '''

    def __init__(self, cfg: ConfigHandler):
        self.cfg = cfg

    def compute(self, dfs, train_dfs=None):
        ts = ReplayTimeSeriesFeature(self.cfg).compute(dfs)
        df = pd.concat(dfs.get_df(platform) for platform in dfs.get_platforms())
        
        return df
