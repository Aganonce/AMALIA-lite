import logging
import numpy as np
from scipy.sparse import csr_matrix

logger = logging.getLogger(__name__.split('.')[-1])

from archetypes.ResponseTypeArchetype import ResponseTypeArchetype
from archetypes.TimeSeriesArchetype import TimeSeriesArchetype
import tools.Cache as Cache

class ResponseTypeFeature:
    '''

    Calculates the probabilities of users replying to each other.

    Parameters
    ----------
    
    act : 1D float array
        This variable contains each user's total baseline activity.          
    
    Output
    ----------
    
    This class outputs a dictionary of dictionary of csr matrices, where the key
    to the first dictionary is the platform, the key to the second dictionary is
    the type of response and the value is a csr sparse matrix where the (i, j)
    entry is the number of times i has responded to j, divided by the baseline 
    activity of j. 
    '''

    def __init__(self, cfg):
        self.cfg = cfg
    
    @Cache.amalia_cache
    def compute(self, dfs):
        ts = TimeSeriesArchetype(self.cfg).compute(dfs)
        rc = ResponseTypeArchetype(self.cfg).compute(dfs)
        res = {}
        logger.info('Calculating response probabilities.')
        for platform in ts:            
            act = ts[platform].sum(axis=1, dtype=float)
            act = np.fromiter(map(lambda x: x if x != 0 else 1, act), dtype=np.float)
            act = np.reciprocal(act)
            res[platform] = {}
            for a_type in rc[platform]:
                res[platform][a_type] = rc[platform][a_type].getH().multiply(act).getH()
        return res