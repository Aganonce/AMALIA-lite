import logging

logger = logging.getLogger(__name__.split('.')[-1])

import numpy as np
import collections
from scipy.sparse import csr_matrix, csc_matrix
import tools.Cache as Cache


class ResponseTypeArchetype:
    '''
    Response type archetype generates a sparse matrix representation
    of how often users have responded to each other with the various
    response types of a given platform.
    Parameters
    ----------
    response_types : dict (default : {'Twitter':['reply', 'retweet', 'quote']})
        A dictionary of response event types, where the key
        is the platform and the value is a list of possible
        responsible types.
    Output
    ------
    This class outputs a dictionary of dictionaries of csr matrices,
    where the first key is the platform, the second key is the action type
    and the value is a csr sparse matrix that represents the amount of
    times a user has responded to another user. The row index and the col
    index can be mapped to a userID given the platform's node_map.

    
    '''

    def __init__(self, cfg):
        self.response_types = cfg.get('response_type_archetype.response_types')
        self.cfg = cfg

    @Cache.amalia_cache
    def compute(self, dfs):
        logger.info('Generating response counts.')
        platforms = dfs.get_platforms()
        res = {}
        for platform in platforms:
            node_map = dfs.get_node_map(platform)
            res[platform] = _process_function(dfs.get_df(platform), node_map, self.response_types[platform])

        return res


def _process_function(df, node_map, response_types, *args, **kwargs):
    dep = {}
    nm_dict = dict(zip(node_map, range(len(node_map))))
    for action in response_types:
        df_t = df[df['actionType'] == action]
        df_temp = df[(df.nodeID.isin(df_t['parentID'].tolist()))]
        if len(df_temp) > 0:
            nID_to_uID_map = dict(zip(df_temp.nodeID.tolist(), df_temp.nodeUserID.tolist()))
            df_t = df_t[df_t.parentID.isin(nID_to_uID_map.keys())]
            rows = df_t['nodeUserID'].tolist()
            cols = list(map(nID_to_uID_map.__getitem__, df_t['parentID']))
            row_ind = [nm_dict[i] for i in rows]
            col_ind = [nm_dict[i] for i in cols]
            counter = collections.Counter(zip(row_ind, col_ind))
            indices = list(zip(*counter.keys()))
            dep[action] = csr_matrix((list(counter.values()), (indices[0], indices[1])),
                                     shape=((len(node_map), len(node_map))))
        else:
            dep[action] = csr_matrix(((len(node_map), len(node_map))), dtype=int)

    return dep