import logging

logger = logging.getLogger(__name__.split('.')[-1])

import time
from datetime import datetime
from os import listdir
from os.path import isfile, join, isdir
import pandas as pd
import sys
import json


class DataFrame:
    '''

    The DataFrame handles all data loading before passing it to the archetypes.

    Parameters
    ----------

    fpaths : dict
        Dictionary where the key is the data identifier and the value is a string pointing to the data associated
        with that identifier. Generally, if the data is primary (CSV, and following the PNNL format), the 
        identifier should be the name of the platform that data originates from.

    Notes
    -----

    Currently the DataFrame takes CSV or JSON. If CSV is passed in, the DataFrame assumes it is a primary dataset
    associated with a platform and stores it as a Pandas Dataframe. If JSON is passed in, the DataFrame isolates it 
    from the CSVs, preventing the identifier from being associated with actual platforms, and stores the data as a 
    dictionary. JSON data must be called explicitly from get_df(), and the identifier will not show up when
    get_platforms() is called.

    '''

    def __init__(self, fpaths, **kwargs):
        logger.info('Initializing dataframe.')
        self.dataframes = load(fpaths)
        self.node_maps = _generate_node_maps(self.dataframes)
        self.reverse_node_maps = _generate_reverse_node_map(self.node_maps)

    def get_df(self, key) -> pd.DataFrame:
        '''

        Call the data stored in DataFrame by key.

        Parameters
        ----------

        key : str
            The key (identifier) that is associated with the target data as passed in with fpaths.

        Output
        ------

        This function outputs either a Pandas Dataframe (if the identifier was associated with a CSV dataset)
        or a dictionary (if the identifier was associated with a JSON dataset).

        '''

        try:
            return self.dataframes[key]
        except:
            logger.error(
                'Dataframe \'' + key + '\' was not loaded in config. Please add required dataframe path to config prior to runtime.',
                exc_info=True)
            sys.exit()

    def get_platforms(self):
        '''

        Return a list of the identifiers of all platforms loaded into DataFrame

        '''

        platforms = []
        for platform in self.dataframes:
            if isinstance(self.dataframes[platform], pd.DataFrame):
                platforms.append(platform)
        return platforms

    def get_node_map(self, key):
        '''

        Return a list of unique nodeUserIDs extracted from a specified primary dataset

        Parameters
        ----------

        key : str
            The key (identifier) that is associated with the target data as passed in with fpaths.

        '''

        return self.node_maps[key]

    def get_reverse_node_map(self, key):
        return self.reverse_node_maps[key]

    def get_time_range(self, key):
        t_start = self.dataframes[key]['nodeTime'].iloc[0]
        t_end = self.dataframes[key]['nodeTime'].iloc[-1]
        logger.debug(f'{key} has time range {t_start} - {t_end}')
        return t_start, t_end


# Given a primary dataset (loaded in as a Pandas Dataframe), extract all unique nodeUserIDs and store them in a sorted list
def _generate_node_maps(dataframes):
    node_maps = {}
    for key in dataframes:
        df = dataframes[key]
        if isinstance(df, dict):
            continue
        elif isinstance(df, list):
            continue
        elif isinstance(df, pd.DataFrame):
            node_maps[key] = sorted(df['nodeUserID'].unique().tolist())
        else:
            logger.error('Loaded dataframe does not have correct datatype. ' +
                         f'Instead has type of "{type(df).__name__}". Please rectify in config prior to runtime.',
                         exc_info=True)
            sys.exit()
    return node_maps


def _generate_reverse_node_map(node_maps):
    reverse_node_maps = {}
    for platform in node_maps:
        reverse_node_maps[platform] = {user_id: idx for idx, user_id in enumerate(node_maps[platform])}

    return reverse_node_maps


def load(fpaths):
    res = {}
    for k, v in fpaths.items():
        logger.info(f'Loading {k}')
        is_json = False
        if isfile(v):
            if 'json' in v:
                is_json = True
                try:
                    with open(v) as f:
                        data = json.load(f)
                    res[k] = data
                except:
                    l_data = []
                    with open(v) as f:
                        for line in f:
                            l_data.append(json.loads(line))
                    res[k] = l_data
            else:
                res[k] = pd.read_csv(v)
        elif isdir(v):
            files = _get_files(v)
            if all([('json' in fn) for fn in files]):
                is_json = True
                try:
                    res[k] = {}
                    for fn in files:
                        with open(fn) as f:
                            res[k].update(json.load(f))
                except:
                    res[k] = []
                    for fn in files:
                        l_data = []
                        with open(fn) as f:
                            for line in f:
                                l_data.append(json.loads(line))
                        res[k] += l_data
            else:
                res[k] = pd.concat(map(pd.read_csv, _get_files(v)))
        else:
            logger.error(
                'Path \'' + v + '\' does not exist. Please add correct dataframe path to config prior to runtime.',
                exc_info=True)
            sys.exit()
        if not is_json:
            if isinstance(res[k]['nodeTime'].iloc[0], str):
                try:
                    res[k]['nodeTime'] = res[k]['nodeTime'].apply(
                        lambda x: time.mktime(datetime.strptime(x, '%Y-%m-%d %H:%M:%S').timetuple()))
                except ValueError:
                    res[k]['nodeTime'] = res[k]['nodeTime'].apply(
                        lambda x: time.mktime(datetime.strptime(x, '%Y-%m-%dT%H:%M:%SZ').timetuple()))
            res[k] = res[k].sort_values(by=['nodeTime'])
    return res


def _get_files(fpath):
    return [fpath + f for f in listdir(fpath) if isfile(join(fpath, f))]
