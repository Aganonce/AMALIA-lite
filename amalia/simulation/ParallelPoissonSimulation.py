import logging

from tools.EventGeneration import convert_date, generate_random_time, generate_random_node_id

logger = logging.getLogger(__name__.split('.')[-1])

from features.ResponseTypeFeature import ResponseTypeFeature
from features.ReplayTimeSeriesFeature import ReplayTimeSeriesFeature
import tools.Cache as Cache

# Required for MapReduce functionality
from dask import delayed
from tools.MapReduce import map_reduce, reduce_list, partition_nodes

import random
import pandas as pd

import warnings
from scipy.sparse import SparseEfficiencyWarning
warnings.simplefilter('ignore', SparseEfficiencyWarning)

random.seed(1234)

class ParallelPoissonSimulation:
    '''

    Simple event simulation. Given a replay of base events
    and probabilities of responses, generate arbitrary single-layer 
    event cascades.

    Parameters
    ----------

    Parameters here

    '''

    def __init__(self, cfg, generate_replies=None, **kwargs):
        self.start_date = cfg.get("limits.start_date", type=convert_date)
        self.end_date = cfg.get("limits.end_date", type=convert_date)
        self.time_delta = cfg.get("limits.time_delta", type=pd.Timedelta).total_seconds()
        self.nodes_per_thread = cfg.get("parallel_poisson_simulation.nodes_per_thread")
        self.n_workers = cfg.get('dask.n_workers')

        if generate_replies is None:
            self.generate_replies = cfg.get("parallel_poisson_simulation.generate_replies", True)
        else:
            self.generate_replies = generate_replies

        self.cfg = cfg

    @Cache.amalia_cache
    def compute(self, dfs, train_dfs=None):
        # Retrieve replay time-series feature and response type feature
        ts = ReplayTimeSeriesFeature(self.cfg).compute(dfs)
        responses = ResponseTypeFeature(self.cfg).compute(dfs)

        res = []

        platforms = dfs.get_platforms()
        for platform in platforms:
            ts = ts[platform]
            responses = responses[platform]
            node_map = dfs.get_node_map(platform)

            # For all users that have a nonzero row in their ts, generate events
            logger.info('Generating new events.')
            nonzero_rows, __ = ts.nonzero()

            if self.n_workers < 2:
                logger.error('ParallelPoissonSimulation requires dask workers (>1) to run for MapReduce functionality.')
                raise ValueError('ParallelPoissonSimulation requires dask workers (>1) to run for MapReduce functionality.')

            # Partition all base event generators into bins
            # Each Dask worker will recieve one bin and run only the ids within that bin
            nodes = partition_nodes(len(nonzero_rows), self.nodes_per_thread)

            # Define kwargs to scatter and pass to worker function
            scatter_data_kwargs = {'ts': ts, 'node_map': node_map}
            map_function_kwargs = {'nonzero_node_map': nonzero_rows, 'start_date': self.start_date, 'responses': responses,
                                'generate_replies': self.generate_replies, 'platform': platform}

            # Run MapReduce
            sub_res = map_reduce(nodes,
                            delayed(_worker_generate_base_event),
                            delayed(reduce_list),
                            scatter_data_kwargs=scatter_data_kwargs,
                            map_function_kwargs=map_function_kwargs).compute()
            res = res + sub_res

        # Return a pandas DataFrame sorted by time  
        # Feed into the output module for actual result generation  
        res = pd.DataFrame(res)

        if len(res) == 0:
            logger.error('ParallelPoissonSimulation produced no events. Terminating.')
            raise ValueError('ParallelPoissonSimulation produced no events.')

        return res.sort_values(by=['nodeTime']).reset_index(drop=True)


# Worker function that each Dask worker will run
def _worker_generate_base_event(nodes, ts, node_map, nonzero_node_map, start_date, responses, generate_replies, platform):
    res = []
    for root_user_id in nonzero_node_map[nodes]:
        ts_row = ts.getrow(root_user_id)
        __, events = ts_row.nonzero()

        # For each user, get event counts and the time index in which those events occurred
        event_counts = [ts_row.getcol(event).toarray()[0][0] for event in events]
        for i in range(len(event_counts)):
            for j in range(event_counts[i]):
                # Generate the base event
                current_day_time = int(start_date + events[i] * 86400)
                root_event_id = generate_random_node_id()
                res.append({'nodeID': root_event_id, 'nodeUserID': node_map[root_user_id], 'parentID': root_event_id,
                            'rootID': root_event_id, 'actionType': 'tweet', 'nodeTime': current_day_time,
                            'platform': platform})
                # Generate responses to the base event

                if generate_replies:
                    generated_responses = _generate_responses(root_event_id, root_user_id, current_day_time, responses,
                                                              node_map, platform)

                    # if len(generated_responses) == 0:
                    #     msg = 'Root user ID ' + str(root_user_id) + ' generated no responses.'
                    #     logger.warning(msg)

                    res = res + generated_responses
    return res


def _generate_responses(root_event_id, root_user_id, current_day_time, responses, node_map, platform):
    res = []

    # For each event type generate responses using associated probabilities
    for response_type in responses:
        # Get the user response probabilities for the given event type and root user id
        response_row = responses[response_type].getrow(root_user_id)

        # If the probability is below some threshold, zero it out
        # Have the users associated with the nonzero indices generate an event
        response_row[response_row < random.random()] = 0
        __, acting_indices = response_row.nonzero()

        # Generate random timestamps and find the associated user id for each new event
        time_stamps = [generate_random_time(current_day_time) for x in acting_indices]
        node_user_ids = [node_map[x] for x in acting_indices]

        res = res + [{'nodeID': generate_random_node_id(), 'nodeUserID': node_user_id, 'parentID': root_event_id,
                      'rootID': root_event_id, 'actionType': response_type, 'nodeTime': node_time,
                      'platform': platform} for
                     node_user_id, node_time in zip(node_user_ids, time_stamps)]
    return res

