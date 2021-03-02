from collections import defaultdict


def get_time_bins(max_time, min_time, time_delta):
    """
    Returns number of bins of size `time_delta` between `max_time` and `min_time`
    """
    return ((max_time - (max_time % time_delta)) - min_time) // time_delta


def dataframe_to_dict_array(df):
    results = {}
    for idx, dat in df.iterrows():

        # cursor to keep track of current path
        cur = results

        # for each level
        for i, level in enumerate(idx):
            # if this is the last level
            if i == len(idx) - 1:
                # set the data here
                cur[level] = dat.values

            else:
                if level not in cur:
                    # Create the level if it does not exist
                    cur[level] = {}
                # move the cursor down a level
                cur = cur[level]

    return results

