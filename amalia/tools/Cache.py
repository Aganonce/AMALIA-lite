import os
import pickle
import logging
import json
import glob
import hashlib


logger = logging.getLogger(__name__.split('.')[-1])


_cfg = None


def amalia_cache(func):
    """
    AMALIA Caching decorator
    Automatically detects if the decorated function has been called in the same python thread using the fact that the default hash()  function is randomly seeded in each thread.
    Uses the function name and arguments to store the pickeled results on disk, and the results are loaded if the function has already been called with those exact arguments before in the same thread.
    The function will only cache anything if the PYTHONHASHSEED is unset or set to 'random'
    """
    # Checks if using hash() to differentiate threads will work
    # If it can't, it records a warning and no caching will be done
    if 'PYTHONHASHSEED' in os.environ and not os.environ['PYTHONHASHSEED'] == "random":
        logger.warning(
            "amalia_cache decorator requires for the PYTHONHASHSEED environment variable to be unset or 'random'")
        return func

    def func_wrapper(*args, **kwargs):
        global _cfg
        # Gets unique process hash at runtime
        runtime_hash = hash("amalia_cache runtime hash")
        # The config should be set, but doing imports in a weird way can disrupt that
        if _cfg == None:
            logger.error("Config object has not been set properly for the cache decorator. Caching is disabled as a result. Please ensure that you are importing the cache decorator exactly as in test_cache.py")
            return func(*args, **kwargs)
        # If the config has not been set to cache, then do not cache
        if not _cfg.get("enable_cache", type=bool, default=True):
            return func(*args, **kwargs)
        # Lets the user manually set a cache folder in the config
        cache_path = _cfg.get(
            "cache_path", type=str, default="./.cache/")
        # Lets caching still work between threads, if the user allows it
        if _cfg.get("use_last_cache", type=bool, default=False):
            if os.path.exists(os.path.join(cache_path, "last_runtime_hash.txt")):
                with open(os.path.join(cache_path, "last_runtime_hash.txt"), "r") as fp:
                    runtime_hash = json.load(fp)
        # Stores the runtime hash we're using if we aren't looking at previous runs
        else:
            with open(os.path.join(cache_path, "last_runtime_hash.txt"), "w") as fp:
                json.dump(runtime_hash, fp)
        # Turns function call information into a hash by first pickling it
        pickle_str = ""
        try:
            # Use _ordered_collection() to guard against dicts or sets being pickled differently between runs
            pickle_str = func.__name__ + \
                str(pickle.dumps(_ordered_collection(args))) + \
                str(pickle.dumps(_ordered_collection(kwargs)))
        except (pickle.PicklingError, RuntimeError):
            # Sometimes the things passed in may not be picklable, so the normal function will be run
            logger.error(
                f"Pickle could not parse the arguments for cached function {func.__name__}. This may be because one or more of the arguments are functions or classes that are not defined at the top level of a module.")
            return func(*args, **kwargs)
        func_hash = hashlib.md5(pickle_str.encode("utf-8")).hexdigest()
        result = None
        # If the right file has been saved, load it's contents as the result
        if os.path.exists(os.path.join(cache_path, f"{runtime_hash}_{func_hash}.txt")):
            with open(os.path.join(cache_path, f"{runtime_hash}_{func_hash}.txt"), "rb") as fp:
                result = pickle.load(fp)
        # Otherwise call the base function and save the results
        else:
            result = func(*args, **kwargs)
            with open(os.path.join(cache_path, f"{runtime_hash}_{func_hash}.txt"), "wb") as fp:
                try:
                    pickle.dump(result, fp)
                except (pickle.PicklingError, RuntimeError):
                    logger.error(
                        f"Pickle could not save caching results. This may be because one or more of the arguments are functions or classes that are not defined at the top level of a module.")
        return result
    return func_wrapper


def set_cache_config(global_cfg):
    global _cfg
    _cfg = global_cfg


def print__cfg():
    global _cfg
    print(_cfg)


def flush_cache():
    logger.info("Flushing cache...")
    global _cfg
    if _cfg == None:
        logger.error(
            "Cache config was unset when the cache was to be flushed. No changes have been made.")
        return
    cache_path = _cfg.get("cache_path", type=str, default="./.cache/")
    for filename in glob.glob(cache_path):
        if os.path.isfile(os.path.join(cache_path, filename)):
            try:
                os.unlink(os.path.join(cache_path, filename))
            except Exception:
                logger.error(
                    f"Could not remove file {filename} from the cache.")


def _is_sortable(obj):
    cls = obj.__class__
    return cls.__lt__ != object.__lt__ or \
        cls.__gt__ != object.__gt__


def _is_iterable(obj):
    try:
        _ = iter(obj)
        return True
    except TypeError:
        return False


def _ordered_collection(item):
    if type(item) != dict and type(item) != set:
        logger.debug(
            "Tried to order an item that is not a set or dict, returning itself")
        if type(item) == list:
            item = [_ordered_collection(a) for a in item]
        return item
    ret = []
    if type(item) == dict:
        for a, b in item.items():
            if not _is_sortable(a):
                logger.error(
                    f"Using primary key of type {type(a)}, which is not orderable")
                return item
            ret.append((a, _ordered_collection(b)))
        ret = sorted(ret, key=lambda x: x[0])
    else:
        for a in item:
            if not _is_sortable(a):
                logger.error(
                    f"Using primary key of type {type(a)}, which is not orderable")
                return item
            ret.append(a)
        ret = sorted(ret)
    return ret
