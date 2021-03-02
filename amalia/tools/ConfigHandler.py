import logging
from typing import Optional, Any, Callable, Union, Type, List
try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper

logger = logging.getLogger(__name__.split('.')[-1])

import yaml

class ConfigHandler:
    '''

    Class responsible for reading config JSON file and presenting it to the user.
    This class should be passed into all modules and stored as a member.

    Parameters
    ----------
    fpath : str
        Path to the config file.
    '''

    def __init__(self, fpath: str):
        self.config = _read(fpath)

    def _get_value_or_default(self, key, default):
        try:
            path = key.split('.')
            return _read_from_path(self.config, path)
        except KeyError:
            if default is not None:
                return default
            logger.error(
                'Key \'' + key + '\' not available in config. Please add missing key to config prior to runtime.',
                exc_info=True)
            raise

    def as_json(self, indent=4):
        return yaml.dump(self.config, Dumper=Dumper)

    def get(self,
            key: str,
            default: Optional[Any] = None,
            type: Union[Callable[[Union[str, int, float, bool, dict, list]], Any], Type] = None) -> Any:
        """Gets the value of a config variable.

            Parameters
            ----------
            key : str
                The key in the config file to lookup
            default : Optional[Any]
                Default value to use if key is not in the config. If None, and there is no key in the config file, a
                KeyError is thrown.
            type : Union[Callable[[Union[str, int, float, bool, dict, list]], Any], Type]
                A function to cast the result to a more usable type. For example, dates written as strings may be
                converted to pandas Timestamps with `type=pd.Timestamp`.
        """
        result = self._get_value_or_default(key, default)

        if type is not None:
            return type(result)
        else:
            return result


class TestConfigHandler(ConfigHandler):

    def __init__(self):
        self.config = {}

    def set(self, key, value):
        self.config[key] = value

def _read_from_path(config:dict, path:List[str]):
    """Reads nested dictionary.
    Eg `_read_config({'foo':{'bar':1}}, ['foo','bar']) = 1`
    """
    if len(path) == 1:
        return config[path[0]]
    return _read_from_path(config[path[0]], path[1:])

def _merge(source, destination):
    """
    https://stackoverflow.com/a/20666342/3639005
    >>> a = { 'first' : { 'all_rows' : { 'pass' : 'dog', 'number' : '1' } } }
    >>> b = { 'first' : { 'all_rows' : { 'fail' : 'cat', 'number' : '5' } } }
    >>> _merge(b, a) == { 'first' : { 'all_rows' : { 'pass' : 'dog', 'fail' : 'cat', 'number' : '5' } } }
    True
    """
    for key, value in source.items():
        if isinstance(value, dict):
            # get node or create one
            node = destination.setdefault(key, {})
            _merge(value, node)
        else:
            destination[key] = value

    return destination

def _read(fpath):
    with open(fpath) as f:
        data = yaml.load(f, Loader=Loader)

    # support legacy !include
    if '!include' in data:
        logger.warning("Detected legacy `!include`. Switch to `include`")
        to_merge = _read(data['!include'])
        data = _merge(data, to_merge)
    if 'include' in data:
        to_merge = _read(data['include'])
        data = _merge(data, to_merge)

    return data
