import hashlib
import json
import logging
import sys
from zipfile import ZipFile

import pandas as pd
import numpy as np
import os
import subprocess
from amalia.tools.ConfigHandler import ConfigHandler
from pathlib import Path

logger = logging.getLogger(__name__.split('.')[-1])


class OutputWriter:
    """Responsible for saving output in known format

    Parameters
    ----------
    cfg:
    """

    def __init__(self, cfg: ConfigHandler):

        self.destination = cfg.get('output.destination', type=Path)
        self.config_string = cfg.as_json()

        self.identifier = cfg.get('output.header.identifier')
        self.write_config = cfg.get('output.write_config', default=True)
        self.id_sep = cfg.get('output.id_sep', default='$$$', type=str)
        self.lines = cfg.get('output.lines', default=True)

        if cfg.get('output.include_git_hash', default=False):
            self.identifier += 'GIT' + _get_git_hash()
        if cfg.get('output.include_config_hash', default=True):
            self.identifier += 'HASH' + _get_config_hash(self.config_string)

        if not self.destination.exists():
            logger.warning("Destination directory does not exist.")

    def _build_header(self):
        return {
            'identifier': self.identifier,
        }

    def _separate_ids(self, df):
        results = []
        for row in df.to_dict('records'):
            for nodeUserID in row['nodeUserID'].split(self.id_sep):
                results.append(row)
                results[-1]['nodeUserID'] = nodeUserID

        return pd.DataFrame.from_records(results)

    def _change_types(self, df):
        if df.dtypes['nodeTime'] in [float, np.float32, np.float64]:
            df['nodeTime'] = df['nodeTime'].astype(int).astype(str)

    def _lowercase_platforms(self,df):
        df['platform'] = df['platform'].str.lower()


    def write(self, result):
        logger.info(f"Writing results for {self.identifier} in {self.destination}")
        if type(result) != pd.DataFrame:
            logger.error(f'Trying write non pandas dataframe type {type(result)}. Terminating')
            raise ValueError(f'Cannot write type {type(result)}.')

        if not (self.destination / self.identifier).exists():
            os.mkdir(self.destination / self.identifier)

        result = self._separate_ids(result)
        self._change_types(result)

        if self.lines:
            ext = '.ndjson'
        else:
            ext = '.json'

        with open(self.destination / self.identifier / (self.identifier + ext), 'w') as output_file:
            if self.lines:
                self._write_lines(output_file, result)
            else:
                self._write_structured(output_file, result)
            logger.debug(f"Wrote output file for {self.identifier}.")

        with open(self.destination / self.identifier / 'node_list.txt', 'w') as node_list:
            for info_id in result['nodeUserID'].unique():
                node_list.write(info_id + '\n')

        if self.write_config:
            with open(self.destination / self.identifier / 'config.yaml', 'w') as config_file:
                config_file.write(self.config_string)
                logger.debug(f"Wrote config file for {self.identifier}.")

        return result

    def _write_lines(self, output_file, result):
        output_file.write(json.dumps(self._build_header()))
        output_file.write('\n')
        result.to_json(output_file, orient='records', lines=True)

    def _write_structured(self, output_file, result:pd.DataFrame):
        records = result.to_dict('records')
        doc = self._build_header()
        doc['data'] = records
        json.dump(doc, output_file)


def _get_git_hash():
    working_directory = Path(os.path.abspath(__file__)).parent  # get the directory in which this file lives
    p = subprocess.Popen(['git', 'log', '--pretty=format:%h', '-n', '1'], cwd=str(working_directory),
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    p.wait()
    hash, error = p.communicate()
    if p.returncode != 0:
        logger.error("git returned non-zero status code.")
    if error:
        logger.error(error.decode('utf-8'))
    return hash.decode('utf-8')


def _get_config_hash(config_string):
    return hashlib.md5(config_string.encode()).hexdigest()[:7]
