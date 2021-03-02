from abc import ABC, abstractmethod
from typing import Dict

import pandas as pd
from amalia import ConfigHandler, ReportWriter


class Report(ABC):
    @abstractmethod
    def __init__(self, cfg: ConfigHandler, results: Dict[str, pd.DataFrame]):
        self.cfg = cfg
        self.results = results

    @abstractmethod
    def write(self, writer: ReportWriter):
        pass
