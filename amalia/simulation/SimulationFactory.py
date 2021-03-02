import logging

from amalia.dataframe.DataFrame import DataFrame

logger = logging.getLogger(__name__.split('.')[-1])


class SimulationFactory:
    '''

    Write stuff here

    Parameters
    ----------
    cfg : ConfigHandler
        Simulation global config

    '''

    def __init__(self, cfg):
        self.simulation_map = initalize_simulations(cfg)

    def run_simulation(self, sim_key, data_loader: DataFrame):
        return self.simulation_map[sim_key].compute(data_loader)

def get_simulation_map():
    from simulation.ReplaySimulation import ReplaySimulation
    from simulation.ParallelPoissonSimulation import ParallelPoissonSimulation
    from simulation.PoissonSimulation import PoissonSimulation

    return {
        'ReplaySimulation': ReplaySimulation,
        'PoissonSimulation': PoissonSimulation,
        'ParallelPoissonSimulation': ParallelPoissonSimulation
    }

def initalize_simulations(cfg):
    return {name: sim(cfg) for name,sim in get_simulation_map().items()}

