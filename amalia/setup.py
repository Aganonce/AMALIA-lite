#!/usr/bin/env python

from distutils.core import setup

setup(name='amalia',
    version='1.0',
    description='Adaptive Modular Application framework for Large network simulations and Information Analysis',
    author='James Flamino',
    author_email='flamij@rpi.edu',
    packages=['archetypes', 'dataframe', 'features', 'simulation', 'tools'],
    install_requires = ['atomicwrites', 'attrs', 'bokeh', 'Click', 'cloudpickle', 'cycler', 'dask[complete]', 'decorator', 'distributed', 'fastdtw', 'fsspec', 'HeapDict', 'Jinja2', 'joblib', 'kiwisolver', 'locket', 'MarkupSafe', 'matplotlib', 'msgpack', 'networkx', 'numpy', 'pandas', 'Pillow', 'pluggy', 'pyparsing', 'pytest', 'pytz', 'PyYAML', 'sklearn', 'scipy', 'tblib', 'tornado', 'zict', 'zipp', 'fastdtw', 'pyyaml']
)
