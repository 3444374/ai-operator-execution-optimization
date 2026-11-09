"""Install only the experiment allowance, then run the unchanged native worker."""
import runpy
import sys

from physical_budget import install

assert sys.argv[1:5]==['-u','-B','-m','src.experiments.native_daft_query']
sys.argv=['src.experiments.native_daft_query',*sys.argv[5:]]
install()
runpy.run_module('src.experiments.native_daft_query',run_name='__main__')
