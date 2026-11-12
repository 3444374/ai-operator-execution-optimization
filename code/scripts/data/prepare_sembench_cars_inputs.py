"""Prepare original Cars CSVs from existing full metadata; no media downloads."""
import argparse
import json
from pathlib import Path
import sys

sys.path.insert(0,str(Path(__file__).resolve().parents[2]))

from src.baselines.text.sembench_cars import FULL_SCALE_FACTOR,prepare_inputs


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    for name in ('sembench-checkout','source-dir','output-dir'):
        parser.add_argument('--'+name,required=True)
    parser.add_argument('--scale-factor',type=int,default=FULL_SCALE_FACTOR)
    args=parser.parse_args()
    report=prepare_inputs(args.sembench_checkout,args.source_dir,args.output_dir,args.scale_factor)
    print(json.dumps(report))


if __name__=='__main__':main()
