#!/usr/bin/env python
# encoding=utf-8

"""
This script is used to parse job definition config file into all separate shell commands, or parameter dict for python functions
"""

import sys
import os
import os.path
import glob
import argparse
import itertools
import json


def resolveParams(defdict):
    params_solid = {}
    for k, v in defdict["params"].items():
        if isinstance(v, list):
            raise RuntimeError("Cannot set list as solid parameters in the toml")
        if not isinstance(v, dict):
            params_solid[k] = v
    params_product = borrowed_product_withkey(defdict["params"]["product"])
    params_zip = borrowed_zip_withkey(defdict["params"]["zip"])
    return [{**a, **b, **c} for a,b,c in itertools.product([params_solid], params_product, params_zip)]

def buildCommands(params):
    commands = []
    for p in params:
        cmds = []
        for cmd in defdict["commands"]:
            cmds.append(update_cmd(cmd, p))
        commands.append(cmds)
    return commands

def update_cmd(cmd: str, p: dict):
    for k,v in p.items():
        cmd = cmd.replace(f'<{k}>', f'{v}')
    return cmd

def borrowed_zip_withkey(D: dict):
    """
    zip lists with keys, borrowed from rdee-python
    --------------------------------
    @2024-05-27
    """
    length = -1
    for k, v in D.items():
        assert hasattr(v, "__len__")
        if length == -1:
            length = len(v)
        assert length == len(v), "Different length for list in zip_withkey values"

    rst = []
    keys = list(D.keys())
    for ele in  list(zip(*list(D.values()))):
        rst.append({keys[i]:ele[i] for i in range(len(keys))})
    return rst
    
def borrowed_product_withkey(D):
    """
    product lists with keys, borrowed from rdee-python
    --------------------------------
    @2024-05-27
    """
    import itertools

    rst = []
    keys = list(D.keys())
    for ele in itertools.product(*list(D.values())):
        rst.append({keys[i]:ele[i] for i in range(len(keys))})
    return rst


if __name__ == '__main__':

    #@ Prepare
    parser = argparse.ArgumentParser(description="""Script for parsing job definition file into separate shell commands or python function parameters""")
    parser.add_argument('jdfile', type=str, help="Input job definition file")
    
    args = parser.parse_args()

    jdfile = args.jdfile

    if not jdfile:
        jdfiles_candidate = ("jobdef.toml", "jobdef.json")
        for jdfc in jdfiles_candidate:
            if os.path.exists(jdfc):
                jdfie = jdfc
                break
        if not jdfile:
            raise RuntimeError("Cannot find default job definition file, please select it manually!")
    elif not os.path.exists(jdfile):
        raise RuntimeError("Cannot find given job definition file, please confirm")

    with open(jdfile, 'r') as f:
        if jdfile.endswith(".toml"):
            import toml
            defdict = toml.load(f)
        elif jdfile.endswith(".json"):
            defdict = json.load(f)
        else:
            raise NotImplementedError("Only support .json and .toml by now")

    if "commands" in defdict:
        outfile = os.path.splitext(args.jdfile)[0] + ".cmd"
    else:
        outfile = os.path.splitext(args.jdfile)[0] + ".param"
    print(outfile)

    params = resolveParams(defdict)
    if "commands" in defdict:
        commands = buildCommands(params)
        with open(outfile, "w") as f:
            json.dump(commands, f)
    else:
        defdict["params"] = params
        with open(outfile, "w") as f:
            json.dump(defdict, f)