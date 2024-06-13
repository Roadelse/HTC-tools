#!/usr/bin/env python
# coding=utf-8


import sys
import os
import os.path
import json
import traceback

from mpi4py import MPI
import threading
from typing import Sequence


def run_cmd(infile):
    cmds = json.load(open(infile, "r"))
    def call_system(cmds):
        for cmd in cmds:
            rcode = os.system(cmd)
            if rcode != 0:
                raise RuntimeError
    
    mpp = mpiPool()
    mpp.map(call_system, cmds, "direct")
    mpp.close()


def run_func(infile):
    import importlib
    def load_module_from_path(module_path, module_name=None):
        """
        load a module from filepath
        -----------------------------------
        2024-04-12 init
        -----------------------------------
        borrowed from rdee-python
        """
        if module_name is None:
            module_name = module_path.split('/')[-1].split('.')[0]

        spec = importlib.util.spec_from_file_location(module_name, module_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
    
    paramdict = json.load(open(infile, "r"))
    file = paramdict["script"]
    
    if not file.endswith(".py"):
        raise TypeError("Unexpected filename, must be a python script")

    if os.path.exists(file):
        module = load_module_from_path(file)
    elif os.path.exists(os.path.dirname(infile) + f"/{file}"):
        module = load_module_from_path(os.path.dirname(infile) + f"/{file}")
    else:
        raise RuntimeError

    function = paramdict["function"]

    exec = getattr(module, function)

    params = paramdict["params"]

    mpp = mpiPool()
    mpp.map(exec, params, "kwargs")
    mpp.close()

    # MPI.COMM_WORLD.Barrier()


def _is_sequence(obj):
    return isinstance(obj, Sequence) and not isinstance(obj, str)

def _isinstanceAll(seq: Sequence, targetType):
    if not _is_sequence(seq):
        raise TypeError
    for ele in seq:
        if not isinstance(ele, targetType):
            return False
    return True


class mpiPool:
    """
    A pool that distributes tasks over a set of MPI processes using
    mpi4py. This class provides a similar interface to Python's 
    multiprocessing Pool, but currently only supports the
    :func:`map` method.

    Moreover, in this mpi pool, master processor can be set as a slave as well
    i.e., tasks are allocated in all processes including master proc while master
    proc arrange the tasks as well. That will take full advantage of CPUs....in 171 $ZJX

    Parameters
    ----------
    master_work : bool, optional
        Master processor take on calculation missions as well
    """
    def __init__(self, master_work = "auto"):
        self.comm = MPI.COMM_WORLD
        self.rank = self.comm.Get_rank()    # rank 0 is considered as master proc
        self.size = self.comm.Get_size()
        if isinstance(master_work, bool):
            self.master_work = master_work      # switch for the special full mode
        elif master_work.lower() == "auto":
            if self.size > 10:
                self.master_work = False
            else:
                self.master_work = True
        else:
            raise TypeError(f"Unexpected {master_work=}")
        self.comm.Barrier()

    def wait(self):
        # --- Loop, twice handshake
        #print("start to wait, rank = {}".format(self.rank))
        while 1:
            self.comm.isend(self.rank, dest = 0)    # send request for task
            arg = self.comm.recv(source = 0)        # receive task
            #print("rank = {}, arg = {}".format(self.rank, arg))
            if arg is None:                         # receive end signal
                return
            try:
                if self.argType == "kwargs":
                    result = self.function(**arg)
                elif self.argType == "args":
                    result = self.function(*arg)
                else:
                    result = self.function(arg)
            except:
                print(f"Mission failed in rank = {self.rank}")
                traceback.print_exc()
                self.comm.Abort(101)

    def map(self, func, args, argType = "kwargs"):
        

        self.argType = argType
        self.function = func                        # set function
        #print("start map")

        if self.rank != 0:                          # for slave proc 
            #print("goto wait")
            self.wait()
            return
        else:
            if argType == "kwargs":
                if not _isinstanceAll(args, dict):
                    print("Error in args type, must be list[dict] under argType=kwargs")
                    self.comm.Abort()
            elif argType == "args":
                if not _isinstanceAll(args, list) and not _isinstanceAll(args, tuple):
                    print("Error in args type, must be list[list|tuple] under argType=args")
                    self.comm.Abort()
            elif argType == "direct":
                if not _is_sequence(args):
                    print("Error in args type, must be list or tuple under argType=direct")
                    self.comm.Abort()
            else:
                print("Error in argType, must be one of (kwargs, args, direct)")
                self.comm.Abort()

        if self.size == 1:
            if self.master_work == False:
                print("Warning, 1 core need mask_work, or nothing will happens!")
                return 
            else :
                for arg in args:
                    #print("rank = 0, only master, arg = {}".format(arg))
                    if self.argType == "kwargs":
                        self.function(**arg)
                    elif self.argType == "args":
                        self.function(*arg)
                    else:
                        self.function(arg)

                return

        master_thread = None

        for arg in args:                            # loop and arrange tasks
            if self.master_work:                    # if in full mode which makes master proc work hard
                # --- use thread 
                if not isinstance(master_thread, threading.Thread) or not master_thread.is_alive():
                    #print("rank = {}, arg = {}".format(self.rank, arg))
                    if self.argType == "kwargs":
                        master_thread = threading.Thread(target = self.function, kwargs = arg)
                    elif self.argType == "args":
                        master_thread = threading.Thread(target = self.function, args = arg)
                    else:
                        master_thread = threading.Thread(target = self.function, args=(arg,))

                    master_thread.start()
                    continue
            askRank = self.comm.recv(source = MPI.ANY_SOURCE)   # receive request from slave proc
            self.comm.isend(arg, dest = askRank)                # send task arg to slave proc

    def close(self):
        # --- close all slave proc
        if self.rank == 0:
            for r in range(1, self.size):
                # --- isend is necessary, due to no synchronization of the last task for each proc
                self.comm.isend(None, dest = r)
        self.comm.Barrier()


if __name__ == "__main__":
    infile = sys.argv[1]
    if not os.path.exists(infile):
        raise RuntimeError
    
    if infile.endswith(".cmd"):
        run_cmd(infile)
    elif infile.endswith(".param"):
        run_func(infile)
    else:
        raise RuntimeError("Unknown infile format")

