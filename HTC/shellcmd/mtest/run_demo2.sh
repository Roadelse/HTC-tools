#!/bin/bash

NPROCS=4

# >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> solid
jdfile=materials/demo2.toml

outfile=$(python ../src/jobdef-parser.py $jdfile)

mpiexec -n $NPROCS python ../src/mpi_backend.py $outfile
