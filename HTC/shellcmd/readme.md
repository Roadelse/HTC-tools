# shellcmd

+ This code aims to provide an easy way to run many indenpendent shell commands (or run one python function many times) in parallel, as a standard high-throughput computation job.
+ Therefore, there must not exist dependency among all tasks.

## Usage
+ Set input job definition config file Manually (See examples of `mtest/materials/demo1.toml` and `mtest/materials/deom2.toml`), supporting both **toml** and **json**
+ use `src/jobdef-parser.py` to generate all shell commands or python function parameters, i.e., `python .../jobdef-parser.py jobdef.toml`
+ run `mpi_backend.py` with mpi, i.e., `mpiexec -np 4 python mpi_backend.py jobdef.cmd`

## src/
### jobdef-parser.py
    + Parse job definition file into shell commands or python function parameters

### mpi\_backend.py
    + Run target shell commands or python functions in HTC style via MPI
