# 2D Orszag-Tang MHD Example

This directory contains the C++ solver sources for a 2D uniform-grid ideal-MHD
Orszag-Tang vortex example.

Included here:

- `CMakeLists.txt`
- `src/`
- `scripts/run_ensembles.py`
- `ensembles/`

Not yet moved into this repo:

- rendering and analysis scripts
- campaign creation helper scripts
- generated run output directories

## Capabilities

The solver supports:

- multiple solver variants: `hll`, `rusanov`, `muscl_hll`, `muscl_rusanov`
- optional MPI domain decomposition in `y`
- ADIOS2 output
- output cadence control by step and/or simulation time

Each output step writes 2D fields such as:

- `rho`
- `pressure`
- `vx`, `vy`, `vz`
- `bx`, `by`, `bz`
- `speed`
- `current_z`

Optional outputs:

- `psi` via `--psi`
- `mx`, `my`, `mz` via `--m`
- `E` via `--E`

Scalar diagnostics per step on rank 0 include:

- `step`, `time`
- `mass`
- `kinetic_energy`, `magnetic_energy`, `internal_energy`, `total_energy`
- `mean_pressure`
- `max_speed`
- `current_abs_max`, `current_rms`
- `divb_abs_max`, `divb_l2`

## Build

Requirements:

- CMake >= 3.18
- C++17 compiler
- ADIOS2 with C++ bindings
- MPI (optional, enabled by default)

From this directory:

```bash
cmake -S . -B build -DOT_ENABLE_MPI=ON
cmake --build build -j
```

For a serial-only build:

```bash
cmake -S . -B build -DOT_ENABLE_MPI=OFF
cmake --build build -j
```

## Run a single case

Serial:

```bash
./build/ot_mhd \
  --nx 512 --ny 512 \
  --solver muscl_hll \
  --tfinal 0.8 --cfl 0.3 \
  --output-dir runs/ot_muscl \
  --output-every-steps 20 \
  --output-every-time 0.05
```

MPI:

```bash
mpirun -n 2 ./build/ot_mhd \
  --nx 512 --ny 512 \
  --solver muscl_hll \
  --tfinal 0.8 --cfl 0.3 \
  --output-dir runs/ot_muscl_mpi \
  --output-every-steps 20 \
  --output-every-time 0.05
```

Each run output directory contains:

- `output.bp`
- `input_parameters.txt`

## Important controls

- `--solver`: `rusanov`, `hll`, `muscl_hll`, `muscl_rusanov`
- `--output-dir <path>`: writes `<path>/output.bp` and `<path>/input_parameters.txt`
- `--prepend-var-names <str>`: prepends `<str>` to every ADIOS variable name
- `--fixed-dt <float>`: force constant timestep
- `--psi`: output the GLM cleaning field `psi`
- `--m`: output momentum fields `mx`, `my`, `mz`
- `--E`: output total energy density field `E`
- `--output-every-steps N`: dump every `N` steps (`0` disables)
- `--output-every-time dt`: dump every `dt` in simulation time (`0` disables)
- `--save-initial` / `--no-save-initial`
- `--glm-ch`, `--glm-damping` for divergence-cleaning behavior
- `--rho-floor`, `--p-floor` for positivity

Post-processing, analysis, and campaign-ingest helpers are planned to be moved
into this example later.

## Ensemble runner

This example now includes a JSON-driven ensemble runner:

```bash
python3 scripts/run_ensembles.py --help
```

It expects a `--config` JSON file describing the ensemble.

Available configs in `ensembles/`:

- `fast_mpi_ensemble.json`
- `resolution_sweep.json`
- `simple.json`

Examples:

```bash
python3 scripts/run_ensembles.py --binary ./build/ot_mhd --config ensembles/simple.json
```

Dry run:

```bash
python3 scripts/run_ensembles.py --binary ./build/ot_mhd --config ensembles/simple.json --dry-run
```

Run only selected members:

```bash
python3 scripts/run_ensembles.py --binary ./build/ot_mhd --config ensembles/simple.json --only muscl_hll hll_first_order
```

Override ranks for all selected runs:

```bash
python3 scripts/run_ensembles.py --binary ./build/ot_mhd --config ensembles/fast_mpi_ensemble.json --ranks 8
```

Override the ensemble output root:

```bash
python3 scripts/run_ensembles.py --binary ./build/ot_mhd --config ensembles/simple.json --output-dir ./runs_custom
```

Resolution sweep example:

```bash
python3 scripts/run_ensembles.py --binary ./build/ot_mhd --config ensembles/resolution_sweep.json
```

The ensemble configs write run output under `runs/` relative to this example
directory unless `--output-dir` is provided.

## TL;DR

```bash
python3 ./scripts/run_ensembles.py \
  --binary ./build/ot_mhd \
  --config ./ensembles/resolution_sweep.json
```
