Docker build and integration tests
==================================

The project has two Docker workflows. The default target is the lightweight
image used for the Python test suite. The opt-in Compose environment builds a
feature-complete ADIOS2 and tests campaigns over local disk, SSH, HTTPS,
S3-compatible RustFS, and XRootD HTTP. Every access method is tested with
individual files and an indexed TAR containing ADIOS BP, HDF5, IMAGE, and TEXT
datasets from ``testdata/``.

For the incremental source-editing workflow with VSCode, persistent ADIOS2
build artifacts, and focused debugging, see :doc:`development`.
To run host-built tools against the services published by Docker, see
:doc:`development_host`.

Requirements
------------

Install Docker with the Compose and Buildx plugins. Run the commands below from
the hpc-campaign repository root. The full integration image builds the AWS C++
SDK and ADIOS2, so its first build takes considerably longer than the default
image. Docker caches those native layers independently of the hpc-campaign
source code.

Select the ADIOS2 source
------------------------

Compose supplies ADIOS2 upstream ``master`` as a named BuildKit context by
default. Set ``ADIOS2_SOURCE`` to test another source. A local checkout includes
its current working tree, including uncommitted edits::

   export ADIOS2_SOURCE=/mnt/wsl/shared/ADIOS2

A Git context can select a branch, tag, release, or full commit::

   export ADIOS2_SOURCE=https://github.com/ornladios/ADIOS2.git#my-branch
   export ADIOS2_SOURCE=https://github.com/ornladios/ADIOS2.git#v2.12.1
   export ADIOS2_SOURCE=https://github.com/ornladios/ADIOS2.git#ae374045062ebc8920e88c6d21aafcb1ec1ef245

Unset the variable to return to upstream ``master``::

   unset ADIOS2_SOURCE

The equivalent direct Docker build uses ``--build-context``::

   docker build \
     --file dockerfiles/Dockerfile \
     --target integration \
     --build-context adios2-source=/mnt/wsl/shared/ADIOS2 \
     --tag hpc-campaign-integration .

Replace the local path with one of the Git URLs above to build a remote
revision. BuildKit invalidates the ADIOS2 build layer when the selected source
context changes.

Build the regular test image
----------------------------

The default Dockerfile target remains the CI-style Python image::

   docker build --file dockerfiles/Dockerfile --tag hpc-campaign-ci .
   docker run --rm hpc-campaign-ci poetry run pytest -q

Build and use the integration environment
-----------------------------------------

Build the full runner and its service images using the selected ADIOS2 source::

   docker compose \
     --file dockerfiles/compose.integration.yaml \
     --profile manual build

Start the shared services and create all ten campaigns::

   docker compose \
     --file dockerfiles/compose.integration.yaml \
     up --detach \
       s3-service.docker.hpc-campaign setup \
       https-service.docker.hpc-campaign \
       ssh-service.docker.hpc-campaign \
       xrootd-service.docker.hpc-campaign

Open an interactive shell in the full-featured image::

   docker compose \
     --file dockerfiles/compose.integration.yaml \
     --profile manual run --rm runner

The runner starts ``hpc_campaign connector`` on port 30000. Example commands
inside the container are::

   hpc_campaign ls testdata-
   hpc_campaign manager testdata-https-tar.aca info -rfdc
   python -m tests.integration.verify_campaigns_incontainer --campaign-store /campaigns

Run the complete matrix non-interactively
-----------------------------------------

The test profile creates the campaigns, waits for every service health check,
and reads the generated campaigns with the selected ADIOS2 build::

   docker compose \
     --file dockerfiles/compose.integration.yaml \
     --profile test run --rm integration-tests

The matrix contains ``files`` and ``tar`` campaigns for ``local``, ``ssh``,
``https``, ``s3``, and ``xrootd``. Campaign and service data live in named
volumes so they remain available for manual inspection after a test run.

The verifier opens every dataset separately, so one failure does not hide the
rest of the matrix. It prints every successful and failed read, summarizes all
failures, and exits nonzero unless all 110 protocol/layout/dataset combinations
pass. This is intentionally a development report for now; it can become a CI
gate once the full matrix is green.

Inspect and stop the services
-----------------------------

The services are published only on loopback:

* RustFS S3 API: port 9000
* RustFS console: port 9001
* HTTPS: port 8443
* SSH: port 2222
* XRootD HTTP: port 8080

The credentials and generated SSH/TLS keys are for local tests only. Stop the
environment without deleting its data::

   docker compose --file dockerfiles/compose.integration.yaml down

Add ``--volumes`` to also delete generated campaigns, test data, and RustFS
objects.
