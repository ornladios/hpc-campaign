Develop hpc-campaign and ADIOS2 in Docker
=========================================

This workflow is for editing hpc-campaign or ADIOS2 in VSCode, exercising a
single failing campaign read, and then running the complete integration matrix.
Both Git repositories remain on the host. The development container bind
mounts them, so no source changes are trapped in a disposable container.
To run the host's own ADIOS2 and hpc-campaign processes against the Docker
services instead, see :doc:`development_host`.

The development service is defined in ``dockerfiles/compose.development.yaml``.
It is an overlay for ``compose.integration.yaml`` and is not used by the normal
Docker or CI builds. It provides:

* the local hpc-campaign checkout at ``/workspace/hpc-campaign``;
* the local ADIOS2 checkout at ``/workspace/ADIOS2``;
* persistent ADIOS2 build and installation volumes;
* the same campaign and service volumes as the integration tests; and
* GDB and ``SYS_PTRACE`` access for native debugging.

Prepare the environment
-----------------------

Open the hpc-campaign and ADIOS2 directories as a VSCode multi-root workspace.
Edit the host files from the WSL VSCode window so new files retain the host
user's ownership. VSCode can attach to the container for its terminal and
debugger, but source edits do not need to happen through that attached window.

Run all commands below from the hpc-campaign repository root. Select the local
ADIOS2 checkout for both the development container and the SSH service::

   export ADIOS2_DEV_SOURCE=/mnt/wsl/shared/ADIOS2
   export ADIOS2_SOURCE="${ADIOS2_DEV_SOURCE}"

The development source must be a local directory; a Git URL cannot be bind
mounted. Define a short shell function for the two Compose files::

   devcompose() {
       docker compose \
           --file dockerfiles/compose.integration.yaml \
           --file dockerfiles/compose.development.yaml \
           "$@"
   }

Build the development client and the service images::

   devcompose --profile develop build developer ssh-service.docker.hpc-campaign setup https-service.docker.hpc-campaign

The first build compiles the AWS SDK and a baseline ADIOS2. Docker reuses those
layers on later image builds.

Start and enter the development container
-----------------------------------------

Start the developer service and all of its dependencies::

   devcompose --profile develop up --detach developer

Compose waits for RustFS, HTTPS, SSH, and XRootD and for the campaign setup job
to finish. Open a shell with::

   devcompose exec developer bash

The container deliberately runs ``sleep infinity`` so repeated ``exec`` calls
reuse the same environment. In VSCode, **Dev Containers: Attach to Running
Container...** can attach a terminal or debugger to
``hpc-campaign-integration-developer-1``.

Iterate on hpc-campaign
-----------------------

The host hpc-campaign checkout is first on ``PYTHONPATH``. Each new Python or
``hpc_campaign`` process therefore uses the edited files immediately, without
rebuilding an image or reinstalling the package. Run a focused test or the
project checks inside the developer shell::

   pytest -q tests/test_integration_campaign_creation.py
   ruff check hpc_campaign tests
   ruff format --check hpc_campaign tests
   mypy hpc_campaign --check-untyped-defs
   pylint hpc_campaign

The campaign connector is a long-running process started with the container.
Restart the developer service after changing connector code::

   exit
   devcompose restart developer
   devcompose exec developer bash

If campaign creation or the served test data changes, rebuild and rerun the
``setup`` service before testing those campaigns::

   devcompose build setup
   devcompose up --force-recreate setup

Iterate on ADIOS2
-----------------

Inside the developer shell, configure, compile, and install the bind-mounted
ADIOS2 source with::

   build-adios2-dev

The default build type is ``RelWithDebInfo``. The build directory and
``/opt/adios2-dev`` installation survive container recreation in named volumes.
The development installation is first on ``PATH``, ``PYTHONPATH``,
``CMAKE_PREFIX_PATH``, and ``LD_LIBRARY_PATH``. Confirm which Python module is
active with::

   python -c "import adios2; print(adios2.__version__, adios2.__file__)"

After editing ADIOS2 in VSCode, repeat ``build-adios2-dev``. Ninja recompiles
only affected files. Extra CMake definitions may be appended to the helper
command. For example, enable ADIOS2's own tests or request a Debug build with::

   ADIOS2_DEV_BUILD_TESTING=ON build-adios2-dev
   ADIOS2_DEV_BUILD_TYPE=Debug build-adios2-dev

When tests are enabled, run a selected upstream regression with::

   ctest --test-dir /workspace/ADIOS2-build \
       --output-on-failure -R Campaign

Restart the developer service after installing ADIOS2 if the long-running
campaign connector must load the new libraries. The build and install volumes
remain intact across the restart.

Reproduce and debug one read
----------------------------

The integration verifier accepts a single campaign and dataset, avoiding the
other 109 reads during diagnosis::

   python -m tests.integration.verify_campaigns \
       --campaign-store /campaigns \
       --single-campaign testdata-s3-tar.aca \
       --single-dataset testdata/T_10_15_00000.png

Use the same command after every source rebuild. For a native ADIOS2 failure,
run the focused reproducer under GDB::

   gdb --args python -m tests.integration.verify_campaigns \
       --campaign-store /campaigns \
       --single-campaign testdata-s3-tar.aca \
       --single-dataset testdata/T_10_15_00000.png

Because ADIOS2 was compiled from ``/workspace/ADIOS2``, GDB and an attached
VSCode debugger see the same source paths as the mounted checkout.

Run final validation
--------------------

First run the complete matrix against the incrementally installed build::

   python -m tests.integration.verify_campaigns_incontainer --campaign-store /campaigns

Then rebuild the disposable integration images from the same host checkout.
This catches differences hidden by an incremental build or persistent volume::

   ADIOS2_SOURCE="${ADIOS2_DEV_SOURCE}" \
   docker compose --file dockerfiles/compose.integration.yaml \
       --profile test build \
       integration-tests ssh-service.docker.hpc-campaign setup \
       https-service.docker.hpc-campaign

   docker compose --file dockerfiles/compose.integration.yaml \
       --profile test run --rm integration-tests

If only hpc-campaign changed, rebuilding ``setup`` and the HTTPS service is
necessary only when the modified code is used by those services. If ADIOS2
remote-server code changed, rebuild the SSH service as well as the client
image.

Commit the fix
--------------

Review and commit from the host checkout after both focused and complete tests
have run. A cross-project bug normally needs two forms of coverage:

* a small regression test in the repository containing the faulty behavior;
* an hpc-campaign integration case when the problem depends on a remote
  protocol, archive layout, or dataset type.

Stop only the development container with::

   devcompose stop developer

Stop the complete integration environment without deleting the persistent
campaign or ADIOS2 build volumes with::

   devcompose down
