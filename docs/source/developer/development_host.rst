Use the Docker services from the host
=====================================

This workflow runs hpc-campaign and ADIOS2 tools directly in the WSL or Linux
host environment while Docker provides the SSH, HTTPS, S3, and XRootD test
services. It is useful for debugging a host build without attaching a shell to
the integration container. For development inside the container, see
:doc:`development`.

The campaign archives contain stable logical host names such as
``docker-https``. ADIOS2 resolves those names through
``~/.config/hpc-campaign/hosts.yaml``. The host configuration maps them to the
ports published on ``127.0.0.1``; it does not make Docker's internal DNS names
resolvable on the host.

Prepare the host
----------------

1. Make /srv writable by $USER
2. Make /srv/data writable by $USER
   /srv/data/testdata
   /srv/data/archive
   will be created by user 

Prepare and start the services
------------------------------

Install Docker with the Compose and Buildx plugins. The host also needs an
hpc-campaign installation and a feature-complete ADIOS2 build with Campaign,
OpenSSL, AWS SDK, CURL, and XRootD support. Run the following commands from the
hpc-campaign repository root and select the ADIOS2 source to build into the SSH
service::

   export ADIOS2_SOURCE=/mnt/wsl/shared/ADIOS2

   hostcompose() {
       docker compose --file dockerfiles/compose.integration.yaml "$@"
   }

Build the services that use local source, then start all four remote services
and the one-shot campaign setup job::

   hostcompose build \
       setup \
       https-service.docker.hpc-campaign \
       ssh-service.docker.hpc-campaign

   hostcompose up --detach \
       s3-service.docker.hpc-campaign \
       setup \
       https-service.docker.hpc-campaign \
       ssh-service.docker.hpc-campaign \
       xrootd-service.docker.hpc-campaign

Compose waits for the S3 service before running ``setup`` and makes the other
services wait until setup completes. Confirm their state with::

   hostcompose ps --all

The ``setup`` service should have exited with status 0. The other four services
should be running and healthy. They publish only loopback ports:

* HTTPS: ``127.0.0.1:8443``
* S3 API: ``127.0.0.1:9000``
* SSH: ``127.0.0.1:2222``
* XRootD HTTP: ``127.0.0.1:8080``

Option 1: Export the generated campaigns from the Docker container
--------------------------------------------------------

The setup job writes ten ACA files to a Docker volume rather than the source
tree. Copy them to an ignored host build directory::

   campaign_dir="${PWD}/build/docker-campaigns"
   mkdir -p "${campaign_dir}"
   hostcompose cp setup:/campaigns/. "${campaign_dir}/"

Repeat this copy after rerunning ``setup``. The local campaigns contain
container paths under ``/srv`` and are not expected to work from the host. The
SSH, HTTPS, S3, and XRootD campaigns use logical names that can be redirected
through the host configuration below.


Option 2: Generate the campaigns on the host directly
-----------------------------------------------------

   HPC_CAMPAIGN_DIR=<hpc-campaign-source-dir>
   python3 ${HPC_CAMPAIGN_DIR}/tests/integration/create_campaigns.py  \
      --data-root /srv/data --campaign-store . \
      --source ${HPC_CAMPAIGN_DIR}/testdata  2>&1 >log.create_docker_campaigns

   hpc_campaign manager ./testdata-https-file.aca info
   bpls -l -P "include-dataset=testdata/readme" ./testdata-https-file.aca


Export the HTTPS CA and SSH key
-------------------------------

The Docker build generates a self-signed HTTPS certificate and a test-only SSH
key. Export the public CA certificate to ``certs`` and the private SSH key to
``secrets`` under the hpc-campaign configuration directory::

   config_dir="${HOME}/.config/hpc-campaign"
   mkdir -p "${config_dir}/certs" "${config_dir}/secrets"

   hostcompose cp \
       https-service.docker.hpc-campaign:/run/https/https.crt \
       "${config_dir}/certs/docker-https-ca.crt"

   hostcompose cp \
       setup:/run/ssh/client_key \
       "${config_dir}/secrets/docker-ssh-client-key"
   chmod 600 "${config_dir}/secrets/docker-ssh-client-key"

The CA certificate is not a secret and is kept separate from the private key.
ADIOS2 uses it only for this endpoint; the command does not modify the host's
system trust store. Re-export both files after a Docker rebuild that regenerates
the integration secrets, because the client files must match the running
services.

Configure host access
---------------------

Merge the following entries into ``${config_dir}/hosts.yaml``. Do not replace
unrelated host definitions already in that file::

   docker-https:
     docker-https-loopback:
       protocol: https
       endpoint: https://127.0.0.1:8443
       ca_file: ~/.config/hpc-campaign/certs/docker-https-ca.crt
       verbose: 0

   docker-rustfs:
     docker-s3-loopback:
       protocol: s3
       profile: hpc-campaign-rustfs
       endpoint: http://127.0.0.1:9000
       aws_ec2_metadata: false
       recheck_metadata: false
       verbose: 0

   docker-ssh:
     docker-ssh-loopback:
       protocol: ssh
       host: 127.0.0.1
       port: 2222
       user: campaign
       authentication: publickey
       identity_file: ~/.config/hpc-campaign/secrets/docker-ssh-client-key
       serverpath: /usr/local/bin/adios2_remote_server
       args: -background -report_port_selection
       verbose: 0

   docker-xrootd:
     docker-xrootd-loopback:
       protocol: xrootd
       transfer_protocol: http
       host: 127.0.0.1
       port: 8080
       verbose: 0

The top-level names must match the logical host names in the ACA files. The
names below them are descriptive labels and may be changed.

Configure the S3 test profile
-----------------------------

Add this profile to ``~/.aws/config``::

   [profile hpc-campaign-rustfs]
   region = us-east-1
   s3 =
       addressing_style = path

Add the matching test credentials to ``~/.aws/credentials``::

   [hpc-campaign-rustfs]
   aws_access_key_id = HPC_CAMPAIGN_TEST
   aws_secret_access_key = hpc-campaign-test-secret

These credentials are fixed local test values. Do not reuse them for a real
service.

Start the host connector
------------------------

SSH campaign reads ask the hpc-campaign connector to create the SSH connection
and launch ``adios2_remote_server``. Start it in a separate host terminal using
the same Python environment and configuration as the reader. Activate that
environment first, then run::

   hpc_campaign connector \
       -c ~/.config/hpc-campaign/hosts.yaml \
       -p 30000

Leave this process running while testing SSH. HTTPS, S3, and XRootD reads do not
need the connector.

Verify a host read
------------------

Use the feature-complete host ADIOS2 build, not a minimal Python wheel. This
example verifies the HTTPS endpoint, explicit port, CA file, and TEXT range
read::

   ~/shared/ADIOS2/build.debug/bin/bpls \
       -E Campaign \
       -P 'include-dataset=^testdata/readme$' \
       -l "${campaign_dir}/testdata-https-files.aca" \
       -d testdata/readme -Sy

The single-case verifier is also useful from the host when its Python process
imports the same feature-complete ADIOS2 build::

   python -m tests.integration.verify_campaigns \
       --campaign-store "${campaign_dir}" \
       --single-campaign testdata-https-files.aca \
       --single-dataset testdata/readme

Run the full 110-case read matrix on the host with the same loopback mappings::

   python -m tests.integration.verify_campaigns \
       --campaign-store "${campaign_dir}"

This read verifier does not perform Docker-specific service checks. Inside the
integration or development container, use
``tests.integration.verify_campaigns_incontainer`` instead. That wrapper first
checks the Docker-internal service names and connector port, then runs the same
read matrix.

Known limitations
-----------------

HTTPS and S3 access to HDF5 datasets still require a transport-backed HDF5 VFD.
Use ``include-dataset`` when focusing on BP, IMAGE, or TEXT so an unrelated HDF5
open does not abort the CampaignReader initialization. XRootD IMAGE and TEXT
reads currently fall through to the SSH simple-file path; BP and HDF5 exercise
the configured XRootD backend.

Stop the services
-----------------

Stop the environment without deleting its generated data::

   hostcompose down

Add ``--volumes`` to remove the generated campaigns, served data, and S3
objects as well. The exported host ACA, CA, and key files are not removed by
Compose.
