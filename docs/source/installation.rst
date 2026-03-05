Installation
============

.. _prerequisits:

Prerequisits
------------

This toolkit requires the following Python packages.

- **paramiko**, a networking package to run a local service the I/O library can talk to to establish a remote connection
- **pyyaml**, parsing yaml configuration files
- **PyNaCl** for encoding metadata with keys
- **pillow** for processing images
- **adios2** for adding ADIOS data
- **h5py** for adding HDF5 data
- **redis** for *hpc_campaign cache* to list local cache information (Redis is used by ADIOS2 for caching)

Besides these packages, Python needs support for Tcl/Tk (python3-tk), so that *hpc_campaign connector* can pop up a dialog for password entries.

How to install
--------------

The easiest way to install HPC Campaign Management is with pip3. 

.. code-block:: console

    (.venv) $ pip3 install hpc-campaign
    (.venv) $ hpc_campaign ls

HPC Campaign Management is just a bunch of Python scripts that can be installed in your Python environment from the repository, 

.. code-block:: console

    (.venv) $ git clone https://github.com/ornladios/hpc-campaign.git
    (.venv) $ cd hpc-campaign
    (.venv) $ pip3 install -e .
    (.venv) $ hpc_campaign ls

or can be accessed without installation by setting PYTHONPATH 

.. code-block:: bash

    $ git clone https://github.com/ornladios/hpc-campaign.git
    $ cd hpc-campaign
    $ export PYTHONPATH=${PWD}:$PYTHONPATH
    $ python3 -m hpc_campaign ls


Versions
--------

ACA 0.6 version is compatible with ADIOS 2.11.0

ACA 0.7 version is compatible with ADIOS 2.12.0

Setup 
-----

~/.config/hpc-campaign/config.yaml
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

There are three paths/names important in the campaign setup. 

- `hostname` is used in the campaign archive. Define a specific name the project agrees upon (e.g. OLCF, NERSC, ALCF) that identifies the generic location of the data and then use that name later to specify the modes of remote data access.

- `campaignstorepath` is the directory where all the campaign archives are stored. This should be shared between project members in a center, and a private one on every member's laptop. It is up to the project to determine what file sharing / synchronization mechanism to use to sync this directories. 

- `cachepath` is the directory where ADIOS can unpack metadata from the campaign archive so that ADIOS engines can read them as if they were entirely local datasets. The cache contains the metadata as well as data that have already been retrieved by previous read requests. 

Use `~/.config/hpc-campaign/config.yaml` to specify these options. 

.. code-block:: bash
		
    $ cat ~/.config/hpc-campaign/config.yaml

    Campaign:
      hostname: OLCF
      campaignstorepath: /lustre/orion/csc143/proj-shared/campaign-store
      cachepath: /lustre/orion/csc143/proj-shared/campaign-cache
      verbose: 0

    $ ls -R /lustre/orion/csc143/proj-shared/campaign-store
    /lustre/orion/csc143/proj-shared/campaign-store/demoproject:
    frontier_gray-scott_100.aca

    $ hpc_campaign ls
    demoproject/frontier_gray-scott_100.aca

~/.config/hpc-campaign/hosts.yaml
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The campaign archive records locations of datasets as a host name plus directory and optionally a default protocol like https. The host name is an artifical identifier, and ADIOS needs to be told how to access data on that resource. 

Use `~/.config/hpc-campaign/hosts.yaml` to specify these access settings. ADIOS can use 

- SSH tunnels to launch a private server for the session. For this you need to have an account on that resource, and have ADIOS2 installed including the `adios2_remote_server` executable. 

- xRootD service, using various "transfer protocols" to reach the xRootD service (http, https or xroot protocols)

- S3 protocol to reach Amazon AWS cloud as well as other S3-compatible storages. Set up an AWS profile (the S3 access key id and secret key in `~/.aws/credentials`) and use that profile name here. 


.. code-block:: bash

    $ cat ~/.config/hpc-campaign/hosts.yaml

    OLCF:
        dtn-ssh:
            protocol: ssh
            host: dtn.olcf.ornl.gov
            user: user007
            authentication: passcode
            serverpath: ~/dtn/sw/adios2/bin/adios2_remote_server -background -report_port_selection -v -v -l /ccs/home/user007/dtn/log.adios2_remote_server -t 16
            verbose: 1

    NERSC:
        dtn-ssh:
            protocol: ssh
            host: dtn.nersc.gov
            user: user007
            authentication: publickey
            identity_file: ~/.ssh/nersc
            serverpath: ~/adios/master/dtn/bin/adios2_remote_server -background -report_port_selection -v -v -l /global/homes/u/user007/dtn/log.adios2_remote_server -t 16
            verbose: 1

    UEDGE.Spin:
        spin:
            protocol: xrootd
            transfer_protocol: https  # http, https or xrootd (default is xrootd)
            host: adios2-xrootd-ingress.adios2-remote.development.svc.spin.uedge.org
            port: 443
            verbose: 0

    Robinia:
        s3-transport:
            protocol: s3
            profile: robinia
            endpoint: https://projects.handsey.org.au
            AWS_EC2_METADATA: false
            recheck_metadata: false
            verbose: 0
