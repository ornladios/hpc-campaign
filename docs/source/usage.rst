Usage
=====


Creating a Campaign archive file
--------------------------------

The `hpc_campaign manager` command is the primary tool for creating, modifying, and viewing campaign archive files (`.aca`). It enables full lifecycle management for datasets, replicas, and archival storage locations.

A campaign archive name without the required `.aca` extension will be automatically corrected. Relative paths for archive names are resolved using the `campaignstorepath` defined in `~/.config/hpc-campaign/config.yaml` unless otherwise specified. Multiple commands can be chained in a single execution.

.. note::

  Updates to moving data for other location is not supported yet


**Global Usage and Options**

The manager command is invoked using the following general format:

.. code-block:: bash

  usage: hpc_campaign manager <archive> [command] [options]

The [command] argument must be one of the following: create | delete | info | dataset | text | image | add-archival-storage | archived | time-series. In addition, the following options are available globally for the manager command to overwrite the default options:

* `--campaign_store, -s <CAMPAIGN_STORE>` specifies the path to the local campaign store used by the campaign manager instead of the default path set in `~/.config/hpc-campaign/config.yaml`
* `--hostname, -n <HOSTNAME>` provides the host name, which must be unique for hosts within a campaign used by the campaign manager instead of the default hostname set in `~/.config/hpc-campaign/config.yaml`
* `--keyfile, -k <KEYFILE>` specifies the key file used to encrypt metadata.
* `--s3_bucket <S3_BUCKET>` specifies the target bucket on an S3 server for remote operations.
* `--s3_datetime <S3_DATETIME>` specifies the datetime of data on the S3 server, expected in the format: ``'YYYY-MM-DD HH:MM:SS -HHMM'`` (e.g., ``'2024-04-19 10:20:15 -0400'``).


**Commands**

The [command] argument can take one of the following values: create | delete | info | dataset | text | image | add-archival-storage | archived | time-series.

**1. create**

Creates a new campaign archive file stored in the specified or default path to the local campaign store folder. Example usage:

.. code-block:: bash

  hpc_campaign manager test_campaign_001 create


**2. delete**

Delete specific items (datasets or replicas) from a campaign archive file.
Example usage:

.. code-block:: bash

  hpc_campaign manager test_campaign_001 delete [options]

The optional options specifies what will be deleted:

* `--uuid <id> [<id> ...]` removes datasets by their universally unique identifier (UUID).
* `--name <str> [<str> ...]` removes datasets by their representation name.
* `--replica <id> [<id> ...]` removes replicas by their ID number.
* `--campaign` deletes the entire campaign file.


**3. info**

Prints the content and metadata of a campaign archive file.
Example usage:

.. code-block:: bash

  hpc_campaign manager test_campaign_001 info [options]

The optional options allow listing replicas, entries that have been deleted and checksums. A complete list of options can be found in the help menu (`-h` option).

**4. dataset**

Adds one or more datasets to the archive with datasets being valid HDF5 or ADIOS2 BP files.

.. note::

  A temporary file is created from HDF5 files during processing, so write access to the ``/tmp`` directory is required.


Example usage:

.. code-block:: bash

  hpc_campaign manager test_campaign_001 dataset run_001.bp run_002.h5


Additional option (`--name <NAME>`) can specify the representation name for the dataset in the campaign hierarchy. The same option can be applied to the text and image commands.


**5. text/image**

Add one or more text files or image files to the archive. Text files are always stored compressed directly within the archive.  By default, only a remote reference is stored for image files.

.. note::

  Since text is stored internally, be mindful of the resulting archive's size when adding large text files.

Example usage:

.. code-block:: bash

  hpc_campaign manager test_campaign_001 text input.json
  hpc_campaign manager test_campaign_001 image 2dslice.jpg


Additional options for images include:
* `--name, -n <NAME>` allows multiple files with different resolutions can share the same name.
* `--store, -s` stores the image file directly in the campaign archive instead of just a reference.
* `--thumbnail <X> <Y>` stores a resized image with an X-by-Y resolution as a thumbnail, while referring to the original.

**6. add-archival-storage**

Records an archival storage location (e.g., tape system) to the list of known storage locations for the campaign.

**7. archived**

Indicates that a dataset or replica has been copied or moved to an archival storage location. A new replica entry is created pointing to the archival host/directory.

**8. time-series**

Organizes a sequence of datasets into a single named time-series. Subsequent calls with the same name will add datasets to the list, unless --replace is used.

**Example creating an archive campaign file**

In this example we will create an archive campaign file with:
- the text json input file for a simulation
- the data generated by the simulation code
- analysis data generated by a code that reads the simulation data and produces histograms
- the images generated by a visualization code on the simulation data

Configuration:
- the `campaignpath` in `~/.config/hpc-campaign/config.yaml` is set to `/path/to/adios-campaign-store/demoproject`
- the runs are made on a machine named OLCF in the Campaign hostname in `~/.config/hpc-campaign/config.yaml`
- all the files above are generated and stored in `${pwd}/runs`

.. code-block:: bash

  $ hpc_campaign manager demoproject/test_campaign_001 delete --campaign
  $ hpc_campaign manager demoproject/test_campaign_001 create
  $ hpc_campaign manager demoproject/test_campaign_001 text runs/input-configuration.json
  $ hpc_campaign manager demoproject/test_campaign_001 dataset runs/simulation-output.bp runs/simulation-chekpoint.bp
  $ hpc_campaign manager demoproject/test_campaign_001 dataset analysis/pdf.bp
  $ hpc_campaign manager demoproject/test_campaign_001 image analysis/plot-2d.json --store

  $ hpc_campaign manager demoproject/test_campaign_001 info
  =========================================================
  ADIOS Campaign Archive, version 0.5, created on Oct 18 14:29

  Hosts and directories:
    OLCF   longhostname = frontier05341.frontier.olcf.ornl.gov
      1. /path/to/simulation

  Other Datasets:
      3a4bf0b14cc33424a470862bd67ed007  TEXT   Oct 18 14:25   runs/input-configuration.json
      0fce4b1173f432f7ae5d2282df9077a6  ADIOS  Oct 18 14:25   runs/simulation-output.bp
      aa5d2282df9077a60fc643f5ab53b351  ADIOS  Oct 18 14:26   runs/simulation-chekpoint.bp
      b42d0da4a0793adca341ace1ff6e628d  ADIOS  Oct 18 14:28   analysis/pdf.bp
      85a0b724b22f37a4a79ad8a0cf1127d1  IMAGE  Oct 18 14:24   analysis/plot-2d.json


Comparing the campaign archive size to the data it points to can be done by the default method on each operating system.

.. code-block:: bash

  $ du -sh runs/*bp
  263M    simulation-chekpoint.bp
  3.8G    simulation-output.bp

  $ du -sh /path/to/adios-campaign-store/demoproject/test_campaign_001 info.aca
  127K     /path/to/adios-campaign-store/demoproject/test_campaign_001 info.aca


Launch local connection server
------------------------------

to be continued...

