Usage
=====




Creating a Campaign archive file
--------------------------------

The `hpc_campaign manager` command is the primary tool for creating, modifying, and viewing campaign archive files (`.aca`). It enables full lifecycle management for datasets, replicas, and archival storage locations.

A campaign archive name without the required `.aca` extension will be automatically corrected. Relative paths for archive names are resolved using the `campaignstorepath` defined in `~/.config/hpc-campaign/config.yaml` unless otherwise specified. Multiple commands can be chained in a single execution.

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

**create**

Creates a new campaign archive file stored in the specified or default path to the local campaign store folder. Example usage:

.. code-block:: bash

hpc_campaign manager test_campaign_001 create


**delete**

Delete specific items (datasets or replicas) from a campaign archive file.
Example usage:

.. code-block:: bash

hpc_campaign manager test_campaign_001 delete [options]

The optional options specifies what will be deleted:

* `--uuid <id> [<id> ...]` removes datasets by their universally unique identifier (UUID).
* `--name <str> [<str> ...]` removes datasets by their representation name.
* `--replica <id> [<id> ...]` removes replicas by their ID number.
* `--campaign` deletes the entire campaign file.


**info**

Prints the content and metadata of a campaign archive file.
Example usage:

.. code-block:: bash

hpc_campaign manager test_campaign_001 info [options]

The optional options allow listing replicas, entries that have been deleted and checksums. A complete list of options can be found in the help menu (`-h` option).

**dataset**

Adds one or more datasets to the archive with datasets being valid HDF5 or ADIOS2 BP files.

.. warning::

A temporary file is created from HDF5 files during processing, so write access to the ``/tmp`` directory is required.


Example usage:

.. code-block:: bash

hpc_campaign manager test_campaign_001 dataset run_001.bp run_002.h5


Additional option (`--name <NAME>`) can specify the representation name for the dataset in the campaign hierarchy. The same option can be applied to the text and image commands.


**text** and **image**

Add one or more text files or image files to the archive. Text files are always stored compressed directly within the archive.  By default, only a remote reference is stored for image files.

.. note::

Since text is stored internally, be mindful of the resulting archive's size when adding large text files.


Usage:

.. code-block:: bash

hpc_campaign manager test_campaign_001 text input.json
hpc_campaign manager test_campaign_001 image 2dslice.jpg


Additional options for images include:
* `--name, -n <NAME>` allows multiple files with different resolutions can share the same name.
* `--store, -s` stores the image file directly in the campaign archive instead of just a reference.
* `--thumbnail <X> <Y>` stores a resized image with an X-by-Y resolution as a thumbnail, while referring to the original.

**add-archival-storage**

Records an archival storage location (e.g., tape system) to the list of known storage locations for the campaign.

**archived**

Indicates that a dataset or replica has been copied or moved to an archival storage location. A new replica entry is created pointing to the archival host/directory.

**time-series**

Organizes a sequence of datasets into a single named time-series. Subsequent calls with the same name will add datasets to the list, unless --replace is used.


Launch local connection server
------------------------------

to be continued...

