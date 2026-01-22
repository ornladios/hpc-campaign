Usage on command-line 
=====================

The Python command `hpc_campaign` is the entry point to all commands:

* **connector** Launches a service that can make SSH tunnels on demand to remote hosts
* **genkey**  Generates/validates keys used for encrypting datasets in the campaign archive
* **ls**  Lists the available campaign archives, all or those that matches an expression
* **rm**  Remove campaign archives, those that matches an expression
* **manager** The main command with many sub-commands to create/delete/update a campaign archive
* **taridx**  Creates an index from a TAR file that can be used to point to replicas on an archival storage
* **cache** List/clear the content of the local cache

Creating a Campaign archive file
--------------------------------

The `hpc_campaign manager` command is the primary tool for creating, modifying, and viewing campaign archive files (`.aca`). It enables full lifecycle management for datasets, replicas, and archival storage locations.

A campaign archive name without the required `.aca` extension will be automatically corrected. Relative paths for archive names are resolved using the `campaignstorepath` defined in `~/.config/hpc-campaign/config.yaml` unless otherwise specified. Multiple sub-commands can be chained in a single execution.


**Global Usage and Options**

The manager command is invoked using the following general format:

.. code-block:: bash

  usage: hpc_campaign manager <archive> [sub-command] [options]

The following options are available globally for the manager command to overwrite the default options:

* `\-\-campaign_store, -s <CAMPAIGN_STORE>` specifies the path to the local campaign store used by the campaign manager instead of the default path set in `~/.config/hpc-campaign/config.yaml`
* `\-\-hostname, -n <HOSTNAME>` provides the host name, which must be unique for hosts within a campaign used by the campaign manager instead of the default hostname set in `~/.config/hpc-campaign/config.yaml`
* `\-\-keyfile, -k <KEYFILE>` specifies the key file used to encrypt metadata.


..
  * `\-\-s3_bucket <S3_BUCKET>` specifies the target bucket on an S3 server for remote operations.
  * `\-\-s3_datetime <S3_DATETIME>` specifies the datetime of data on the S3 server, expected in the format: ``'YYYY-MM-DD HH:MM:SS -HHMM'`` (e.g., ``'2024-04-19 10:20:15 -0400'``).


**Manager sub-commands**

The [sub-command] argument can take one of the following values

* **add-archival-storage** Register an archival location (tape system, https, s3)
* **archived-replica** Create a replica of a dataset pointing to an archival storage location
* **dataset** Add ADIOS2 or HDF5 files
* **delete** Delete dataset/image/text, one or all replicas from a campaign archive
* **image** Add images, embedded or remote optionally with an embedded thumbnail image
* **info** List the content of a campaign archive
* **text** Add text files, embedded or just reference to remote file
* **time-series** Organizing a series of individual datasets as a single entry with extra dimension for time
* **upgrade** For upgrading an older ACA format to newer format


**1. add-archival-storage**

Records an archival storage location (e.g., tape system) to the list of known storage locations for the campaign.

.. code-block:: bash

  hpc_campaign manager demoproject/test_campaign_001 add-archival-storage \
    --longhostname users.nccs.gov https USERS.NCCS ~pnorbert/campaign-test/gray-scott-ensemble

This adds a second host/directory location into the campaign archive:

.. code-block:: bash

  USERS.NCCS   longhostname = users.nccs.gov
     2. ~pnorbert/campaign-test/gray-scott-ensemble  - Archive: https

Replicas of datasets then can be created (in the campaign file) by the `archived-replica` sub-command. Note that hpc-campaign does not copy/move files on disk, someone else has to do that. These commands only record the action into the campaign archive file. 

If we put a TAR file there instead of individual files, we can just point to that, and use `archived-replica` one by one for each dataset. However, it is easier and provides more information, if we create an index file with the `taridx` command, then let the manager to create (i.e. record) a new replica for every dataset/replica already in the campaign archive that is also in the tar index. Moreover, if this index is available, ADIOS2 can read BP files, images and text directly from the tar file without the need of extracting them first (on file systems and https servers, that is).

**2. archived-replica**

Indicates that a (replica of a) dataset has been manually copied or moved to an archival storage location. A new replica entry is created pointing to the archival host/directory. This sub-command only works if the metadata of the dataset is still included in the ACA file, so that it can be copied for the new replica. Therefore, always execute this sub-command before deleting the original replica from the ACA file. The two operations can be combined using the `\-\-move` option. This sub-command requires the use of `add-archival-storage` sub-command that adds the location (host/directory/tar file) to the campaign first. If many files are added to the archival location in a TAR file, it is better to use the `taridx` command to create an index of the tar file and then use that in the `add-archival-storage` operation to automatically create replicas of all datasets involved. However, this individual sub-command allows for placing the replica in a different relative path string than the original, while the tar indexing requires them to be placed exactly with the same relative paths. 

.. code-block:: bash

  hpc_campaign manager $ACA add-archival-storage fs faketape $PWD/data/archive 2>&1 | tee log.archive

  # determine the directory-id of the newly inserted archival storage from the output log
  faketape_dirID=`grep "Archive storage added" log.archive | sed -e "s/^.*directory id = //" -e "s/ *archive id.*$//"`
  echo "faketape_dirID from stdout:" ${faketape_dirID}

  # determine the directory-id of the newly inserted archival storage in three steps from running info
  hpc_campaign manager $ACA info -rfdc 2>&1 | tee log.1 
  faketape_dirID=`grep -A1 faketape log.1 | tail -1 | sed -e "s/^ *//" -e "s/\..*$//"`
  rm -rf log.1

  hpc_campaign manager $ACA archived-replica heat ${faketape_dirID} --newpath archivedheat.bp 

**3. dataset**

Adds one or more datasets to the archive with datasets being valid HDF5 or ADIOS2 BP files.

.. note::

  A temporary file is created from HDF5 files during processing, so write access to the ``/tmp`` directory is required.


Example usage:

.. code-block:: bash

  hpc_campaign manager demoproject/test_campaign_001 dataset run_001.bp run_002.h5


Additional option (`\-\-name <NAME>`) can specify the representation name for one dataset in the campaign hierarchy. The same option can be applied to the text and image sub-commands.

**4. delete**

Delete specific items (datasets, images, texts or selected replicas) from a campaign archive file.
Example usage:

.. code-block:: bash

  hpc_campaign manager demoproject/test_campaign_001 delete [options]

The optional options specifies what will be deleted:

* `\-\-uuid <id> [<id> ...]` removes datasets by their universally unique identifier (UUID).
* `\-\-name <str> [<str> ...]` removes datasets by their representation name.
* `\-\-replica <id> [<id> ...]` removes replicas by their ID number.

**5. image**

Add an image files to the archive. By default, only a remote reference is stored for image files but it can be stored (`\-\-store``) or a thumbnail with a smaller resolution can be stored. 

Example usage:

.. code-block:: bash

  hpc_campaign manager demoproject/test_campaign_001 image remote_image.png
  hpc_campaign manager demoproject/test_campaign_001 image stored_image.png --store
  hpc_campaign manager demoproject/test_campaign_001 image big_image.jpg --thumbnail 64 64


Additional options for images include:
* `\-\-name, -n <NAME>` representation name for one image in the campaign hierarchy
* `\-\-store, -s` stores the image file directly in the campaign archive instead of just a reference.
* `\-\-thumbnail <X> <Y>` stores a resized image with an X-by-Y resolution as a thumbnail, while referring to the original.

**6. info**

Prints the content and metadata of a campaign archive file.
Example usage:

.. code-block:: bash

  hpc_campaign manager demoproject/test_campaign_001 info [options]

The optional options allow listing replicas, entries that have been deleted and checksums. A complete list of options can be found in the help menu (`-h` option).


**7. text**

Add one or more text files to the archive. If requested, text files are stored within the archive. In that case, zlib is used to compress the text file.

.. note::

  When storing text internally, be mindful of the resulting archive's size when adding large text files.

Example usage:

.. code-block:: bash

  hpc_campaign manager demoproject/test_campaign_001 text input.json --store


Additional options for text include:
* `\-\-name, -n <NAME>` representation name for one text file in the campaign hierarchy
* `\-\-store, -s` stores the text file directly in the campaign archive instead of just a reference.

**8. time-series**

Organizes a sequence of datasets into a single named time-series. Subsequent calls with the same name will add datasets to the list, unless `\-\-replace` is used.

.. code-block:: bash

  hpc_campaign manager test.aca dataset series/array00.bp --name array00
  hpc_campaign manager test.aca dataset series/array01.bp --name array01
  hpc_campaign manager test.aca dataset series/array02.bp --name array02
  hpc_campaign manager test.aca dataset series/array03.bp --name array03
  hpc_campaign manager test.aca time-series array array00 array01 array02
  hpc_campaign manager test.aca time-series array array03
  hpc_campaign manager test.aca info

  ...
  Time-series and their datasets:
  array
    89635fe22f85314ebfc04c902bca42f3  ADIOS  Jun  9 08:49   array00
    cea0302ea4ce39ccabca6b40bbeb09d1  ADIOS  Jun  9 08:49   array01
    fad5daf925e13f938c2649d81a1821f2  ADIOS  Jun  9 08:49   array02
    180b6d3123a832d786e1d0ff99c7e303  ADIOS  Jun  9 08:49   array03

  Other Datasets:
  ...

  # ADIOS tools will present them as a single dataset with multiple steps
  bpls -l  test.aca array/*
    int64_t  array/Nx                          4*scalar = 10 / 10
    double   array/bpArray                     4*{10} = 0 / 9
    double   array/time                        4*scalar = 0 / 0


Additional options for text include:

* `\-\-replace` redefine the time-series starting with the current command
* `\-\-remove`  delete the time-series definition (but not the datasets)


**9. upgrade**

An ADIOS2 release will only read the latest ACA version and throw errors if an older ACA files is opened. The `upgrade` sub-command will modify the old ACA to jump to the next version. It may be called multiple times to get to the current version. This is an in-place conversion. If an error occurs during conversion, all changes are cancelled, leaving the original file intact. 

**Example creating an archive campaign file**

In this example we will create an archive campaign file with:

- the text json input file for a simulation
- the data generated by the simulation code
- analysis data generated by a code that reads the simulation data and produces histograms
- the images generated by a visualization code on the simulation data

Configuration:

- the `campaignpath` in `~/.config/hpc-campaign/config.yaml` is set to `/path/to/campaign-store`
- the path `/path/to/campaign-store/demoproject` is writable directory 
- the runs are made on a machine named OLCF in the Campaign hostname in `~/.config/hpc-campaign/config.yaml`
- all the files above are generated and stored in `${pwd}/runs`

.. note::

  In the first command, we use the `\-\-truncate` option to wipe all content in the aca file if it already exists. 

.. code-block:: bash

  $ hpc_campaign manager demoproject/test_campaign_001 --truncate text runs/input-configuration.json
  $ hpc_campaign manager demoproject/test_campaign_001 dataset runs/simulation-output.bp runs/simulation-chekpoint.bp
  $ hpc_campaign manager demoproject/test_campaign_001 dataset analysis/pdf.bp
  $ hpc_campaign manager demoproject/test_campaign_001 image analysis/plot-2d.png --store

  $ hpc_campaign manager demoproject/test_campaign_001 info
  =========================================================
  ADIOS Campaign Archive, version 0.7, created on Oct 18 14:29

  Hosts and directories:
    OLCF   longhostname = frontier05341.frontier.olcf.ornl.gov
      1. /path/to/simulation

  Other Datasets:
      3a4bf0b14cc33424a470862bd67ed007  TEXT   Oct 18 14:25   runs/input-configuration.json
      0fce4b1173f432f7ae5d2282df9077a6  ADIOS  Oct 18 14:25   runs/simulation-output.bp
      aa5d2282df9077a60fc643f5ab53b351  ADIOS  Oct 18 14:26   runs/simulation-chekpoint.bp
      b42d0da4a0793adca341ace1ff6e628d  ADIOS  Oct 18 14:28   analysis/pdf.bp
      85a0b724b22f37a4a79ad8a0cf1127d1  IMAGE  Oct 18 14:24   analysis/plot-2d.png


Comparing the campaign archive size to the data it points to can be done by the default method on each operating system.

.. code-block:: bash

  $ du -sh runs/*bp
  263M    simulation-chekpoint.bp
  3.8G    simulation-output.bp

  $ du -sh /path/to/adios-campaign-store/demoproject/test_campaign_001 info.aca
  127K     /path/to/adios-campaign-store/demoproject/test_campaign_001 info.aca


