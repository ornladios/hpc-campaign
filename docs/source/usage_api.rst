Usage in Python
===============

.. code-block:: python

  from hpc_campaign.manager import Manager
  from hpc_campaign.ls import ls
  from hpc_campaign.rm import rm

`Manager` is the class for creating, modifying, and viewing campaign archive files (`.aca`). It enables full lifecycle management for datasets, replicas, and archival storage locations.

A campaign archive name without the required `.aca` extension will be automatically corrected. Relative paths for archive names are resolved using the `campaignstorepath` defined in `~/.config/hpc-campaign/config.yaml` unless directly specified when creating a Manager object. 


**Global Usage and Options**

The manager command is invoked using the following general format:

.. code-block:: python

  manager = Manager("demoproject/test_campaign_001")
  manager.open(create=True)
  ...
  manager.close()


.. code-block:: python

  manager = Manager(archive=str(acafile), campaign_store=str(campaign_store))


The following options are available globally for the manager command to overwrite the default options:

* `campaign_store` specifies the path to the local campaign store used by the campaign manager instead of the default path set in `~/.config/hpc-campaign/config.yaml`
* `hostname` provides the host name, which must be unique for hosts within a campaign used by the campaign manager instead of the default hostname set in `~/.config/hpc-campaign/config.yaml`
* `keyfile` specifies the key file used to encrypt metadata in all subsequent dataset/image/text operation


**Manager sub-commands**

The [sub-command] argument can take one of the following values

* **add_archival_storage** Register an archival location (tape system, https, s3)
* **add_dataset** Add ADIOS2 or HDF5 files
* **add_image** Add images, embedded or remote optionally with an embedded thumbnail image
* **add_text** Add text files, embedded or just reference to remote file
* **add_time_series** Organizing a series of individual datasets as a single entry with extra dimension for time
* **archived_replica** Create a replica of a dataset pointing to an archival storage location
* **close** Close the campaign archive after all operations.
* **delete_name** Delete dataset/image/text by name (all replicas)
* **delete_replica** Delete a replica of dataset/image/text
* **delete_time_series** Delete a time-series definition (but not the datasets themselves)
* **delete_uuid** Delete dataset/image/text by UUID (all replicas)
* **info** List the content of a campaign archive
* **open** Open/create a campaign archive, optionally wipe all content to start afresh
* **upgrade** For upgrading an older ACA format to newer format


**1. add-archival-storage**

Records an archival storage location (e.g., tape system) to the list of known storage locations for the campaign. HTTPS and S3 server locations are considered to be archival locations for campaign management, since we cannot add datasets directly on these servers. 

The following `system` types are allowed and understood by ADIOS2 readers: 
* `https` for a file/tar accessible on a public HTTPS server
* `s3` for a file/tar accessible with the S3 protocol
* `fs` for a file/tar on a readable file system location
* `hpss` for the HPSS tape archive system - it cannot be read by ADIOS2
* `kronos` for the Kronos disk/tape archive system - typically readable by ADIOS2

Other `system` types that are allowed but for now not understood by ADIOS2 readers
* `http`
* `ftp`

.. code-block:: python

  manager.add_archival_storage(system = "fs", host="faketape", directory="/mnt/d/archive")

  host_id, dir_id, archive_id = manager.add_archival_storage(
    system = "https", 
    host="USERS.NCCS", 
    directory="~pnorbert/campaign-test",
    tarfilename="gray-scott-ensemble.tar"
    tarfileidx="gray-scott-ensemble.tar.idx"
    longhostname="users.nccs.gov",
    note="This is an example TAR file stored on an HTTPS server"
  )

Replicas of datasets then can be created (in the campaign file) by the `archived_replica` function. Note that hpc-campaign does not copy/move files on disk, someone else has to do that. These commands only record the action into the campaign archive file. 

If we put a TAR file there instead of individual files, we can just point to that, and use `archived_replica` one by one for each dataset. However, it is easier and provides more information, if we create an index file with the `hpc_campaign taridx` CLI command, then let the manager to create (i.e. record) a new replica for every dataset/replica already in the campaign archive that is also in the tar index. Moreover, if this index is available, ADIOS2 can read BP files, images and text directly from the tar file without the need of extracting them first (on file systems and https servers, that is).

**Return values**: `host_id`, `directory_id`, `archive_id`, use the directory_id and archive_id in follow-up `archived_replica` calls to identify this archival storage. Technically, `archive_id` is only needed if one host has multiple archive-storages under the same directory (e.g. individual files plus a TAR file in the same location for storing two replicas).

**2. add_dataset**

Adds one or more datasets to the archive with datasets being valid HDF5 or ADIOS2 BP files.

.. note::

  A temporary file is created from HDF5 files during processing, so write access to the ``/tmp`` directory is required.


Example usage:

.. code-block:: python

  manager.add_dataset("data/heat.bp", name="heat")
  manager.add_dataset(["data/run_001.h5", "data/run_002.h5", "data/run_003.h5"])

Additional option (`name="<NAME>"`) can specify the representation name for one dataset in the campaign hierarchy. The same option can be applied to the text and image sub-commands.

**3. add_image**

Add an image file to the archive. By default, only a remote reference is stored for image files but it can be stored (`store=True``) or a thumbnail with a smaller resolution can be stored (`thumbnail=[64, 64]`)

Example usage:

.. code-block:: python

    manager.add_image("data/T0.png", name="T0")
    manager.add_image("data/T1.png", name="T1", store=True)
    manager.add_image("data/T2.png", name="T2", thumbnail=[64, 64])

Additional options for images include:
* `name="<NAME>"` representation name for the image in the campaign hierarchy

**4. add_text**

Add one or more text files to the archive. If requested, text files are stored within the archive. In that case, zlib is used to compress the text file.

.. note::

  When storing text internally, be mindful of the resulting archive's size when adding large text files.

Example usage:

.. code-block:: python

  manager.add_text("input.json", name="input", store=True)
  file = Path("data/readme")
  manager.add_text(file, name="readme", store=True)


Additional options for text include:
* `name="<NAME>"` representation name for the image in the campaign hierarchy
* `store=True` stores the text file directly in the campaign archive instead of just a reference.

**5. add_time_series**

Organizes a sequence of datasets into a single named time-series. Subsequent calls with the same name will add datasets to the list, unless `replace=True` is used.

.. code-block:: python

  manager = Manager("test.aca")
  manager.open(create=True)
  manager.add_dataset("series/array00.bp", name="array00")
  manager.add_dataset("series/array01.bp", name="array01")
  manager.add_dataset("series/array02.bp", name="array02")
  manager.add_time_series("array", ["array00", "array01", "array02"])

  manager.add_dataset("series/array03.bp", name="array03")
  manager.add_time_series("array", ["array03"])
  manager.close()
  

.. code-block:: python

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

* `replace=True` redefine the time-series starting with the current command


**6. archived_replica**

Indicates that a (replica of a) dataset has been manually copied or moved to an archival storage location. A new replica entry is created pointing to the archival host/directory. This sub-command only works if the metadata of the dataset is still included in the ACA file, so that it can be copied for the new replica. Therefore, always execute this sub-command before deleting the original replica from the ACA file. The two operations can be combined using the `move=True` option. This sub-command requires the use of `add_archival_storage` sub-command that adds the location (host/directory/tar file) to the campaign first. If many files are added to the archival location in a TAR file, it is better to use the `taridx` command to create an index of the tar file and then use that in the `add_archival_storage` operation to automatically create replicas of all datasets involved. However, this individual sub-command allows for placing the replica in a different relative path string than the original, while the tar indexing requires them to be placed exactly with the same relative paths. 

.. code-block:: python

    _host_id, dir_id, archive_id = manager.add_archival_storage("fs", "faketape", "/mnt/d/archive")
    manager.archived_replica("heat", dir_id, archiveid=archive_id, newpath="archivedheat.bp")


Additional options for text include:

* `archiveid:int` redefine the time-series starting with the current command
* `newpath` use this option if the replica's relative path under the archival directory is different from the relative path of the original replica under its own directory, 
* `replica:int` if a dataset has multiple replicas already, you must tell which one has been actually replicated for this record
* `move=True` indicate that the replica has been moved to new location, not copied. The original replica will be removed from the campaign archive. 


**7. close** 

Close the campaign archive after all operations. It is only for freeing up resources. Since all operations commit their changes, there is nothing left for close(). 


**8. delete_name**

Delete specific item (dataset, image, text) from a campaign archive file referring to their representation name. All replicas are deleted along with all embedded files. 
Example usage:

.. code-block:: python

  manager.delete_name("array00")

**9. delete_replica**

Delete specific replica of an item (dataset, image, text) from a campaign archive file. Embedded files are deleted only if it is the only replica referring to a file.
Example usage:

.. code-block:: python

  manager.delete_replica(12)

**10. delete_time_series**

Delete specific replica of an item (dataset, image, text) from a campaign archive file. Embedded files are deleted only if it is the only replica referring to a file.
Example usage:

.. code-block:: python

  manager.delete_time_series("array")


**11. delete_uuid**

Delete specific item (dataset, image, text) from a campaign archive file. All replicas are deleted along with all embedded files. 
Example usage:

.. code-block:: python

  manager.delete_uuid("180b6d3123a832d786e1d0ff99c7e303")

**12. info**

Prints the content and metadata of a campaign archive file.
Example usage:

.. code-block:: bash

  hpc_campaign manager demoproject/test_campaign_001 info [options]

The optional options allow listing replicas, entries that have been deleted and checksums. A complete list of options can be found in the help menu (`-h` option).


**13. open**

Open is the first function to be called after the manager object is created. It opens the campaign archive file (opens the database connection). By default, it will raise a FileNotFound error if the campaign archive does not exists. The `create=True` option will result in creating the campaign archive if it does not exists. 
The `truncate=True` option will wipe the content of an existing campaign archive to start afresh. 

.. code-block:: python

    manager = Manager(archive=str(api_archive), campaign_store=str(campaign_store))
    manager.open(create=True, truncate=True)

Additional options for opem include:
* `create=True` to create a new campaign archive if it does not exist already
* `truncate=True` if the campaign archive already exist, open and wipe its content


**14. upgrade**

An ADIOS2 release will only read the latest ACA version and throw errors if an older ACA files is opened. The `upgrade` sub-command will modify the old ACA to jump to the next version. It may be called multiple times to get to the current version. This is an in-place conversion. If an error occurs during conversion, all changes are cancelled, leaving the original file intact. 

.. code-block:: python

    manager = Manager(archive=str(api_archive), campaign_store=str(campaign_store))
    manager.open(create=True, truncate=True)
    new_version = manager.upgrade()
    print(new_version)


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

.. code-block:: python

    manager = Manager(archive="demoproject/test_campaign_001")
    manager.open(create=True, truncate=True)
    manager.add_text("runs/input-configuration.json", store=True)
    manager.add_dataset(["runs/simulation-output.bp" ,"runs/simulation-chekpoint.bp"])
    manager.add_dataset("analysis/pdf.bp")
    manager.add_image("analysis/plot-2d.png", store=True)

    info_data = manager.info(True, False, False, False)
    output = format_info(info_data)
    print(output)
    manager.close()


.. code-block:: bash

  ADIOS Campaign Archive, version 0.7, created on Oct 18 14:29

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


