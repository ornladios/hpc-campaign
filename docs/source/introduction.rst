Introduction
============

**HPC Campaign Management** is a set of Python scripts for creating small metadata files about large datasets
in one or more locations, which can be shared among project users, and which refer back to the real data. 

A Campaign Archive file can contain

- metadata of ADIOS2 BP5 files 
- metadata of HDF5 files 
- images (stored inside the campaign archive, or reference to a remote image)
- text files (stored compressed in campaign archive, or reference to a remote text file)

Each dataset can have multiple replicas, in multiple host/directory locations. Datasets are assigned a unique identifier at first insert and then all history of replicas can be tracked by the unqiue identifier. 

Campaign management requires an I/O solution to support 

- extracting metadata from supported self-describing datasets
- handling the metadata file (.ACA) as a supported file format
- understand that the data is remote and support remote data access for actual data operations.

Currently, this toolkit is being developed for the `ADIOS I/O framework <https://adios2.readthedocs.io/>`_, however, the intention is to make this toolkit extendible for other file formats and I/O libraries. 

.. note::
    The following terms are used in this document interchangibly: `campaign archive`, `<archive>`, or an `ACA file`. ACA is the format of the campaign archive file and it stands for **A Campaign Archive**. Note that `archival storage` refers to a storage location, while `archive` is the campaign archive file. 

.. warning::

    Campaign Management is fairly new. It will change substantially in the future and campaign files produced by this version will have to be updated to newer versions. Make sure to use a compatible versions of ADIOS2 and hpc-campaign.

The idea
--------

Applications produce one or more output files in a single simulation/experiment run. Multiple restarts may create more output files or update (append to) existing files. Subsequent analysis and visualization runs produce more output files. Campaign is a data organization concept one step higher than a file. A campaign archive includes information about multiple files, including single-value variable's values and the min/max of arrays, the location of the data files (host and directory information), thumbnails of images, or the images and text files themselves. A science project can agree on how to organize their campaigns, i.e., how to name them, what files to include in a single campaign archive, how to distribute them, how to name the hosts where the actual data resides, etc.

Campaign archive files should be distributed among project members. Everyone has them locally on their own computer and use ADIOS2 to read them as normal, local, files. 

Concepts and names
------------------

* **Campaign archive**. A single file which holds all metadata about all kinds of data. Used locally but it can point to datasets on remote locations.
* **Host**. A unique name, that identifies a remote location. This name is used as reference and every user can configure their own access method to this host on their own local system. 
* **Directory**. A base directory on a host where the data items are located. Data items still have a relative path under this directory. It's up to the creator to decide where to split the full path into directory/dataset. TAR files should be created from these directories for enabling automatic replication.  
* **Dataset**. A data object with a unique identifier, with the following supported types:

    * **Data**. Self-describing data that has metadata part that can be included in the campaign archive itself. A dataset has a representation name, as they appear in the campaign hierarchy, which can be different from the path/name on the disk. Currently ADIOS2 and HDF5 files are supported but the list of supported formats will grow in the future.
    * **Image**. An image file, either embedded in the campaign archive, or just a reference to a remote image, or the latter with an embedded thumbnail image in the campaign archive. 
    * **Text**. This is the "blob" for campaign management. Anything else can be inserted as "text" which has no other metadata. It can be embedded or just refer to a remote object. ADIOS2 reader will present this as a char[] array in the hierarchy. 

* **Replica**. A dataset can be in multiple locations (multiple hosts, directories, and on archival storages). The replicas are identical except for images, where different resolutions can be bundled into a single item in the campaign archive.
* **Archival storage**. This is a location where we don't have the ability to execute commands and therefore we have to create a replica of a dataset already placed into the campaign archive. E.g. HPSS/Kronos tape systems, https servers, S3 servers. We may still be able to read out data from there.
* **TAR files**. Tape systems especially, require tarring up lots of files into giant archive files. Many datasets can be included in a TAR file. An index can be created from the TAR file and then every replicas that match an entry will get another replica in the campaign archive, pointing to the TAR file on the archival storage location.
* **Keys**. Metadata, embedded text and images, can be encrypted with key files or password-protected key files. Only those who have the key locally (and know the password) are able to see these pieces of data. Others only can list and process the unencrypted items. 



What is NOT part of this campaign management toolkit?
-----------------------------------------------------

- Keeping campaign archive files up to date. The project has to design its protocol to make sure to update the campaign archive file every time there is a change to the data (updates, removals, archivals, move). If the archive becomes outdated, one cannot reach the data anymore. 
- Distribution of campaign archive files. Campaign archive files need to be shareable, transferred and distributed to multiple locations. A cloud file sharing works best to keep the copies in sync and maintain a way to propagate changes from the maintainer of the campaign archive to all the copies. `Rclone is a great command-line tool <https://rclone.org>`_ to sync the campaign store with many cloud-based file sharing services and cloud instances.
- Strict adhering to FAIR principles. Strongly encouraged to create project-related text documents that describe rich metadata and include that in every campaign archive.
- Reading data. This is currently provided by the ADIOS2 I/O framework. 

