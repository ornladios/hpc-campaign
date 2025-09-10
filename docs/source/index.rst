Welcome to HPC Campaign Management documentation!
=================================================

**HPC Campaign Management** is a set of Python scripts for creating small metadata files about large datasets
in one or more locations, which can be shared among project users, and which refer back to the real data. 

A Campaign Archive file can contain

- metadata of ADIOS2 BP5 datasets 
- metadata of HDF5 files 
- images (stored inside the campaign archive, or reference to a remote image)
- text files (stored compressed in campaign archive, or reference to a remote text file)

Each dataset can have multiple replicas, in multiple host/directory locations. Datasets are assigned a unique identifier at first insert and then all history of replicas can be tracked by the unqiue identifier. Special locations can be designated as *archives*, meaning they are not directly accessible but first have to be restored to some location. 

Campaign management requires an I/O solution to support 

- extracting metadata from datasets
- handling the metadata file (.ACA) as a supported file format
- understand that the data is remote and support remote data access for actual data operations.

Currently, this toolkit is being developed for the `ADIOS I/O framework <https://adios2.readthedocs.io/>`_, however, the intention is to make this toolkit extendible for other file formats and I/O libraries. 

.. warning::

    Campaign Management is fairly new. It will change substantially in the future and campaign files produced by this version will have to be updated to newer versions. Make sure to use a compatible versions of ADIOS2 and hpc-campaign.

The idea
--------

Applications produce one or more output files in a single simulation/experiment run. Multiple restarts may create more output files or update (append to) existing files. Subsequent analysis and visualization runs produce more output files. Campaign is a data organization concept one step higher than a file. A campaign archive includes information about multiple files, including single-value variable's values and the min/max of arrays, the location of the data files (host and directory information), thumbnails of images, or the images and text files themselves. A science project can agree on how to organize their campaigns, i.e., how to name them, what files to include in a single campaign archive, how to distribute them, how to name the hosts where the actual data resides, etc.

What is NOT part of this campaign management toolkit?
-----------------------------------------------------

- Keeping campaign archive files up to date. The project has to design its protocol to make sure to update the campaign archive file every time there is a change to the data (updates, removals, archivals, move). If the archive becomes outdated, one cannot reach the data anymore. 
- Distribution of campaign archive files. Campaign archive files need to be shareable, transferred and distributed to multiple locations. A cloud file sharing works best to keep the copies in sync and maintain a way to propagate changes from the maintainer of the campaign archive to all the copies. `Rclone is a great command-line tool <https://rclone.org>`_ to sync the campaign store with many cloud-based file sharing services and cloud instances.
- Strict adhering to FAIR principles. Strongly encouraged to create project-related text documents that describe rich metadata and include that in every campaign archive.
- Reading data. This is currently provided by the ADIOS2 I/O framework. 



.. Check out the :doc:`usage` section for further information, including
   how to :ref:`installation` the project.

Contents
--------

.. toctree::
   :caption: Introduction

   installation
   usage
