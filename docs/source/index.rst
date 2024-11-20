Welcome to HPC Campaign Management documentation!
=================================================

**HPC Campaign Management** is a set of Python scripts for creating small metadata files about large datasets
in one or more locations, which can be shared among project users, and which refer back to the real data. 

It requires an I/O solution to support 
- extracting metadata from datasets
- packing up the metadata
- handling the metadata file (.ACA) as a supported file format
- understand that the data is remote and support remote data access for actual data operations.

Currently, this toolkit is being developed for the `ADIOS I/O framework <https://adios2.readthedocs.io/>`_, however, the the intention is to make this toolkit extendible for other file formats. 

Check out the :doc:`usage` section for further information, including
how to :ref:`installation` the project.

.. note::

   This project is under active development.

Contents
--------

.. toctree::

   usage
