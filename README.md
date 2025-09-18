[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Documentation](https://readthedocs.org/projects/hpc-campaign/badge/?version=latest)](https://hpc-campaign.readthedocs.io/en/latest/?badge=latest)

## Documentation

Documentation is hosted at [readthedocs](https://hpc-campaign.readthedocs.io).
See [Installation](https://hpc-campaign.readthedocs.io/en/latest/installation.html) for installation instructions. It can be installed by `pip3 install hpc-campaign` but needs to be configured before first use.

# hpc-campaign
HPC Campaign Management is a set of Python scripts for creating small metadata files about large datasets in one or more locations, which can be shared among project users, and which refer back to the real data. 

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


# hpc_campaign manager
Script to create/update/delete a campaign archive.

This is the code to create a campaign archive of small files that can be shared with users and which contains references to the actual data in large files. 

Example 1: add existing files on a resource 
```
hpc_campaign manager myproject/mycampaign_001.aca create
hpc_campaign manager myproject/mycampaign_001.aca dataset file1.bp restart/restart.bp`
```

Example 2: add existing files on a resource and encrypt them with a keyfile
`hpc_campaign manager myproject/mycampaign_001.aca -k keyfile dataset file1.bp restart/restart.bp` 

Example 2: create a new campaign file and add datasets pointing to an S3 bucket
`hpc_campaign manager mys3campaigns/shot001.aca create dataset --hostname SERVERNAME --s3_bucket /example-bucket --s3_datetime "2024-10-22 10:20:15 -0400" file1.bp file2.bp file3.bp`

Note: use `SERVERNAME` in the host configuration file to specify on a local resource to how to connect to it

# hpc_campaign genkey
Script to generate a key file used to encrypt files added into a campaign archive.

Anyone with the keyfile can process a campaign archive but without it one can only list the content of the archive itself (list of files, host, folders and creation times).

The encoding key in the file is stored as plain text by default. Optionally, a password can be used to encrypt the key itself. Only those who has the keyfile and know the password, can process the campaign archive. 

Example 1: Generate a keyfile. 
`hpc_campaign genkey generate keyfile`

Example 2: Generate a password-encrypted keyfile 
`hpc_campaign genkey generate -p keyfile`

Example 3: Verify a password encrypted keyfile. Asks for the password and decrypts the key in memory to verify the key/password combo is valid. 
`hpc_campaign genkey verify keyfile`

Example 4: Print keyfile information. It does not ask for password.
`hpc_campaign genkey info keyfile`

# hpc_campaign connector
SSH tunnel and port forwarding using paramiko.

This is a service that needs to be started on the local machine, so that ADIOS can ask for connections to remote hosts, as specified in the remote host configuration. Additionally, this scripts loads the available keyfiles, and serves to ADIOS on demand. 

Example: `hpc_campaign connector -c ~/.config/hpc-campaign/hosts.yaml -p  30000`

Note that ADIOS currently looks for this service on fixed port 30000
