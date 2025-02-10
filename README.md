# hpc-campaign
HPC Campaign manager with remote data access. 
# hpc_campaign_manager
Script to create/update/delete a campaign archive.

This is the code to create a campaign archive of small files that can be shared with users and which contains references to the actual data in large files. 

Example 1: add existing files on a resource 
`hpc_campaign_manager create myproject/mycampaign_001.aca  -f file1.bp restart/restart.bp`

Example 2: create a campaign file pointing to an S3 bucket
`hpc_campaign_manager create mys3campaigns/shot001.aca --hostname SERVERNAME --s3_bucket /example-bucket --s3_datetime "2024-10-22 10:20:15 -0400" -f file1.bp file2.bp file3.bp`

Note: use `SERVERNAME` in the host configuration file to specify on a local resource to how to connect to it

# hpc_campaign_connector
SSH tunnel and port forwarding using paramiko.

This is a service that needs to started on the local machine, so that ADIOS can ask for connections to remote hosts, as specified in the remote host configuration. 

Example: `python3 ./hpc_campaign_connector.py -c ~/.config/adios2/hosts.yaml -p  30000`

Note that ADIOS currently looks for this service on fixed port 30000
