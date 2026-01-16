#!/bin/bash
#
# run this in the hpc-campaign directory, so that data/heat.bp is a valid local path
# the output file will  be in the hpc-campaign directory, named example_cli.aca

# aca file with full path
ACA=$PWD/example_cli.aca

# delete the aca (in case it exists) to start from scratch
hpc_campaign manager $ACA delete --campaign

# create the campaign file. 
hpc_campaign manager $ACA create 

# add a dataset with a representation name
hpc_campaign manager $ACA dataset data/heat.bp  --name heat

# add an image with a representation name
hpc_campaign manager $ACA image data/T00000.png  --name T0
hpc_campaign manager $ACA image data/T00001.png  --name T1 --store
hpc_campaign manager $ACA image data/T00002.png  --name T2 --thumbnail 64 64

# add a text file
hpc_campaign manager $ACA text data/readme  --name readme --store

# print info about the aca
hpc_campaign manager $ACA info -rfdc

# add an archival storage to the aca
hpc_campaign manager $ACA add-archival-storage fs faketape $PWD/data/archive 

# determine the dirid of the newly inserted archival storage in three steps
hpc_campaign manager $ACA info -rfdc 2>&1 | tee log.1 
archivedirID=`grep -A1 faketape log.1 | tail -1 | sed -e "s/^ *//" -e "s/\..*$//"`
rm -rf log.1

# add a replica of the heat.bp located in the archival location
hpc_campaign manager $ACA archived heat ${archivedirID} --newpath archivedheat.bp 

# info
hpc_campaign manager $ACA info -rfdc 

# delete the aca
# hpc_campaign manager $ACA delete --campaign

