Campaign index time series
==========================

An ACX keeps one row in ``variables`` per logical time-series variable. Its
``datasetid`` references the logical series row in ``datasets``; that row's
``archiveid`` identifies the source ACA. The logical dataset's ``dsid`` is the
negated ACA time-series ID. Positive ``dsid`` values retain their meaning as
physical ACA dataset IDs.

The variable summary contains the sum of its available steps across members,
the rank and shape at its first occurrence, and global min/max computed from
all available block statistics in all members and steps. Missing statistics
remain SQL NULL. Scalar values are concatenated in series order. Array payloads
are not read to compute bounds. Variable types must agree across members.
For changing shapes, the ACA/native reader remains responsible for per-step
shape discovery.

Membership and step mapping
---------------------------

Three additive tables describe time series:

* ``timeseries(tsid, archiveid, aca_tsid, datasetid, name)`` identifies a logical
  series. ``tsid`` is local to the ACX; ``aca_tsid`` is its ID in the source ACA.
  ``datasetid`` references the logical dataset that owns the variable summaries.
  Empty series are included.
* ``timeseries_datasets(tsid, datasetid, tsorder)`` records the physical members.
  Here ``datasetid`` references a positive-``dsid`` row in ``datasets``.
  ``tsorder`` preserves the ACA ordering, including any gaps.
* ``timeseries_variable_steps(tsid, datasetid, name, step_start, steps)`` records
  each variable's zero-based logical step range in each member. Members where
  a variable is absent have no row for that variable. ``name`` is relative to
  the logical series, as in ``variables.name``.

For a logical step, readers can select the physical ACA dataset and its
variable-relative local step using:

.. code-block:: sql

   SELECT d.dsid, d.name, :global_step - v.step_start AS local_step
   FROM timeseries_variable_steps AS v
   JOIN datasets AS d ON d.datasetid = v.datasetid
   WHERE v.tsid = :tsid AND v.name = :variable
     AND :global_step >= v.step_start
     AND :global_step < v.step_start + v.steps;

The ACA still supplies replica locations and embedded metadata. Readers that
do not consume these additional tables may continue discovering membership
through the ACA. Consuming the tables requires corresponding ADIOS2 reader
support.

Refresh and compatibility
-------------------------

Opening an existing ACX adds missing time-series tables without replacing its
existing data. Re-index an ACA to populate the tables and refresh its summaries.
Each archive refresh replaces its membership and summaries in one transaction;
failure retains the previous snapshot. Removing an indexed archive cascades to
all its series, membership, and variable-step records. ``index dump`` includes
the new tables.

Member metadata is inspected through the Campaign reader with an exact dataset
filter and a separate temporary cache per member, so ATS cache entries from
different filters cannot interfere.


Embedded HDF5 metadata
----------------------

The indexer opens the non-HDF5 contents of an ACA with the ADIOS2 Campaign
engine. It supplies an ``exclude-dataset`` expression for every physical HDF5
dataset, including time-series members, and uses an isolated Campaign cache
when filtering. An archive containing only HDF5 datasets does not require
opening a Campaign reader.

Each HDF5 dataset is processed separately by ``hpc_campaign.hdf5_reader``.
The reader selects the newest live replica's embedded file from the ACA
SQLite tables, decompresses it when needed, and opens it read-only using
``h5py.File(BytesIO(image), "r")``. Neither extraction to disk nor access to
the original replica is required. Missing, corrupt, or encrypted metadata
raises an error; the index API currently has no encryption-key argument.
A failed refresh retains the previous committed archive snapshot.

Native HDF5 paths become variable names, with NumPy storage types mapped to
ACX variable types. Root, group, and dataset attributes retain their physical
HDF5 paths. Strings are decoded as UTF-8, complex values use real/imaginary
pairs, and object references are represented by JSON null.

For ADIOS-written HDF5 files, ``StepN`` groups
are combined into logical variables with per-variable step counts and ordered
scalar values. These variables also participate in ACA time-series summaries
and member step mappings.

Scalar values are indexed; array payloads are not read. HDF5 min/max remain SQL NULL because embedded
surrogates may contain placeholder arrays rather than the original values.
