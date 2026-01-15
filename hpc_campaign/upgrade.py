import argparse
import sqlite3

from .config import ACA_VERSION
from .utils import SQLCommit, SQLErrorList, SQLExecute


# pylint:disable = unused-argument
def _upgrade_to_0_6(args: argparse.Namespace, cur: sqlite3.Cursor, con: sqlite3.Connection):
    print("Upgrade to 0.6")
    # host
    SQLExecute(cur, "ALTER TABLE host ADD default_protocol TEXT")
    # replica
    SQLExecute(
        cur,
        "CREATE TABLE replica_new"
        + "(datasetid INT, hostid INT, dirid INT, archiveid INT, name TEXT, modtime INT, deltime INT"
        + ", keyid INT, size INT"
        + ", PRIMARY KEY (datasetid, hostid, dirid, archiveid, name))",
    )
    SQLExecute(
        cur,
        "INSERT INTO replica_new (datasetid, hostid, dirid, name, modtime, deltime, keyid, size)"
        " SELECT datasetid, hostid, dirid, name, modtime, deltime, keyid, size FROM replica",
    )
    SQLExecute(cur, "UPDATE replica_new SET archiveid = 0")
    SQLExecute(cur, "DROP TABLE replica")
    SQLExecute(cur, "ALTER TABLE replica_new RENAME TO replica")
    # archive
    SQLExecute(
        cur,
        "CREATE TABLE archive_new" + "(dirid INT, tarname TEXT, system TEXT, notes BLOB, PRIMARY KEY (dirid, tarname))",
    )
    SQLExecute(
        cur,
        "INSERT INTO archive_new (dirid, system, notes) SELECT dirid, system, notes FROM archive",
    )
    SQLExecute(cur, 'UPDATE archive_new SET tarname = ""')
    SQLExecute(cur, "DROP TABLE archive")
    SQLExecute(cur, "ALTER TABLE archive_new RENAME TO archive")
    # archiveidx
    SQLExecute(
        cur,
        "create table archiveidx"
        + "(archiveid INT, replicaid INT, filename TEXT, offset INT, offset_data INT, size INT"
        + ", PRIMARY KEY (archiveid, replicaid, filename))",
    )
    # info: update version
    SQLExecute(cur, 'UPDATE info SET version = "0.6"')
    if len(SQLErrorList) == 0:
        SQLCommit(con)
        SQLExecute(cur, "VACUUM")
    else:
        print("SQL Errors detected, drop all changes.")

# pylint: disable=too-many-locals
def _upgrade_to_0_7(args: argparse.Namespace, cur: sqlite3.Cursor, con: sqlite3.Connection):
    print("Upgrade to 0.7")
    # file and replica-file relationship
    SQLExecute(cur, "ALTER TABLE file RENAME TO file_old")
    SQLExecute(
        cur,
        "CREATE TABLE file"
        + "(fileid INTEGER PRIMARY KEY, name TEXT, compression INT, lenorig INT"
        + ", lencompressed INT, modtime INT, checksum TEXT, data BLOB)",
    )
    SQLExecute(
        cur,
        "create table repfiles" + "(replicaid INT, fileid INT, PRIMARY KEY (replicaid, fileid))",
    )
    res = SQLExecute(cur, "select rowid, datasetid from replica")
    replica_datasets = {row[0]: row[1] for row in res.fetchall()}
    res = SQLExecute(
        cur,
        "select rowid, replicaid, name, compression, lenorig, lencompressed, modtime, checksum, data "
        "from file_old order by rowid",
    )
    files = res.fetchall()
    for f in files:
        (
            old_fileid,
            replicaid,
            name,
            compression,
            lenorig,
            lencompressed,
            modtime,
            checksum,
            data,
        ) = f
        datasetid = replica_datasets.get(replicaid, -1)
        curFile = SQLExecute(
            cur,
            "select file.fileid from file "
            "join repfiles on file.fileid = repfiles.fileid "
            "join replica on repfiles.replicaid = replica.rowid "
            "where replica.datasetid = ? and file.name = ? and file.lenorig = ? "
            "and file.lencompressed = ? and file.checksum = ? limit 1",
            (datasetid, name, lenorig, lencompressed, checksum),
        )
        row = curFile.fetchone()
        if row is None:
            SQLExecute(
                cur,
                "insert into file "
                "(fileid, name, compression, lenorig, lencompressed, modtime, checksum, data) "
                "values (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    old_fileid,
                    name,
                    compression,
                    lenorig,
                    lencompressed,
                    modtime,
                    checksum,
                    data,
                ),
            )
            fileid = old_fileid
        else:
            fileid = row[0]
        SQLExecute(
            cur,
            "insert into repfiles (replicaid, fileid) values (?, ?)",
            (replicaid, fileid),
        )
    SQLExecute(cur, "DROP TABLE file_old")
    # info: update version
    SQLExecute(cur, 'UPDATE info SET version = "0.7"')
    if len(SQLErrorList) == 0:
        SQLCommit(con)
        SQLExecute(cur, "VACUUM")
    else:
        print("SQL Errors detected, drop all changes.")


UPGRADESTEP = {
    "0.5": {"new_version": "0.6", "func": _upgrade_to_0_6},
    "0.6": {"new_version": "0.7", "func": _upgrade_to_0_7},
}


def UpgradeACA(args: argparse.Namespace, cur: sqlite3.Cursor, con: sqlite3.Connection):
    res = SQLExecute(cur, 'select version from info where id = "ACA"')
    info = res.fetchone()
    version: str = info[0]
    if version != ACA_VERSION:
        print(f"Current version is {version}")
        # vlist = version.split('.')
        v = UPGRADESTEP.get(version)
        if v is not None:
            v["func"](args, cur, con)  # type: ignore[operator]
        else:
            print("This version cannot be upgraded")
    else:
        print(f"This archive has the latest version already: {ACA_VERSION}")
