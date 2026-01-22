import argparse
import sqlite3

from .config import ACA_VERSION
from .utils import sql_commit, sql_error_list, sql_execute


# pylint:disable = unused-argument
def _upgrade_to_0_6(args: argparse.Namespace, cur: sqlite3.Cursor, con: sqlite3.Connection) -> str:
    print("Upgrade to 0.6")
    # host
    sql_execute(cur, "ALTER TABLE host ADD default_protocol TEXT")
    # replica
    sql_execute(
        cur,
        "CREATE TABLE replica_new"
        + "(datasetid INT, hostid INT, dirid INT, archiveid INT, name TEXT, modtime INT, deltime INT"
        + ", keyid INT, size INT"
        + ", PRIMARY KEY (datasetid, hostid, dirid, archiveid, name))",
    )
    sql_execute(
        cur,
        "INSERT INTO replica_new (datasetid, hostid, dirid, name, modtime, deltime, keyid, size)"
        " SELECT datasetid, hostid, dirid, name, modtime, deltime, keyid, size FROM replica",
    )
    sql_execute(cur, "UPDATE replica_new SET archiveid = 0")
    sql_execute(cur, "DROP TABLE replica")
    sql_execute(cur, "ALTER TABLE replica_new RENAME TO replica")
    # archive
    sql_execute(
        cur,
        "CREATE TABLE archive_new" + "(dirid INT, tarname TEXT, system TEXT, notes BLOB, PRIMARY KEY (dirid, tarname))",
    )
    sql_execute(
        cur,
        "INSERT INTO archive_new (dirid, system, notes) SELECT dirid, system, notes FROM archive",
    )
    sql_execute(cur, 'UPDATE archive_new SET tarname = ""')
    sql_execute(cur, "DROP TABLE archive")
    sql_execute(cur, "ALTER TABLE archive_new RENAME TO archive")
    # archiveidx
    sql_execute(
        cur,
        "create table archiveidx"
        + "(archiveid INT, replicaid INT, filename TEXT, offset INT, offset_data INT, size INT"
        + ", PRIMARY KEY (archiveid, replicaid, filename))",
    )
    # info: update version
    sql_execute(cur, 'UPDATE info SET version = "0.6"')
    if len(sql_error_list) == 0:
        sql_commit(con)
        sql_execute(cur, "VACUUM")
        return "0.6"

    print("SQL Errors detected, drop all changes.")
    return "0.5"


# pylint: disable=too-many-locals
def _upgrade_to_0_7(args: argparse.Namespace, cur: sqlite3.Cursor, con: sqlite3.Connection) -> str:
    print("Upgrade to 0.7")
    # file and replica-file relationship
    sql_execute(cur, "ALTER TABLE file RENAME TO file_old")
    sql_execute(
        cur,
        "CREATE TABLE file"
        + "(fileid INTEGER PRIMARY KEY, name TEXT, compression INT, lenorig INT"
        + ", lencompressed INT, modtime INT, checksum TEXT, data BLOB)",
    )
    sql_execute(
        cur,
        "create table repfiles" + "(replicaid INT, fileid INT, PRIMARY KEY (replicaid, fileid))",
    )
    res = sql_execute(cur, "select rowid, datasetid from replica")
    replica_datasets = {row[0]: row[1] for row in res.fetchall()}
    res = sql_execute(
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
        cur_file = sql_execute(
            cur,
            "select file.fileid from file "
            "join repfiles on file.fileid = repfiles.fileid "
            "join replica on repfiles.replicaid = replica.rowid "
            "where replica.datasetid = ? and file.name = ? and file.lenorig = ? "
            "and file.lencompressed = ? and file.checksum = ? limit 1",
            (datasetid, name, lenorig, lencompressed, checksum),
        )
        row = cur_file.fetchone()
        if row is None:
            sql_execute(
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
        sql_execute(
            cur,
            "insert into repfiles (replicaid, fileid) values (?, ?)",
            (replicaid, fileid),
        )
    sql_execute(cur, "DROP TABLE file_old")
    # info: update version
    sql_execute(cur, 'UPDATE info SET version = "0.7"')
    if len(sql_error_list) == 0:
        sql_commit(con)
        sql_execute(cur, "VACUUM")
        return "0.7"

    print("SQL Errors detected, drop all changes.")
    return "0.6"


UPGRADESTEP = {
    "0.5": {"new_version": "0.6", "func": _upgrade_to_0_6},
    "0.6": {"new_version": "0.7", "func": _upgrade_to_0_7},
}


def upgrade_aca(args: argparse.Namespace, cur: sqlite3.Cursor, con: sqlite3.Connection) -> str:
    res = sql_execute(cur, 'select version from info where id = "ACA"')
    info = res.fetchone()
    version: str = info[0]
    new_version = version
    if version != ACA_VERSION:
        print(f"Current version is {version}")
        # vlist = version.split('.')
        v = UPGRADESTEP.get(version)
        if v is not None:
            new_version = v["func"](args, cur, con)  # type: ignore[operator]
        else:
            print("This version cannot be upgraded")
    else:
        print(f"This archive has the latest version already: {ACA_VERSION}")
    return new_version
