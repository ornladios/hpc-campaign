from datetime import datetime, timedelta
from time import sleep
from os import walk
from os.path import join, getsize
import sqlite3


def timestamp_to_datetime(timestamp: int) -> datetime:
    digits = len(str(int(timestamp)))
    t = float(timestamp)
    if digits > 18:
        t = t / 1000000000
    elif digits > 15:
        t = t / 1000000
    elif digits > 12:
        t = t / 1000
    return datetime.fromtimestamp(t)


def datetime_to_str(t: datetime) -> str:
    # The constant 31556952 is used in the ls source code,
    # available at https://www.gnu.org/software/coreutils/.
    # It roughly represents the number of seconds in a Gregorian year.
    six_months_in_seconds = 31556952 // 2
    if (datetime.now() - t) < timedelta(seconds=six_months_in_seconds):
        date_format = "%b %e %H:%M"
    else:
        date_format = "%b %e  %Y"
    return t.strftime(date_format)
    # return t.strftime('%Y-%m-%d %H:%M:%S')


def timestamp_to_str(timestamp: int) -> str:
    t = timestamp_to_datetime(timestamp)
    return datetime_to_str(t)


def input_yes_or_no(msg: str, default_answer: bool = False) -> bool:
    ret = default_answer
    print(msg, end="")
    while True:
        answer = input().lower()
        if answer == "n" or answer == "no":
            ret = False
            break
        if answer == "y" or answer == "yes":
            ret = True
            break
        print("Answer y[es] or n[o]: ", end="")
    return ret


def get_folder_size(Folderpath: str) -> int:
    size = 0
    for path, dirs, files in walk(Folderpath):
        for f in files:
            size += getsize(join(path, f))
    return size


def sizeof_fmt(num: int, suffix="B") -> str:
    if num < 1024:
        return f"{num}"
    n = float(num) / 1024
    for unit in ("Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"):
        if abs(n) < 1024.0:
            return f"{n:3.1f} {unit}{suffix}"
        n /= 1024.0
    return f"{n:.1f} Yi{suffix}"


SQLErrorList = []


def SQLExecute(cur: sqlite3.Cursor, cmd: str, parameters=()) -> sqlite3.Cursor:
    res = cur
    try:
        res = cur.execute(cmd, parameters)
    except sqlite3.OperationalError as e:
        print(
            f"SQL execute Operational Error: {e.sqlite_errorcode}  {e.sqlite_errorname}: {e}"
        )
        SQLErrorList.append(e)
        sleep(1.0)
        try:
            res = cur.execute(cmd, parameters)
        except sqlite3.Error as sqle:
            print(
                f"SQL re-execute error: {e.sqlite_errorcode}  {e.sqlite_errorname}: {sqle}"
            )
            raise e
        else:
            print("SQL re-execute succeeded")

    except sqlite3.Error as e:
        print(f"SQL execute Error: {e.sqlite_errorcode}  {e.sqlite_errorname}: {e}")
        raise e
    finally:
        return res


def SQLCommit(con: sqlite3.Connection):
    """Function to commit the SQL connection
    """
    try:
        con.commit()
    except sqlite3.OperationalError as e:
        print(
            f"SQL commit Operational Error: {e.sqlite_errorcode}  {e.sqlite_errorname}: {e}"
        )
        SQLErrorList.append(e)
        if e.sqlite_errorcode == sqlite3.SQLITE_IOERR_DELETE:
            sleep(1.0)
            try:
                con.commit()
            except sqlite3.Error as sqle:
                print(
                    f"SQL recommit error: {e.sqlite_errorcode}  {e.sqlite_errorname}: {sqle}"
                )
                raise sqle
            print("SQL recommit succeeded")
    except sqlite3.Error as e:
        print(f"SQL commit Error: {e.sqlite_errorcode}  {e.sqlite_errorname}: {e}")
        raise e
