import fnmatch
import re
import sqlite3
from argparse import Namespace
from datetime import datetime, timedelta
from os import walk
from os.path import getsize, isdir, join
from pathlib import Path
from time import sleep, time_ns

from .config import Config

CURRENT_TIME = time_ns()


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
        if answer in ("n", "no"):
            ret = False
            break
        if answer in ("y", "yes"):
            ret = True
            break
        print("Answer y[es] or n[o]: ", end="")
    return ret


def get_folder_size(folder_path: str) -> int:
    size = 0
    for path, _dirs, files in walk(folder_path):
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


def check_campaign_store(campaign_store: str, error_on_empty: bool = False):
    if error_on_empty and not campaign_store:
        raise ValueError(
            "Campaign directory must be set in ~/.config/hpc-campaign/config.yaml or by --campaign_store argument"
        )
    if campaign_store and not isdir(campaign_store):
        raise FileNotFoundError(f"Campaign directory {campaign_store} does not exist")


def matches_pattern(name: str, patterns: list[str], wildcard: bool, ignore_re_errors: bool = True):
    """True if name matches any of the patterns, which are interpreted as regular expressions or file-name wildcards.
    If patterns is an empy list, the function returns True. If ignore_re_errors is False, errors in regular expressions
    will raise the error.
    """
    matches = False
    if len(patterns) == 0:
        matches = True
    else:
        for p in patterns:
            if wildcard:
                if fnmatch.fnmatch(name, p):
                    matches = True
                    break
            else:
                try:
                    if re.search(p, name):
                        matches = True
                        break
                except re.error as e:
                    if not ignore_re_errors:
                        raise e
    return matches


def get_path(path: str, basepath: str = "") -> str:
    """
    Join basepath with path unless
    - `path` is absolute
    - `path` starts with ./, ../, or ~,
    - `basepath` is empty.
    Path with ~ will be expanded to home full path.
    Note, path is not made absolute, only for ~/...
    """
    p = Path(path)

    # Case 1: Absolute path or explicit relative shortcuts
    if p.is_absolute() or path.startswith("./") or path.startswith("../") or path.startswith("~"):
        return str(p.expanduser())

    # Case 2: Relative name + basepath provided
    if basepath:
        return str((Path(basepath) / p).expanduser())

    # Case 3: Fallback
    return str(p)


def set_default_args_from_config(args: Namespace, read_host_config: bool = False) -> Namespace:
    """
    Set default values after user arguments are already parsed.
    Reads the config file and optionally the host file from ~/.config/hpc-campaign
    Adds to args: `user_options`, `verbose`, `campaign_store` and optionally `host_options`
    """
    args.user_options = Config()
    if read_host_config:
        args.host_options = args.user_options.read_host_config()

    if args.verbose == 0:
        args.verbose = args.user_options.verbose

    if args.campaign_store:
        p = Path(args.campaign_store)
    else:
        p = Path(args.user_options.campaign_store_path)

    args.campaign_store = str(p.expanduser().resolve())
    return args


sql_error_list = []


def sql_execute(cur: sqlite3.Cursor, cmd: str, parameters=()) -> sqlite3.Cursor:
    res = cur
    try:
        res = cur.execute(cmd, parameters)
    except sqlite3.OperationalError as oe:
        print(f"SQL execute Operational Error: {oe.sqlite_errorcode}  {oe.sqlite_errorname}: {oe}")
        sql_error_list.append(oe)
        sleep(1.0)
        try:
            res = cur.execute(cmd, parameters)
        except sqlite3.Error as e:
            print(f"SQL re-execute error: {e.sqlite_errorcode}  {e.sqlite_errorname}: {e}")
            raise e
        print("SQL re-execute succeeded")

    except sqlite3.Error as e:
        print(f"SQL execute Error: {e.sqlite_errorcode}  {e.sqlite_errorname}: {e}")
        raise e

    return res


def sql_commit(con: sqlite3.Connection):
    try:
        con.commit()
    except sqlite3.OperationalError as oe:
        print(f"SQL commit Operational Error: {oe.sqlite_errorcode}  {oe.sqlite_errorname}: {oe}")
        sql_error_list.append(oe)
        if oe.sqlite_errorcode == sqlite3.SQLITE_IOERR_DELETE:
            sleep(1.0)
            try:
                con.commit()
            except sqlite3.Error as e:
                print(f"SQL recommit error: {e.sqlite_errorcode}  {e.sqlite_errorname}: {e}")
                raise e
            print("SQL recommit succeeded")
    except sqlite3.Error as e:
        print(f"SQL commit Error: {e.sqlite_errorcode}  {e.sqlite_errorname}: {e}")
        raise e
