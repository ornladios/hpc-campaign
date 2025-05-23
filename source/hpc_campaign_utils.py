from datetime import datetime


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
