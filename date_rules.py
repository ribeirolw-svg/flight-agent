from datetime import datetime, timedelta

def generate_date_pairs(start, end, length, return_deadline):
    start = datetime.fromisoformat(start)
    end = datetime.fromisoformat(end)
    deadline = datetime.fromisoformat(return_deadline)

    pairs = []
    current = start

    while current <= end:
        ret = current + timedelta(days=length)
        if ret <= deadline:
            pairs.append((current.date().isoformat(), ret.date().isoformat()))
        current += timedelta(days=1)

    return pairs
