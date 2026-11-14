"""Share CPU list helpers between suite.py and build-broker.sh."""


def parse_cpu_list(spec):
    """Expand a spec like "0,4-5" into a sorted core list."""
    out = []
    for part in (spec or "").split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            a, b = part.split("-", 1)
            out.extend(range(int(a), int(b) + 1))
        else:
            out.append(int(part))
    return sorted(set(out))


def format_cpu_list(cores):
    """Compress a core list into a spec like "0,4-5"."""
    cs = sorted(set(cores))
    if not cs:
        return ""
    parts = []
    start = prev = cs[0]
    for c in cs[1:]:
        if c == prev + 1:
            prev = c
            continue
        parts.append(str(start) if start == prev else "%d-%d" % (start, prev))
        start = prev = c
    parts.append(str(start) if start == prev else "%d-%d" % (start, prev))
    return ",".join(parts)
