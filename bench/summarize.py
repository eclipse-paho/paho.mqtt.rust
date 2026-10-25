#!/usr/bin/env python3
"""Summarizes a matrix.sh results file. See README.md.

usage: summarize.py RESULTS [--compare BASE:NEW ...] [--keep-noisy]

Prints the median of every metric per configuration and variant. Runs whose
involuntary context switches exceed 1% of n are discarded as disturbed by
background load (unless --keep-noisy); `open` runs are exempt, since their
pacing thread spins and is preempted by design. With --compare, also prints a
before/after table: NEW's medians as a ratio of BASE's, per configuration.
"""

import argparse
import statistics
import sys

CONFIG_KEYS = ("broker", "backend", "scenario", "mode", "size", "window",
               "target_rate", "v5", "persist")

# (column title, field, format)
COLUMNS = (
    ("rate/s", "rate", "{:>9.0f}"),
    ("cpu_us/msg", "cpu_us_per_msg", "{:>10.2f}"),
    ("vcsw/msg", "vcsw_per_msg", "{:>8.3f}"),
    ("p50us", "lat_p50_us", "{:>7.1f}"),
    ("p99us", "lat_p99_us", "{:>7.1f}"),
    ("p999us", "lat_p999_us", "{:>8.1f}"),
)



def config_label(cfg):
    c = dict(zip(CONFIG_KEYS, cfg))
    parts = [c["broker"], c["backend"], c["scenario"], c["mode"]]
    if c["scenario"] == "open":
        parts.append(f"@{int(float(c['target_rate']))}/s")
    elif c["size"] != "32":
        parts.append(f"{c['size']}B")
    if c["window"] != "0":
        parts.append(f"W={c['window']}")
    if c["v5"] == "true":
        parts.append("v5")
    if c["persist"] == "true":
        parts.append("persist")
    return " ".join(parts)


def load(path, keep_noisy):
    runs = {}      # (cfg, variant) -> [kv]
    order = []     # configs, in first-seen order
    variants = []
    noisy = 0
    with open(path) as f:
        for line in f:
            if line.startswith("FAIL"):
                print(line.strip())
                continue
            if not line.startswith("RESULT"):
                continue
            kv = dict(x.split("=", 1) for x in line.split()[1:])
            if (not keep_noisy and kv["scenario"] != "open"
                    and int(kv["ivcsw"]) > 0.01 * int(kv["n"])):
                noisy += 1
                continue
            cfg = tuple(kv.get(k, "0") for k in CONFIG_KEYS)
            var = kv.get("variant", "-")
            if cfg not in order:
                order.append(cfg)
            if var not in variants:
                variants.append(var)
            runs.setdefault((cfg, var), []).append(kv)
    if noisy:
        print(f"# discarded {noisy} noisy run(s) (ivcsw > 1% of n)\n")
    return runs, order, variants


def median(rs, field):
    return statistics.median(float(r[field]) for r in rs)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("results")
    ap.add_argument("--compare", action="append", default=[],
                    metavar="BASE:NEW")
    ap.add_argument("--keep-noisy", action="store_true")
    a = ap.parse_args()

    runs, order, variants = load(a.results, a.keep_noisy)
    width = max((len(config_label(c)) for c in order), default=10)
    vwidth = max((len(v) for v in variants), default=4)

    head = f"{'configuration':{width}} {'variant':{vwidth}} {'runs':>4} "
    head += " ".join(f"{t:>{len(f.format(0))}}" for t, _, f in COLUMNS)
    print(head)
    for cfg in order:
        for var in variants:
            rs = runs.get((cfg, var))
            if not rs:
                continue
            row = f"{config_label(cfg):{width}} {var:{vwidth}} {len(rs):>4} "
            row += " ".join(f.format(median(rs, k)) for _, k, f in COLUMNS)
            print(row)

    for pair in a.compare:
        base, new = pair.split(":")
        print(f"\n## {new} / {base}  (>1 means {new} is higher)\n")
        head = f"{'configuration':{width}} "
        head += " ".join(f"{t:>10}" for t, _, _ in COLUMNS)
        print(head)
        for cfg in order:
            b, n = runs.get((cfg, base)), runs.get((cfg, new))
            if not b or not n:
                continue
            cells = []
            for _, k, _ in COLUMNS:
                mb, mn = median(b, k), median(n, k)
                cells.append(f"{mn / mb:>10.2f}" if mb else f"{'-':>10}")
            print(f"{config_label(cfg):{width}} " + " ".join(cells))


if __name__ == "__main__":
    sys.exit(main())
