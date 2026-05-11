<!--
Copyright(c) MobilityDB Contributors

This documentation is licensed under a
Creative Commons Attribution-Share Alike 3.0 License
https://creativecommons.org/licenses/by-sa/3.0/
-->

# MobilityDB-BerlinMOD PR Reviewer Guide

Quick reference for anyone reviewing open pull requests in **MobilityDB-BerlinMOD** —
the canonical BerlinMOD data generator + portable SQL benchmark surface for the
MobilityDB ecosystem.
Updated in the same commit as any PR that changes PR state or adds new branches.
**Last updated: 2026-05-11 — 2 open PRs (#23 ecosystem-standards, #24 trip_h3 export). Both source-complete, awaiting external committer review.**

---

## How to find this guide

- **In the repo:** `doc/contributing/reviewer-guide.md`
- **Rule:** every commit that opens, closes, or restructures a PR must update this file in the same commit. A one-liner status change is enough; a fuller rewrite is needed when the dependency graph changes.

Reviewers landing in any of the five ecosystem repos (MobilityDB / MobilityDuck /
MobilitySpark / JMEOS / MobilityDB-BerlinMOD) find the same canonical structure
at the same path.

---

## CI legend

| Symbol | Meaning |
|--------|---------|
| ✅ | All checks green |
| ❌ | Real failure — needs investigation before review |
| ⏳ | CI running |
| ❓ | No CI result yet (doc-only, draft, or external PR) |
| ⚠️ | Non-blocking failure (e.g. macOS/Windows `continue-on-error`, Codacy ACTION_REQUIRED — maintainer overrides in UI) |

---

## Dependency chain — land in this order

```
PR #23  feat/ecosystem-standards  (LICENSE + ecosystem README + portability_export
                                    function + Q1-Q6 portable SQL — foundation)
  └─► PR #24  feat/portability-export-th3index  (extends portability_export to
                                                  also write trip_h3 column —
                                                  pairs with MobilityDB PR #938
                                                  geo_to_h3index_set kernel)
```

**PR #23 should land first** — it introduces the portability framework that PR #24 extends.

PR #24 also has a cross-repo dependency: it calls `tgeompoint_to_th3index(Trip, 7)`
which comes from MobilityDB PRs #807 / #866 / #893 (the th3index type itself).
Per `feedback_issued_pr_treat_as_landed.md`, downstream work proceeds without
waiting on upstream merge — PR #24 is source-complete; the actual `trip_h3`
column generation works once the MobilityDB th3index branch lands.

---

## Tier 1 — Merge after MobilityDB ecosystem standards land

| PR | Branch | Description | CI |
|----|--------|-------------|----|
| #23 | `estebanzimanyi:feat/ecosystem-standards` | LICENSE + copyright header sweep + cross-platform portability scaffold: `berlinmod_portability_export(fullpath text)` writes 5 CSVs in the shared schema (vehicles, trips as WKT tgeompoint, query_licences/instants/points); `berlinmod_chapter1_queries_portable.sql` for Q1-Q6 portable dialect; README extension; CI workflow improvements. Stale fork branches deleted (`load_export`, `sync`, `version1.3`) | ✅ |

---

## Tier 0 — Performance (depends on upstream th3index)

| PR | Branch | Description | CI | Notes |
|----|--------|-------------|----|-------|
| #24 | `estebanzimanyi:feat/portability-export-th3index` | Extends `berlinmod_portability_export()` to write a 4-column `trips.csv` including `trip_h3` (th3index hex-WKB at H3 resolution 7). Cross-platform spatial prefilter source for BerlinMOD Q2/Q4/Q5/Q6/Q10. | ❓ | Stacks on **MobilityDB #807** (th3index type) + #866 (spatial wiring) + #938 (geo_to_h3index_set). Source-complete; needs MobilityDB th3index merge to actually generate trip_h3 from a live PG instance. Paired with MobilitySpark PR #9 (the consumer side). |

---

## Review checklist

For every MobilityDB-BerlinMOD PR, verify:

- [ ] PostgreSQL License header on every new `.sql` / `.c` file
- [ ] New SQL function follows ecosystem RFC #861 portable-name convention (camelCase, no operators, no `::numeric` casts)
- [ ] Generated CSV output is platform-portable (WKT or hex-WKB; no platform-specific binary encodings)
- [ ] Any new generator step has a `RAISE INFO` execution trace + start/end timestamps
- [ ] If the PR modifies `berlinmod_export.sql` or `berlinmod_portability_export()`, the cross-platform loaders in MobilitySpark `berlinmod/load_*.sql` need a matching update — flag in the PR description

---

## Cross-repo links

- **MobilityDB:** [doc/contributing/reviewer-guide.md](https://github.com/MobilityDB/MobilityDB/blob/master/doc/contributing/reviewer-guide.md)
- **MobilityDuck:** [doc/contributing/reviewer-guide.md](https://github.com/MobilityDB/MobilityDuck/blob/master/doc/contributing/reviewer-guide.md)
- **MobilitySpark:** [doc/contributing/reviewer-guide.md](https://github.com/MobilityDB/MobilitySpark/blob/main/doc/contributing/reviewer-guide.md)
- **JMEOS:** [doc/contributing/reviewer-guide.md](https://github.com/MobilityDB/JMEOS/blob/main/doc/contributing/reviewer-guide.md)
