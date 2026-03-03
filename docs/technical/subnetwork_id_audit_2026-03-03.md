# subnetwork_id Audit (2026-03-03)

## Scope

Audited `subnetwork_id` on `sword_v17c.duckdb` (248,673 reaches, 6 regions).

`subnetwork_id` is a v17c variable assigning each reach to a weakly connected component
via `nx.weakly_connected_components()`, offset by Pfafstetter continent codes for global
uniqueness (e.g., NA: 7,000,001+, AF: 1,000,001+).

## Checks Implemented

Six new lint checks (V026-V031) added to `src/sword_duckdb/lint/checks/v17c.py`:

| Check | Severity | Rule | Status |
|---|---|---|---|
| V026 | ERROR | No NULL subnetwork_id on connected non-ghost reaches | PASS |
| V027 | ERROR | Values within Pfafstetter band for region | PASS |
| V028 | ERROR | Topology-connected reaches share same subnetwork_id | PASS |
| V029 | ERROR | No subnetwork_id appears in multiple regions | PASS |
| V030 | INFO | Isolated reaches form singleton components | PASS (0 isolated) |
| V031 | INFO | Size distribution statistics | PASS |

**All 6 checks pass. Zero violations.**

## Distribution by Region

| Region | Components | Reaches | Min Size | Median | Max Size | Mean | Singletons |
|--------|-----------|---------|----------|--------|----------|------|------------|
| AF | 230 | 21,441 | 3 | 20 | 4,654 | 93.2 | 0 |
| AS | 854 | 100,185 | 2 | 14 | 11,021 | 117.3 | 0 |
| EU | 436 | 31,103 | 3 | 12 | 3,264 | 71.3 | 0 |
| NA | 586 | 38,696 | 3 | 13 | 5,910 | 66.0 | 0 |
| OC | 676 | 15,089 | 3 | 8 | 1,100 | 22.3 | 0 |
| SA | 245 | 42,159 | 3 | 16 | 21,074 | 172.1 | 0 |
| **Total** | **3,027** | **248,673** | **2** | **12** | **21,074** | **82.1** | **0** |

## Pfafstetter Band Ranges

| Region | Min ID | Max ID | Unique IDs |
|--------|--------|--------|-----------|
| AF | 1,000,001 | 1,000,230 | 230 |
| AS | 3,000,001 | 3,000,854 | 854 |
| EU | 2,000,001 | 2,000,436 | 436 |
| NA | 7,000,001 | 7,000,586 | 586 |
| OC | 5,000,001 | 5,000,676 | 676 |
| SA | 6,000,001 | 6,000,245 | 245 |

All bands are contiguous within their Pfafstetter range. Max per-region count (854 in AS)
uses <0.1% of the 1M band, so headroom is ample.

## subnetwork_id vs network (v17b)

| Region | v17b networks | v17c subnetworks | Exact matches |
|--------|--------------|------------------|---------------|
| AF | 79 | 230 | 0 |
| AS | 246 | 854 | 0 |
| EU | 103 | 436 | 0 |
| NA | 105 | 586 | 0 |
| OC | 211 | 676 | 0 |
| SA | 53 | 245 | 0 |

Zero exact matches (expected: Pfafstetter offsets make numerical equality impossible).
v17c has ~3x more components than v17b's `network` field globally.

## Cross-Network Subnetworks

19 of 3,027 subnetwork_ids (0.6%) span multiple v17b `network` values. This means
the v17c topology graph connects reaches that v17b considered separate networks.

| subnetwork_id | Region | v17b Networks | Reaches | Rivers |
|---|---|---|---|---|
| 6,000,001 | SA | [1, 2] | 21,074 | Amazon basin |
| 3,000,011 | AS | [1, 82] | 11,021 | Chang Jiang (Yangtze) basin |
| 3,000,063 | AS | [1, 14] | 7,736 | Brahmaputra/Ganges basin |
| 3,000,012 | AS | [3, 6] | 3,113 | Pearl River basin |
| 3,000,041 | AS | [2, 3] | 2,796 | Pyasina River (Siberia) |
| 2,000,048 | EU | [2, 3] | 1,817 | Dnieper/Don basin |
| 2,000,035 | EU | [1, 4] | 1,081 | Euphrates/Tigris |
| 7,000,015 | NA | [1, 2] | 1,080 | Columbia/Snake basin |
| 3,000,137 | AS | [10, 13] | 1,057 | Mahanadi basin |
| 5,000,019 | OC | [5, 10] | 283 | Kikori basin (PNG) |

These cross-network merges are expected — v17b `network` was computed at a different
processing stage (before topology fixes). The v17c graph correctly identifies these
as single weakly-connected components.

## Key Findings

1. **100% coverage**: All 248,673 reaches have subnetwork_id (0 NULLs)
2. **Pfafstetter bands clean**: All IDs fall within their region's band
3. **Topology consistent**: 0 edges where neighbors have different subnetwork_id (495,620 edges checked)
4. **Globally unique**: 0 cross-region ID collisions
5. **No singletons**: Smallest component has 2 reaches (0 isolated reaches in the dataset)
6. **19 cross-network merges**: Expected, not a data error

## No Action Required

subnetwork_id is correctly computed with no violations. No fixes needed.
