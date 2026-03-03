# v17c Plan to April 2026

*Deadline: March 31, 2026*

## Priority Overview

| Priority | Category | Est. Issues | Status |
|----------|----------|-------------|--------|
| P0 | Blockers | 2 | Must fix |
| P1 | Core Fixes | 5 | Should fix |
| P2 | Lake/facc | 4 | Important |
| P3 | Exports | 3 | Required |
| P4 | Documentation | 2 | Required |
| P5 | PostgreSQL/GUI | 2 | Should have |
| Deferred | Testing | 1 | Post-April |

---

## P0: Blockers (Week 1)

### ~~Copy type column from v17b GPKG → v17c DuckDB~~ ✅ DONE (2026-02-03)
- Copied to v17b DuckDB, v17c DuckDB, PostgreSQL v17c
- All 248,674 reaches have type populated
- **TODO later**: Add type to validation pipeline

### Error-level lint fixes (#101, #87)
- **T008**: dist_out negative values
- **T009**: dist_out=0 at non-outlets
- **V001**: hydro_dist_out monotonicity (1,210 cases - verify documented)
- **V005**: hydro_dist_out coverage gaps

---

## P1: Core Fixes (Weeks 2-3)

### Topology errors (#87)
- Review T001 (dist_out monotonicity) failures
- Investigate causes: lake sandwiches? actual topology errors?

### Mainstem lint (#83)
- ✅ V004: mainstem continuity
- ✅ V006: mainstem coverage
- ✅ V007/V008: best_headwater/outlet validity
- ✅ V011/T019/T020/A011-A014: river_name audit and formatting fixes (2026-03-03)
- ✅ V013/V014/V015: main_path_id audit and documentation (2026-03-03)

### Validation (#70, #71, #72)
- [x] Run full lint suite, document baseline (Progressing: path_topology, river_name audits complete)
- [ ] Create tracking spreadsheet for violations by region

---

## P2: Lake/facc (Weeks 3-5)

### Lake routing investigation
- Inventory cases like reach 62270000143 (Amazon mid-channel bar)
- Determine: systematic pattern or one-offs?
- Options:
  - Manual review in topology_reviewer GUI
  - Heuristic detection (lakeflag=1 surrounded by lakeflag=0)

### Remaining lake sandwiches
- 393 medium-confidence: run topology_reviewer
- 1,142 low-confidence: batch review or defer

### facc ML model
- Phase 1: Implement regression baseline
- Phase 2: Random Forest classifier
- Features: facc, width, slope, reach_length, n_rch_up, stream_order, facc/width ratio
- Validate: does it reduce T003 violations after fixing?

---

## P3: Exports (Weeks 5-6)

### DuckDB final (#89)
- Verify all v17c columns present
- Run lint, document any remaining issues

### GeoPackage (#90)
- Export reaches, nodes, centerlines
- Verify in QGIS

### NetCDF (if required)
- Match v17b NetCDF structure
- Add v17c-specific variables

---

## P4: Documentation (Week 6)

### Release notes (#92)
- List all v17c additions
- Document known issues
- Comparison with v17b

### Data dictionary (#93)
- Update for v17c columns
- Add validation spec references

---

## P5: PostgreSQL/GUI (Weeks 6-7)

### PostgreSQL deployment
- Deploy to production server
- Test multi-user QGIS editing workflow

### Undergrad QGIS GUI
- Set up editing interface
- Train on topology_reviewer workflow
- Begin lake sandwich fixes

---

## Deferred to Post-April

| Item | Reason |
|------|--------|
| PostgreSQL production testing | Code complete, needs extended real-world testing |

---

## Weekly Milestones

| Week | Focus | Deliverable |
|------|-------|-------------|
| 1 (Feb 3-7) | P0 blockers | type column copied, error-lint run |
| 2 (Feb 10-14) | P1 topology | #87 reviewed, violations documented |
| 3 (Feb 17-21) | P1/P2 validation | Lint baseline, lake inventory |
| 4 (Feb 24-28) | P2 lake fixes | Topology_reviewer sessions |
| 5 (Mar 3-7) | P2/P3 facc + export | Regression + RF model, DuckDB export |
| 6 (Mar 10-14) | P3/P4/P5 export + docs | GeoPackage, release notes, PostgreSQL deploy |
| 7 (Mar 17-21) | P5 + buffer | Undergrad GUI setup, catch-up |
| 8 (Mar 24-28) | Release prep | Final lint, stakeholder review |

---

## Success Criteria for March 31

### Must Have
- [ ] type column in v17c DuckDB
- [ ] Zero error-level lint violations (or documented exceptions)
- [ ] DuckDB export with all v17c columns
- [ ] Release notes documenting v17c additions

### Should Have
- [ ] GeoPackage export
- [ ] Lake sandwich count < 500 (down from 3,167)
- [ ] Lint baseline documented by region
- [ ] facc ML model (regression + RF)
- [ ] PostgreSQL production deployment
- [ ] Undergrad QGIS GUI operational

### Nice to Have
- [ ] NetCDF export

---

## GitHub Issue Tracking

All work tracked in milestone: **v17c-april-2026**

Status snapshot (2026-02-21):
- Open: 20
- Closed: 64
- Due date: 2026-03-31

Primary filter: `milestone:"v17c-april-2026" is:open sort:updated-desc`

Sync check: `scripts/maintenance/check_roadmap_sync.sh`

When closing issues, reference this plan in comments.
