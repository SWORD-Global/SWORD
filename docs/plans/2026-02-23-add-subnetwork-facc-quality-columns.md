# Add subnetwork_id + facc_quality Columns Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add `subnetwork_id INTEGER` and `facc_quality VARCHAR` to reaches and nodes tables (schema DDL, migration helper, and canonical column order), closing issue #33.

**Architecture:** Schema DDL gets the column definitions; `add_v17c_columns()` migration helper gets ALTER TABLE entries so existing DBs are upgraded in-place; `column_order.py` gets the canonical positions. Nodes inherit `facc_quality` from their parent reach via a JOIN UPDATE run inside `add_v17c_columns()`. No data is populated by this PR — values remain NULL until the v17c pipeline / facc-fix workflow runs.

**Tech Stack:** DuckDB, Python, pytest

---

### Task 1: Write failing tests for new schema columns

**Files:**
- Modify: `tests/sword_duckdb/test_sword_db.py`

**Step 1: Add two tests to `TestSWORDDatabaseSchema`**

```python
def test_reaches_has_subnetwork_id_and_facc_quality(self, tmp_path):
    from src.sword_duckdb.sword_db import SWORDDatabase

    db_path = tmp_path / "new.duckdb"
    db = SWORDDatabase(db_path)
    db.init_schema()

    result = db.query(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_name = 'reaches'"
    )
    cols = result["column_name"].tolist()
    assert "subnetwork_id" in cols
    assert "facc_quality" in cols
    db.close()


def test_nodes_has_subnetwork_id_and_facc_quality(self, tmp_path):
    from src.sword_duckdb.sword_db import SWORDDatabase

    db_path = tmp_path / "new.duckdb"
    db = SWORDDatabase(db_path)
    db.init_schema()

    result = db.query(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_name = 'nodes'"
    )
    cols = result["column_name"].tolist()
    assert "subnetwork_id" in cols
    assert "facc_quality" in cols
    db.close()
```

**Step 2: Run to verify they fail**

```bash
cd /Users/jakegearon/projects/SWORD
python -m pytest tests/sword_duckdb/test_sword_db.py::TestSWORDDatabaseSchema::test_reaches_has_subnetwork_id_and_facc_quality tests/sword_duckdb/test_sword_db.py::TestSWORDDatabaseSchema::test_nodes_has_subnetwork_id_and_facc_quality -v
```

Expected: **FAIL** — columns not in schema yet.

---

### Task 2: Add columns to schema DDL

**Files:**
- Modify: `src/sword_duckdb/schema.py`

**Step 1: In the `nodes` DDL, add after the `network` column (line ~136)**

```python
    network INTEGER,             -- connected network ID
    subnetwork_id INTEGER,       -- weakly connected component ID (v17c)
```

And after `facc` (line ~124):

```python
    facc DOUBLE,                 -- flow accumulation (km^2)
    facc_quality VARCHAR,        -- facc correction status flag (v17c)
```

**Step 2: In the `reaches` DDL, add after the `network` column (line ~246)**

```python
    network INTEGER,             -- connected network ID
    subnetwork_id INTEGER,       -- weakly connected component ID (v17c)
```

And after `facc` (line ~231):

```python
    facc DOUBLE,                 -- flow accumulation (km^2)
    facc_quality VARCHAR,        -- facc correction status flag (v17c)
```

**Step 3: Run failing tests to verify they now pass**

```bash
cd /Users/jakegearon/projects/SWORD
python -m pytest tests/sword_duckdb/test_sword_db.py::TestSWORDDatabaseSchema::test_reaches_has_subnetwork_id_and_facc_quality tests/sword_duckdb/test_sword_db.py::TestSWORDDatabaseSchema::test_nodes_has_subnetwork_id_and_facc_quality -v
```

Expected: **PASS**

**Step 4: Commit**

```bash
git add src/sword_duckdb/schema.py tests/sword_duckdb/test_sword_db.py
git commit -m "feat: add subnetwork_id and facc_quality columns to reaches/nodes DDL (#33)"
```

---

### Task 3: Write failing test for migration helper

**Files:**
- Modify: `tests/sword_duckdb/test_sword_db.py`

**Step 1: Add a test that calls `add_v17c_columns()` on a DB missing the new columns**

```python
def test_add_v17c_columns_adds_subnetwork_and_facc_quality(self, tmp_path):
    """add_v17c_columns() upgrades existing DB missing the new columns."""
    from src.sword_duckdb.sword_db import SWORDDatabase
    from src.sword_duckdb.schema import add_v17c_columns

    db_path = tmp_path / "old.duckdb"
    db = SWORDDatabase(db_path)
    db.init_schema()

    # Simulate an old DB by dropping the new columns
    con = db._db  # raw DuckDB connection
    for col in ("subnetwork_id", "facc_quality"):
        try:
            con.execute(f"ALTER TABLE reaches DROP COLUMN {col}")
            con.execute(f"ALTER TABLE nodes DROP COLUMN {col}")
        except Exception:
            pass  # column may not exist yet

    add_v17c_columns(con)

    for table in ("reaches", "nodes"):
        result = con.execute(
            f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table}'"
        ).fetchall()
        cols = {r[0] for r in result}
        assert "subnetwork_id" in cols, f"subnetwork_id missing from {table}"
        assert "facc_quality" in cols, f"facc_quality missing from {table}"

    db.close()
```

**Step 2: Run to verify it fails**

```bash
cd /Users/jakegearon/projects/SWORD
python -m pytest tests/sword_duckdb/test_sword_db.py::TestSWORDDatabaseSchema::test_add_v17c_columns_adds_subnetwork_and_facc_quality -v
```

Expected: **FAIL**

---

### Task 4: Update `add_v17c_columns()` migration helper

**Files:**
- Modify: `src/sword_duckdb/schema.py`

**Step 1: Add to `nodes_v17c_columns` list (around line 860)**

```python
    nodes_v17c_columns = [
        ("best_headwater", "BIGINT"),
        ("best_outlet", "BIGINT"),
        ("pathlen_hw", "DOUBLE"),
        ("pathlen_out", "DOUBLE"),
        ("subnetwork_id", "INTEGER"),   # weakly connected component ID
        ("facc_quality", "VARCHAR"),    # facc correction status flag
    ]
```

**Step 2: Add to `reaches_v17c_columns` list (around line 868)**

```python
    reaches_v17c_columns = [
        ("best_headwater", "BIGINT"),
        ("best_outlet", "BIGINT"),
        ("pathlen_hw", "DOUBLE"),
        ("pathlen_out", "DOUBLE"),
        ("main_path_id", "BIGINT"),
        ("is_mainstem_edge", "BOOLEAN DEFAULT FALSE"),
        ("dist_out_short", "DOUBLE"),
        ("hydro_dist_out", "DOUBLE"),
        ("hydro_dist_hw", "DOUBLE"),
        ("rch_id_up_main", "BIGINT"),
        ("rch_id_dn_main", "BIGINT"),
        ("subnetwork_id", "INTEGER"),   # weakly connected component ID
        ("facc_quality", "VARCHAR"),    # facc correction status flag
    ]
```

**Step 3: After the `_add_columns_to_table` calls, propagate facc_quality from reaches to nodes**

Find the section after both `_add_columns_to_table` calls and add:

```python
    # Propagate facc_quality from reaches to nodes (nodes inherit reach-level flag)
    db.execute("""
        UPDATE nodes
        SET facc_quality = r.facc_quality
        FROM reaches r
        WHERE nodes.reach_id = r.reach_id
          AND nodes.region = r.region
          AND r.facc_quality IS NOT NULL
    """)
```

**Step 4: Run failing test to verify it now passes**

```bash
cd /Users/jakegearon/projects/SWORD
python -m pytest tests/sword_duckdb/test_sword_db.py::TestSWORDDatabaseSchema::test_add_v17c_columns_adds_subnetwork_and_facc_quality -v
```

Expected: **PASS**

**Step 5: Commit**

```bash
git add src/sword_duckdb/schema.py tests/sword_duckdb/test_sword_db.py
git commit -m "feat: add subnetwork_id and facc_quality to add_v17c_columns migration (#33)"
```

---

### Task 5: Write failing tests for column_order.py

**Files:**
- Modify: `tests/sword_duckdb/test_column_order.py`

**Step 1: Add tests to `TestCanonicalLists`**

```python
def test_reaches_has_subnetwork_id(self):
    assert "subnetwork_id" in REACHES_COLUMN_ORDER

def test_reaches_has_facc_quality(self):
    assert "facc_quality" in REACHES_COLUMN_ORDER

def test_nodes_has_subnetwork_id(self):
    assert "subnetwork_id" in NODES_COLUMN_ORDER

def test_nodes_has_facc_quality(self):
    assert "facc_quality" in NODES_COLUMN_ORDER

def test_facc_quality_after_facc_reaches(self):
    idx_facc = REACHES_COLUMN_ORDER.index("facc")
    idx_fq = REACHES_COLUMN_ORDER.index("facc_quality")
    assert idx_fq == idx_facc + 1, "facc_quality should immediately follow facc in reaches"

def test_subnetwork_id_after_network_reaches(self):
    idx_net = REACHES_COLUMN_ORDER.index("network")
    idx_sub = REACHES_COLUMN_ORDER.index("subnetwork_id")
    assert idx_sub == idx_net + 1, "subnetwork_id should immediately follow network in reaches"
```

**Step 2: Run to verify they fail**

```bash
cd /Users/jakegearon/projects/SWORD
python -m pytest tests/sword_duckdb/test_column_order.py -k "subnetwork or facc_quality" -v
```

Expected: **FAIL**

---

### Task 6: Update column_order.py

**Files:**
- Modify: `src/sword_duckdb/column_order.py`

**Step 1: In `REACHES_COLUMN_ORDER`, insert `"facc_quality"` immediately after `"facc"` (~line 50)**

```python
    "facc",
    "facc_quality",   # facc correction status flag (v17c)
    "dist_out",
```

**Step 2: In `REACHES_COLUMN_ORDER`, insert `"subnetwork_id"` immediately after `"network"` (~line 63)**

```python
    "network",
    "subnetwork_id",  # weakly connected component ID (v17c)
    "stream_order",
```

**Step 3: In `NODES_COLUMN_ORDER`, insert `"facc_quality"` immediately after `"facc"` (find `"facc"` in the nodes section)**

```python
    "facc",
    "facc_quality",   # facc correction status flag (v17c)
```

**Step 4: In `NODES_COLUMN_ORDER`, insert `"subnetwork_id"` immediately after `"network"` (find `"network"` in the nodes section)**

```python
    "network",
    "subnetwork_id",  # weakly connected component ID (v17c)
```

**Step 5: Run all column_order tests**

```bash
cd /Users/jakegearon/projects/SWORD
python -m pytest tests/sword_duckdb/test_column_order.py -v
```

Expected: **all PASS**

**Step 6: Commit**

```bash
git add src/sword_duckdb/column_order.py tests/sword_duckdb/test_column_order.py
git commit -m "feat: add subnetwork_id and facc_quality to canonical column order (#33)"
```

---

### Task 7: Update test fixture DB and run full test suite

**Files:**
- Modify: `tests/sword_duckdb/fixtures/sword_test_minimal.duckdb` (binary, via script)

**Step 1: Run the fixture creation script to regenerate the test DB with new columns**

```bash
cd /Users/jakegearon/projects/SWORD
python tests/sword_duckdb/fixtures/create_test_db.py
```

If the script doesn't add new columns automatically, run this instead:

```python
import duckdb
from src.sword_duckdb.schema import add_v17c_columns

con = duckdb.connect("tests/sword_duckdb/fixtures/sword_test_minimal.duckdb")
add_v17c_columns(con)
con.close()
```

**Step 2: Run the relevant test files**

```bash
cd /Users/jakegearon/projects/SWORD
python -m pytest tests/sword_duckdb/test_sword_db.py tests/sword_duckdb/test_column_order.py -v
```

Expected: **all PASS**

**Step 3: Commit fixture update**

```bash
git add tests/sword_duckdb/fixtures/sword_test_minimal.duckdb
git commit -m "chore: migrate test fixture DB to include subnetwork_id and facc_quality (#33)"
```

---

### Task 8: Close issue #33

**Step 1: Push branch and open PR**

```bash
cd /Users/jakegearon/projects/SWORD
git push -u origin issue-33-new-columns
```

**Step 2: Create PR targeting v17c-updates**

```bash
gh pr create \
  --title "feat: add subnetwork_id and facc_quality columns to schema (#33)" \
  --body "Closes #33

Adds two missing v17c columns to reaches and nodes:
- \`subnetwork_id INTEGER\`: weakly connected component ID from v17c pipeline
- \`facc_quality VARCHAR\`: facc correction status flag from facc-fix workflow

Both columns added to DDL, migration helper (\`add_v17c_columns\`), and canonical column order. Nodes inherit \`facc_quality\` from parent reach via JOIN UPDATE in migration." \
  --base v17c-updates
```
