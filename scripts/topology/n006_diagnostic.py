#!/usr/bin/env python3
"""N006 diagnostic - final version"""
import duckdb

DB_PATH = "data/duckdb/sword_v17c.duckdb"
con = duckdb.connect(DB_PATH)

total_down = con.execute("SELECT COUNT(*) FROM reach_topology WHERE direction = 'down'").fetchone()[0]
n_violations = con.execute("""
WITH rb AS (SELECT reach_id, region, MAX(dist_out) as mdo FROM nodes WHERE dist_out IS NOT NULL AND dist_out != -9999 GROUP BY reach_id, region)
SELECT COUNT(*) FROM reach_topology rt
JOIN rb b_up ON rt.reach_id = b_up.reach_id AND rt.region = b_up.region
JOIN rb b_dn ON rt.neighbor_reach_id = b_dn.reach_id AND rt.region = b_dn.region
JOIN reaches r_dn ON rt.neighbor_reach_id = r_dn.reach_id AND rt.region = r_dn.region
WHERE rt.direction = 'down' AND ABS(b_dn.mdo + r_dn.reach_length - b_up.mdo) > 1000.0
""").fetchone()[0]

print("="*80)
print("N006 DIAGNOSTIC - COMPLETE ANALYSIS")
print("="*80)
print("\nTopology: %d downstream links, %d violations (%.2f%%)" % (total_down, n_violations, 100*n_violations/total_down))

print("\n" + "-"*80)
print("TOP 10 LARGEST GAPS: ")
print("Upstream       Downstream      Region   Gap (m)")
print("-"*65)
top = con.execute("""
SELECT rt.reach_id, rt.neighbor_reach_id, rt.region, 
       ABS(b_dn.mdo + r_dn.reach_length - b_up.mdo) as gap
FROM reach_topology rt
JOIN (SELECT reach_id, region, MAX(dist_out) as mdo 
      FROM nodes WHERE dist_out IS NOT NULL AND dist_out != -9999 
      GROUP BY reach_id, region) b_up 
  ON rt.reach_id = b_up.reach_id AND rt.region = b_up.region
JOIN (SELECT reach_id, region, MAX(dist_out) as mdo 
      FROM nodes WHERE dist_out IS NOT NULL AND dist_out != -9999 
      GROUP BY reach_id, region) b_dn 
  ON rt.neighbor_reach_id = b_dn.reach_id AND rt.region = b_dn.region
JOIN reaches r_dn ON rt.neighbor_reach_id = r_dn.reach_id AND rt.region = r_dn.region
WHERE rt.direction = 'down' AND ABS(b_dn.mdo + r_dn.reach_length - b_up.mdo) > 1000.0
ORDER BY gap DESC LIMIT 10
""").fetchdf()
for row in top.to_dict('records'):
    print("%15d %15d %10s %12.1f" % (row['reach_id'], row['neighbor_reach_id'], row['region'], row['gap']))

print("\n" + "-"*80)
print("BIFURCATION CLUSTERING: ")
bif = con.execute("""
WITH rb AS (SELECT reach_id, region, MAX(dist_out) as mdo FROM nodes WHERE dist_out IS NOT NULL AND dist_out != -9999 GROUP BY reach_id, region),
     bg AS (SELECT rt.reach_id, rt.neighbor_reach_id, rt.region,
                  ABS(b_dn.mdo + r_dn.reach_length - b_up.mdo) as gap
           FROM reach_topology rt
           JOIN rb b_up ON rt.reach_id = b_up.reach_id AND rt.region = b_up.region
           JOIN rb b_dn ON rt.neighbor_reach_id = b_dn.reach_id AND rt.region = b_dn.region
           JOIN reaches r_dn ON rt.neighbor_reach_id = r_dn.reach_id AND rt.region = r_dn.region
           WHERE rt.direction = 'down' AND ABS(b_dn.mdo + r_dn.reach_length - b_up.mdo) > 1000.0)
SELECT SUM(CASE WHEN (SELECT COUNT(*) FROM reach_topology rt2 WHERE rt2.reach_id = bg.reach_id AND rt2.direction='down') > 1 
                 OR (SELECT COUNT(*) FROM reach_topology rt2 WHERE rt2.reach_id = bg.reach_id AND rt2.direction='up') > 1 
            THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as bif_pct
FROM bg
""").fetchdf().iloc[0]['bif_pct']
print("  %.2f%% of violations occur at bifurcations/confluences" % bif)

print("\n" + "-"*80)
print("VIOLATIONS BY REGION: ")
print("-"*80)
reg = con.execute("""
SELECT region, COUNT(*) as cnt FROM (
    SELECT rt.region FROM reach_topology rt
    JOIN (SELECT reach_id, region, MAX(dist_out) as mdo 
          FROM nodes WHERE dist_out IS NOT NULL AND dist_out != -9999 
          GROUP BY reach_id, region) b_up 
      ON rt.reach_id = b_up.reach_id AND rt.region = b_up.region
    JOIN (SELECT reach_id, region, MAX(dist_out) as mdo 
          FROM nodes WHERE dist_out IS NOT NULL AND dist_out != -9999 
          GROUP BY reach_id, region) b_dn 
      ON rt.neighbor_reach_id = b_dn.reach_id AND rt.region = b_dn.region
    WHERE rt.direction = 'down' AND 
          ABS(b_dn.mdo + (SELECT reach_length FROM reaches WHERE reach_id = rt.neighbor_reach_id AND region = rt.region) - b_up.mdo) > 1000.0
) x GROUP BY region ORDER BY cnt DESC
""").fetchdf()
for row in reg.to_dict('records'):
    print("  %s: %d violations (%.1f%%)" % (row['region'], row['cnt'], 100*row['cnt']/n_violations))

print("\n" + "="*80)
print("CONCLUSION & RECOMMENDATION: ")
print("="*80)
avg_gap = top['gap'].mean()
print("1. %d topological gaps = %.1f%% of all downstream links!" % (n_violations, 100*n_violations/total_down))
print("2. Largest gap: %.1f km - IMPOSSIBLE to be rounding error" % (top.iloc[0]['gap']/1000))
print("3. Only %.1f%% at bifurcations - most violations are SINGLE LINKS" % bif)
print("4. This suggests MISSING REACHES rather than bad recalculation.")
print("5. RECOMMENDATION: ")
print("   - DO NOT just re-accumulate dist_out (will inherit gaps)")
print("   - INVESTIGATE top 10 gaps - likely missing topology edges")
print("   - Run network analysis to find disconnected components")
print("   - Consider running ReconstructionEngine to rebuild topology")
print("="*80)
con.close()
