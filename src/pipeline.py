"""
PIPELINE RUNNER  —  end-to-end build
======================================
Run this once to build the memory graph from scratch.
Safe to re-run (idempotent).

Usage:
  python src/pipeline.py              # rule-based extraction (no API key needed)
  python src/pipeline.py --llm        # use Gemini Flash (requires GEMINI_API_KEY)

For Gemini mode, set your key first:
  Windows:   set GEMINI_API_KEY=your_key_here
  Mac/Linux: export GEMINI_API_KEY=your_key_here
  Or paste key directly in extractor.py line: GEMINI_API_KEY = "your_key_here"
"""

import json
import os
import sys
from datetime import datetime
from pathlib import Path

# Fix imports — works regardless of where you launch from
sys.path.insert(0, str(Path(__file__).parent))

from loader import EnronLoader
from extractor import extract_rule_based, Extractor
from dedup import run_dedup
from store import MemoryGraphStore
from retrieval import RetrievalEngine, EXAMPLE_QUERIES

# Relative paths — no hardcoded Windows paths
BASE_DIR       = Path(__file__).parent.parent
DB_PATH        = str(BASE_DIR / "data" / "memory.db")
ARTIFACTS_PATH = str(BASE_DIR / "data" / "processed" / "artifacts.json")
OUTPUT_PATH    = str(BASE_DIR / "outputs")


def run_pipeline(use_llm: bool = False):
    mode = "GPT extraction" if use_llm else "Rule-based"    
    print("=" * 60)
    print("         LAYER10 MEMORY PIPELINE")
    print(f"  Mode: {mode} extraction")
    print(f"  Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    os.makedirs(OUTPUT_PATH, exist_ok=True)
    os.makedirs(str(BASE_DIR / "data" / "processed"), exist_ok=True)

    # ── Step 1: Load corpus ──────────────────────────────────
    print("\n[1/5] Loading corpus...")
    loader = EnronLoader()
    artifacts = loader.load_synthetic()
    loader.save(ARTIFACTS_PATH)

    non_dup = [a for a in artifacts if not a.is_duplicate]
    print(f"  → {len(artifacts)} total, {len(non_dup)} unique, {len(artifacts)-len(non_dup)} duplicates")

    # ── Step 2: Extract entities + claims ───────────────────
        # ── Step 2: Extract entities + claims ───────────────────
    print("\n[2/5] Extracting entities and claims...")
    all_entities, all_claims = [], []

    if use_llm:
        try:
            from extractor import client
        except Exception:
            print("  ⚠ LLM client not available — falling back to rule-based")
            use_llm = False

    extractor = Extractor() if use_llm else None

    for artifact in non_dup:
        if use_llm:
            ents, clms = extractor.extract(artifact)
        else:
            ents, clms = extract_rule_based(artifact)

        all_entities.extend(ents)
        all_claims.extend(clms)

    print(f"  → {len(all_entities)} raw entity mentions, {len(all_claims)} raw claims")
    # ── Step 3: Dedup + canonicalize ────────────────────────
    print("\n[3/5] Deduplicating and canonicalizing...")
    canonical_entities, canonical_claims, merge_records, conflicts = run_dedup(all_entities, all_claims)
    print(f"  → {len(canonical_entities)} canonical entities (from {len(all_entities)} mentions)")
    print(f"  → {len(merge_records)} merges performed")
    print(f"  → {len(canonical_claims)} canonical claims (from {len(all_claims)} raw)")
    print(f"  → {len(conflicts)} conflicts/supersessions detected")

    # ── Step 4: Build graph store ────────────────────────────
    print(f"\n[4/5] Building graph store...")
    print(f"  Ingesting into: {DB_PATH}")
    if os.path.exists(DB_PATH):
        try:
            os.remove(DB_PATH)
        except PermissionError:
            print("  ⚠ DB file is locked (close Streamlit first), using existing DB")

    store = MemoryGraphStore(DB_PATH)
    store.ingest_all(artifacts, canonical_entities, canonical_claims, merge_records, conflicts)

    stats = store.stats()
    print("\n  Graph stats:")
    for k, v in stats.items():
        print(f"    {k:25s}: {v}")

    # ── Step 5: Run example queries + save outputs ───────────
    print("\n[5/5] Running example retrieval queries...")
    engine = RetrievalEngine(store)
    context_packs = []

    for q in EXAMPLE_QUERIES:
        pack = engine.query(q)
        formatted = engine.format_context_pack(pack)
        context_packs.append({
            "query": q,
            "formatted": formatted,
            "entity_count": len(pack.entities),
            "claim_count": len(pack.claims),
            "conflict_count": len(pack.conflicts),
        })
        print(f"\n  Q: {q}")
        print(f"     → {len(pack.entities)} entities, {len(pack.claims)} claims, {len(pack.conflicts)} conflicts")

    packs_path = os.path.join(OUTPUT_PATH, "context_packs.json")
    with open(packs_path, 'w') as f:
        json.dump(context_packs, f, indent=2)
    print(f"\n  Saved context packs → {packs_path}")

    graph_path = os.path.join(OUTPUT_PATH, "graph_snapshot.json")
    graph_data = store.get_graph_data()
    with open(graph_path, 'w') as f:
        json.dump(graph_data, f, indent=2, default=str)
    print(f"  Saved graph snapshot → {graph_path}")

    merges_path = os.path.join(OUTPUT_PATH, "merge_log.json")
    with open(merges_path, 'w') as f:
        json.dump([m.to_dict() for m in merge_records], f, indent=2)
    print(f"  Saved merge log → {merges_path}")

    store.close()

    print("\n" + "=" * 60)
    print("✅  PIPELINE COMPLETE")
    print(f"   DB:            {DB_PATH}")
    print(f"   Context packs: {packs_path}")
    print(f"   Graph data:    {graph_path}")
    print("=" * 60)
    print("\nNext: run the visualization")
    print("  streamlit run src/app.py")
    print("=" * 60)


if __name__ == "__main__":
    use_llm = "--llm" in sys.argv
    run_pipeline(use_llm=use_llm)
