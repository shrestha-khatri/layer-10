"""
LAYER10 MEMORY EXPLORER  —  Streamlit Visualization
=====================================================
What this shows:
  1. GRAPH VIEW     — entities as nodes, claims as edges (pyvis)
  2. QUERY PANEL    — ask a question, get grounded context pack
  3. TIMELINE VIEW  — claim diff: how facts changed over time (★ differentiator)
  4. ENTITY PANEL   — inspect entity, all claims, all aliases, merge history
  5. EVIDENCE PANEL — click a claim → see exact source text with highlights

Run with:
  streamlit run src/app.py
"""

import json
import os
import sys
from datetime import datetime
from pathlib import Path

import streamlit as st

sys.path.insert(0, str(Path(__file__).parent))
from store import MemoryGraphStore
from retrieval import RetrievalEngine, EXAMPLE_QUERIES
from schema import ClaimType, EntityType

DB_PATH = str(Path(__file__).parent.parent / "data" / "memory.db")
ARTIFACTS_PATH = str(Path(__file__).parent.parent / "data" / "processed" / "artifacts.json")

# ─────────────────────────────────────────────
# PAGE CONFIG
# ─────────────────────────────────────────────

st.set_page_config(
    page_title="Layer10 Memory Explorer",
    page_icon="🧠",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom CSS — dark, editorial, high-contrast
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=IBM+Plex+Sans:wght@300;400;600;700&display=swap');

html, body, [class*="css"] {
    font-family: 'IBM Plex Sans', sans-serif;
}
.main { background: #0a0a0f; }
.stApp { background: #0a0a0f; color: #e8e8e8; }

h1, h2, h3 { font-family: 'IBM Plex Sans', sans-serif; font-weight: 700; }
h1 { color: #00ff9d; font-size: 1.8rem; letter-spacing: -0.5px; }
h2 { color: #e8e8e8; font-size: 1.2rem; border-bottom: 1px solid #222; padding-bottom: 6px; }
h3 { color: #aaa; font-size: 0.95rem; text-transform: uppercase; letter-spacing: 1px; }

.claim-current {
    background: #0a1a12; border-left: 3px solid #00ff9d;
    padding: 12px 16px; border-radius: 4px; margin: 8px 0;
    font-family: 'IBM Plex Mono', monospace; font-size: 0.82rem;
}
.claim-historical {
    background: #1a1000; border-left: 3px solid #ff6b35;
    padding: 12px 16px; border-radius: 4px; margin: 8px 0;
    font-family: 'IBM Plex Mono', monospace; font-size: 0.82rem;
    opacity: 0.75;
}
.evidence-box {
    background: #111118; border: 1px solid #333; border-radius: 4px;
    padding: 10px 14px; margin: 6px 0;
    font-family: 'IBM Plex Mono', monospace; font-size: 0.78rem;
    color: #aad4ff; white-space: pre-wrap; word-break: break-word;
}
.entity-chip {
    display: inline-block; background: #1a1a2e; border: 1px solid #444;
    border-radius: 12px; padding: 2px 10px; margin: 2px;
    font-size: 0.78rem; color: #ccc;
}
.conflict-row {
    background: #1a0a0a; border-left: 3px solid #ff4444;
    padding: 10px 14px; border-radius: 4px; margin: 6px 0;
    font-family: 'IBM Plex Mono', monospace; font-size: 0.8rem;
}
.timeline-item {
    display: flex; align-items: flex-start; margin: 8px 0;
    font-family: 'IBM Plex Mono', monospace; font-size: 0.82rem;
}
.stat-box {
    background: #111118; border: 1px solid #222; border-radius: 6px;
    padding: 12px; text-align: center;
}
.stat-num { font-size: 1.6rem; font-weight: 700; color: #00ff9d; }
.stat-label { font-size: 0.72rem; color: #666; text-transform: uppercase; letter-spacing: 1px; }
</style>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────────
# DATA LOADING
# ─────────────────────────────────────────────

def load_store():
    return MemoryGraphStore(DB_PATH)

def load_engine():
    return RetrievalEngine(load_store())

@st.cache_data
def load_artifacts():
    if os.path.exists(ARTIFACTS_PATH):
        with open(ARTIFACTS_PATH) as f:
            return json.load(f)
    return []

@st.cache_data
def load_graph_data():
    store = load_store()
    return store.get_graph_data()

def get_entity_name(entity_id: str, graph_data: dict) -> str:
    for e in graph_data["entities"]:
        if e["entity_id"] == entity_id:
            return e["canonical_name"]
    return entity_id[:16] + "..."


# ─────────────────────────────────────────────
# SIDEBAR
# ─────────────────────────────────────────────

with st.sidebar:
    st.markdown("## 🧠 Layer10 Memory")
    st.markdown("*Grounded long-term memory*")
    st.markdown("---")
    
    store = load_store()
    stats = store.stats()
    
    col1, col2 = st.columns(2)
    with col1:
        st.markdown(f'<div class="stat-box"><div class="stat-num">{stats["entities"]}</div><div class="stat-label">Entities</div></div>', unsafe_allow_html=True)
        st.markdown(f'<div class="stat-box"><div class="stat-num">{stats["claims_current"]}</div><div class="stat-label">Current</div></div>', unsafe_allow_html=True)
    with col2:
        st.markdown(f'<div class="stat-box"><div class="stat-num">{stats["artifacts"]}</div><div class="stat-label">Sources</div></div>', unsafe_allow_html=True)
        st.markdown(f'<div class="stat-box"><div class="stat-num">{stats["conflicts"]}</div><div class="stat-label">Changes</div></div>', unsafe_allow_html=True)
    
    st.markdown("---")
    page = st.radio("Navigate", [
        "🔍 Query",
        "🕸 Graph",
        "📅 Timeline",
        "👤 Entities",
        "📄 Sources",
    ])
    st.markdown("---")
    st.markdown("""
    <div style='font-size:0.72rem; color:#555; line-height:1.6;'>
    Corpus: Enron Email Dataset<br>
    Extraction: Rule-based + Claude<br>
    Store: SQLite + FTS5<br>
    Schema: v1 | prompt: v2
    </div>
    """, unsafe_allow_html=True)


# ─────────────────────────────────────────────
# PAGE: QUERY
# ─────────────────────────────────────────────

if page == "🔍 Query":
    st.markdown("# Query Memory")
    st.markdown("*Ask a question — get grounded claims with evidence citations*")
    
    col1, col2 = st.columns([3, 1])
    with col1:
        query = st.text_input("", placeholder="Who owns the Infrastructure Modernization Initiative?", label_visibility="collapsed")
    with col2:
        current_only = st.checkbox("Current only", value=False)
    
    st.markdown("**Example queries:**")
    ex_cols = st.columns(3)
    for i, q in enumerate(EXAMPLE_QUERIES):
        with ex_cols[i % 3]:
            if st.button(q[:45] + "..." if len(q) > 45 else q, key=f"ex_{i}"):
                query = q

    if query:
        engine = load_engine()
        graph_data = load_graph_data()
        pack = engine.query(query, current_only=current_only)

        # Stats row
        s1, s2, s3, s4 = st.columns(4)
        with s1: st.metric("Entities matched", len(pack.entities))
        with s2: st.metric("Claims returned", len(pack.claims))
        with s3: st.metric("Historical claims", sum(1 for c in pack.claims if not c.is_current))
        with s4: st.metric("Conflicts detected", len(pack.conflicts))

        st.markdown("---")

        # Entities matched
        if pack.entities:
            st.markdown("### Entities")
            for e in pack.entities:
                merged = f" · merged {len(e.merged_from)} aliases" if e.merged_from else ""
                st.markdown(f'<span class="entity-chip">🔵 {e.entity_type.value}</span> <b>{e.canonical_name}</b>{merged}', unsafe_allow_html=True)

        st.markdown("---")

        # Claims with evidence
        if pack.claims:
            st.markdown("### Grounded Claims")
            current_claims = [c for c in pack.claims if c.is_current]
            historical_claims = [c for c in pack.claims if not c.is_current]
            
            if current_claims:
                st.markdown("**✅ Current**")
                for claim in current_claims:
                    subject_name = get_entity_name(claim.subject_id, graph_data)
                    obj_name = get_entity_name(claim.object_value, graph_data) if claim.object_is_entity else claim.object_value
                    
                    st.markdown(f'''<div class="claim-current">
<b>{claim.claim_type.value}</b> &nbsp;·&nbsp; conf={claim.confidence:.2f} &nbsp;·&nbsp; since {claim.valid_from.strftime("%Y-%m-%d")}<br>
<span style="color:#aaa">{subject_name}</span> → <span style="color:#00ff9d">{obj_name}</span>
</div>''', unsafe_allow_html=True)
                    
                    if claim.evidence:
                        ev = claim.evidence[0]
                        artifacts_list = load_artifacts()
                        source_subj = next((a.get("subject", "") for a in artifacts_list if a["source_id"] == ev.source_id), "")
                        with st.expander(f"📎 Evidence: [{ev.source_id}] {source_subj[:50]}"):
                            st.markdown(f'<div class="evidence-box">"{ev.excerpt}"\n\nSource: {ev.source_id}\nOffsets: chars {ev.char_start}–{ev.char_end}\nTimestamp: {ev.source_timestamp.strftime("%Y-%m-%d")}\nExtraction: {ev.extraction_version}</div>', unsafe_allow_html=True)
            
            if historical_claims:
                st.markdown("**🕐 Historical (superseded)**")
                for claim in historical_claims:
                    subject_name = get_entity_name(claim.subject_id, graph_data)
                    obj_name = get_entity_name(claim.object_value, graph_data) if claim.object_is_entity else claim.object_value
                    valid_until = claim.valid_to.strftime("%Y-%m-%d") if claim.valid_to else "?"
                    
                    st.markdown(f'''<div class="claim-historical">
<b>{claim.claim_type.value}</b> &nbsp;·&nbsp; <span style="color:#ff6b35">HISTORICAL until {valid_until}</span><br>
<span style="color:#777">{subject_name}</span> → <span style="color:#ff6b35">{obj_name}</span>
</div>''', unsafe_allow_html=True)

        # Conflicts
        if pack.conflicts:
            st.markdown("---")
            st.markdown("### ⚠️ Conflicts / Changes Detected")
            for conflict in pack.conflicts:
                st.markdown(f'''<div class="conflict-row">
<b>{conflict.get("claim_type", "?")} changed</b> at {conflict.get("changed_at", "?")[:10]}<br>
WAS: <span style="color:#ff6b35">"{conflict.get("old_value", "?")}"</span><br>
NOW: <span style="color:#00ff9d">"{conflict.get("new_value", "?")}"</span>
</div>''', unsafe_allow_html=True)


# ─────────────────────────────────────────────
# PAGE: GRAPH
# ─────────────────────────────────────────────

elif page == "🕸 Graph":
    st.markdown("# Memory Graph")
    st.markdown("*Entities and claims as a knowledge graph*")
    
    graph_data = load_graph_data()
    
    try:
        from pyvis.network import Network
        import tempfile

        filter_current = st.checkbox("Show current claims only", value=True)
        claim_type_filter = st.multiselect("Filter by claim type", 
            [ct.value for ct in ClaimType], 
            default=["ASSIGNED_TO", "HAS_STATUS", "HAS_DEADLINE", "DEPENDS_ON"])

        net = Network(height="560px", width="100%", bgcolor="#0a0a0f", font_color="#e8e8e8", directed=True)
        net.set_options("""
        {
          "physics": {"stabilization": {"iterations": 100}, "barnesHut": {"gravitationalConstant": -3000}},
          "edges": {"arrows": {"to": {"enabled": true, "scaleFactor": 0.8}}},
          "nodes": {"borderWidth": 2},
          "interaction": {"hover": true}
        }
        """)

        ENTITY_COLORS = {
            "PERSON": "#4ecdc4", "PROJECT": "#ff6b35", "TEAM": "#a8e6cf",
            "COMPONENT": "#ffd93d", "DECISION": "#c9b1ff", "UNKNOWN": "#888",
        }
        CLAIM_COLORS = {
            "ASSIGNED_TO": "#00ff9d", "HAS_STATUS": "#ff6b35", "HAS_DEADLINE": "#ffd93d",
            "DEPENDS_ON": "#ff4444", "MEMBER_OF": "#4ecdc4", "DECIDED": "#c9b1ff",
            "REPORTED_BY": "#777", "MENTIONED": "#555", "HAS_PRIORITY": "#a8e6cf",
        }

        # Add entity nodes
        added_nodes = set()
        for e in graph_data["entities"]:
            color = ENTITY_COLORS.get(e["entity_type"], "#888")
            aliases = json.loads(e.get("aliases") or "[]")
            merged = json.loads(e.get("merged_from") or "[]")
            title = f"{e['canonical_name']}\nType: {e['entity_type']}\nAliases: {len(aliases)}\nMerged: {len(merged)}"
            net.add_node(e["entity_id"], label=e["canonical_name"][:20],
                        color=color, title=title, size=25 if e["entity_type"] == "PERSON" else 20)
            added_nodes.add(e["entity_id"])

        # Add claim edges
        added_edges = set()
        for c in graph_data["claims"]:
            if filter_current and not c.get("is_current"):
                continue
            if c["claim_type"] not in claim_type_filter:
                continue
            if not c.get("object_is_entity"):
                continue
            if c["subject_id"] not in added_nodes or c["object_value"] not in added_nodes:
                continue
            
            edge_key = f"{c['subject_id']}-{c['claim_type']}-{c['object_value']}"
            if edge_key in added_edges:
                continue
            added_edges.add(edge_key)
            
            color = CLAIM_COLORS.get(c["claim_type"], "#555")
            dash = not c.get("is_current")
            title = f"{c['claim_type']}\nconf={c.get('confidence', 0):.2f}\n{c.get('excerpt', '')[:80]}"
            net.add_edge(c["subject_id"], c["object_value"],
                        label=c["claim_type"].replace("_", " "),
                        color=color, title=title, dashes=dash, width=2)

        with tempfile.NamedTemporaryFile(delete=False, suffix=".html", mode='w') as f:
            net.save_graph(f.name)
            html_content = open(f.name).read()
        
        st.components.v1.html(html_content, height=580)

        # Legend
        st.markdown("**Legend:**")
        cols = st.columns(6)
        for i, (label, color) in enumerate(ENTITY_COLORS.items()):
            with cols[i % 6]:
                st.markdown(f'<span style="color:{color}">●</span> {label}', unsafe_allow_html=True)

    except ImportError:
        st.warning("pyvis not installed. Showing tabular graph view instead.")
        
        st.markdown("### Entities")
        for e in graph_data["entities"]:
            aliases = json.loads(e.get("aliases") or "[]")
            st.markdown(f"**{e['canonical_name']}** [{e['entity_type']}] — {len(aliases)} aliases")
        
        st.markdown("### Claims (current)")
        for c in [c for c in graph_data["claims"] if c.get("is_current")]:
            st.markdown(f"`{c['claim_type']}` {c['subject_id'][:16]} → {c['object_value'][:30]} (conf={c.get('confidence',0):.2f})")


# ─────────────────────────────────────────────
# PAGE: TIMELINE  ★ THE DIFFERENTIATOR
# ─────────────────────────────────────────────

elif page == "📅 Timeline":
    st.markdown("# Claim Timeline")
    st.markdown("*How facts changed over time — the audit trail of organizational memory*")

    graph_data = load_graph_data()
    entity_names = {e["entity_id"]: e["canonical_name"] for e in graph_data["entities"]}
    
    # Entity selector
    entity_options = {e["canonical_name"]: e["entity_id"] for e in graph_data["entities"]}
    selected_entity_name = st.selectbox("Select entity", list(entity_options.keys()))
    selected_entity_id = entity_options[selected_entity_name]

    claim_type_options = ["ALL"] + [ct.value for ct in ClaimType]
    selected_type = st.selectbox("Claim type", claim_type_options)
    
    store = load_store()
    timeline = store.get_timeline(
        selected_entity_id,
        None if selected_type == "ALL" else selected_type
    )

    if not timeline:
        st.info("No timeline data for this entity.")
    else:
        st.markdown(f"### Timeline for **{selected_entity_name}**")
        
        # Group by claim type
        by_type = {}
        for item in timeline:
            ct = item["claim_type"]
            if ct not in by_type:
                by_type[ct] = []
            by_type[ct].append(item)

        for claim_type, items in by_type.items():
            st.markdown(f"#### {claim_type}")
            
            for i, item in enumerate(items):
                is_current = item["is_current"]
                obj = entity_names.get(item["object_value"], item["object_value"])
                
                if is_current:
                    icon = "✅"
                    color = "#00ff9d"
                    status = "CURRENT"
                else:
                    icon = "🕐"
                    color = "#ff6b35"
                    valid_until = item.get("valid_to", "?")
                    status = f"until {valid_until[:10] if valid_until else '?'}"
                
                # Draw timeline connector
                if i < len(items) - 1:
                    connector = "│"
                else:
                    connector = " "
                
                st.markdown(f'''
<div style="display:flex; align-items:flex-start; margin:4px 0;">
  <div style="width:100px; color:#555; font-size:0.75rem; padding-top:3px; font-family:monospace;">
    {item["valid_from"][:10]}
  </div>
  <div style="width:24px; text-align:center; font-size:1.1rem;">{icon}</div>
  <div style="flex:1; background:#111118; border-left:2px solid {color}; 
              padding:8px 12px; border-radius:0 4px 4px 0; margin-left:4px;">
    <span style="color:{color}; font-weight:600;">{obj}</span>
    <span style="color:#555; font-size:0.75rem; margin-left:8px;">[{status}]</span>
    <div style="color:#777; font-size:0.75rem; font-family:monospace; margin-top:4px;">
      "{item.get("excerpt", "")[:80]}"
    </div>
    <div style="color:#444; font-size:0.7rem; margin-top:2px;">
      📎 {item.get("source_id", "")} · conf={item.get("confidence", 0):.2f}
    </div>
  </div>
</div>
''', unsafe_allow_html=True)
        
        # Conflict summary
        conflicts = store.get_conflicts(selected_entity_id)
        if conflicts:
            st.markdown("---")
            st.markdown("### Change Log")
            for c in conflicts:
                ct = c.get("claim_type", "?")
                old = c.get("old_value", "?")
                new = c.get("new_value", "?")
                at = (c.get("changed_at") or "?")[:10]
                st.markdown(f'<div class="conflict-row"><b>{ct}</b> changed at {at}<br>WAS: <span style="color:#ff6b35">"{old}"</span> → NOW: <span style="color:#00ff9d">"{new}"</span></div>', unsafe_allow_html=True)


# ─────────────────────────────────────────────
# PAGE: ENTITIES
# ─────────────────────────────────────────────

elif page == "👤 Entities":
    st.markdown("# Entities")
    
    graph_data = load_graph_data()
    store = load_store()
    
    # Filter controls
    col1, col2 = st.columns([2, 1])
    with col1:
        search = st.text_input("Search entities", placeholder="Kenneth Lay...")
    with col2:
        type_filter = st.selectbox("Type", ["ALL"] + [et.value for et in EntityType])
    
    entities = graph_data["entities"]
    if search:
        entities = [e for e in entities if search.lower() in e["canonical_name"].lower()]
    if type_filter != "ALL":
        entities = [e for e in entities if e["entity_type"] == type_filter]
    
    for e in entities:
        aliases = json.loads(e.get("aliases") or "[]")
        merged_from = json.loads(e.get("merged_from") or "[]")
        
        with st.expander(f"{'👤' if e['entity_type']=='PERSON' else '📁' if e['entity_type']=='PROJECT' else '👥'} {e['canonical_name']} [{e['entity_type']}]"):
            col1, col2 = st.columns(2)
            with col1:
                st.markdown(f"**Entity ID:** `{e['entity_id']}`")
                st.markdown(f"**First seen:** {e['first_seen'][:10]}")
                st.markdown(f"**Last seen:** {e['last_seen'][:10]}")
                if e.get("email_addresses"):
                    emails = json.loads(e.get("email_addresses") or "[]")
                    st.markdown(f"**Emails:** {', '.join(emails)}")
            
            with col2:
                if aliases:
                    st.markdown("**Aliases:**")
                    for a in aliases[:8]:
                        st.markdown(f'<span class="entity-chip">{a}</span>', unsafe_allow_html=True)
                
                if merged_from:
                    st.markdown(f"**Merged from:** {len(merged_from)} entity IDs")
                    for mid in merged_from:
                        st.markdown(f'<span class="entity-chip" style="border-color:#ff6b35;">{mid}</span>', unsafe_allow_html=True)
            
            # Current claims
            claims = store.get_claims_for_entity(e["entity_id"], current_only=True)
            if claims:
                st.markdown("**Current claims:**")
                for claim in claims[:5]:
                    ev = claim.evidence[0] if claim.evidence else None
                    citation = f"[{ev.source_id}:{ev.char_start}-{ev.char_end}]" if ev else ""
                    st.markdown(f'<div class="claim-current"><b>{claim.claim_type.value}</b> → {claim.object_value[:40]} · {citation}</div>', unsafe_allow_html=True)


# ─────────────────────────────────────────────
# PAGE: SOURCES
# ─────────────────────────────────────────────

elif page == "📄 Sources":
    st.markdown("# Source Artifacts")
    st.markdown("*The immutable source record — never modified after ingestion*")

    artifacts = load_artifacts()
    store = load_store()
    graph_data = load_graph_data()
    entity_names = {e["entity_id"]: e["canonical_name"] for e in graph_data["entities"]}

    # Filter
    col1, col2 = st.columns([3, 1])
    with col1:
        search = st.text_input("Search emails", placeholder="pipeline migration...")
    with col2:
        show_dups = st.checkbox("Show duplicates", value=False)

    filtered = artifacts
    if not show_dups:
        filtered = [a for a in artifacts if not a.get("is_duplicate")]
    if search:
        filtered = [a for a in filtered 
                   if search.lower() in a.get("body", "").lower() 
                   or search.lower() in (a.get("subject") or "").lower()]

    st.markdown(f"*Showing {len(filtered)} of {len(artifacts)} artifacts*")

    for a in filtered:
        dup_badge = ' 🔁 DUPLICATE' if a.get("is_duplicate") else ''
        with st.expander(f"📧 {a['timestamp'][:10]} · {a.get('subject', '(no subject)')}{dup_badge}"):
            col1, col2 = st.columns([2, 1])
            with col1:
                st.markdown(f"**From:** {a.get('sender', '?')}")
                st.markdown(f"**To:** {', '.join(a.get('recipients', []))}")
                st.markdown(f"**Thread:** {a.get('thread_id', '?')}")
                if a.get("is_duplicate"):
                    st.warning(f"Duplicate of: {a.get('duplicate_of', '?')}")
            with col2:
                st.markdown(f"**Source ID:** `{a['source_id']}`")
                st.markdown(f"**Ingested:** {a.get('ingested_at', '?')[:16]}")
            
            st.markdown("**Body:**")
            st.code(a.get("body", ""), language=None)
            
            # Claims derived from this artifact
            derived = [c for c in graph_data["claims"] 
                      if c.get("source_id") == a["source_id"]]
            if derived:
                st.markdown("**Claims derived from this artifact:**")
                for c in derived[:6]:
                    status = "✅" if c.get("is_current") else "🕐"
                    subj = entity_names.get(c["subject_id"], c["subject_id"][:16])
                    st.markdown(f"{status} `{c['claim_type']}` {subj} → {c['object_value'][:30]}")