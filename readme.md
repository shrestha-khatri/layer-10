# Layer10 Memory Graph

Grounded Long-Term Organizational Memory via Structured Extraction, Deduplication, and Context Graphs.

This project implements a prototype system that converts unstructured organizational communication (emails) into a **grounded memory graph**. The system extracts entities and claims from messages, deduplicates them, and stores them as a **time-aware graph of facts supported by evidence**.

The goal is to simulate how Layer10 could build **durable organizational memory** from sources like email, Slack, and issue trackers.

---

# System Overview

The pipeline converts raw communication artifacts into a queryable knowledge graph.

Pipeline flow:

Raw Emails
→ Artifact Ingestion
→ Structured Extraction
→ Deduplication & Canonicalization
→ Memory Graph Construction
→ Retrieval Engine
→ Visualization UI

The resulting system allows users to:

• Explore entities (people, teams, projects)
• Inspect grounded claims with evidence
• Query the memory graph using natural language
• Track how facts change over time

---

# Key Features

## Grounded Claims

Each extracted claim is linked to its **supporting evidence** from the original artifact.

Example:

ASSIGNED_TO
Louise Kitchen → Infrastructure Modernization Initiative

Confidence: 0.92
Since: 2001-11-05

Evidence: email_1d4494bfb3

Every memory item is traceable to its source.

---

## Memory Graph

Core objects stored in the system:

Entities
People, teams, projects, systems

Claims
Relationships between entities with timestamps and confidence

Evidence
Email excerpts with source identifiers

Artifacts
Immutable source records (emails)

This structure allows the system to maintain **long-term correctness and traceability**.

---

## Retrieval System

The query engine maps natural language questions to graph entities and claims.

Example queries:

• Who owns the Infrastructure Modernization Initiative?
• What is the current status of the pipeline migration?
• Who is part of the EES team?
• What decisions involved Kenneth Lay?

Returned answers always include **evidence citations**.

---

## Timeline View

The system tracks how facts evolve over time.

Example:

Jeff Skilling
MEMBER_OF
Infrastructure Modernization Initiative
2001-11-02

This provides an **audit trail of organizational decisions** and historical changes.

---

## Visualization

The UI includes multiple views:

Graph View
Explore relationships between entities

Grounded Claims
Inspect extracted facts and evidence

Query Memory
Ask questions and retrieve grounded answers

Timeline
View historical changes to claims

Source Artifacts
Browse original emails

---

# Dataset

This project uses a **subset of the Enron Email Dataset**.

The Enron dataset contains real corporate emails from the Enron corporation and is widely used for research in natural language processing and organizational communication analysis.

---

# Dataset Download Instructions

1. Download the Enron Email Dataset.

Option A (Kaggle mirror)

https://www.kaggle.com/datasets/wcukierski/enron-email-dataset

Option B (Original CMU release)

https://www.cs.cmu.edu/~enron/

2. Extract the dataset.

3. Place email files inside the project directory:

data/raw_emails/

Example structure:

data/
raw_emails/
email_001.txt
email_002.txt
email_003.txt

4. Run the ingestion pipeline to process the dataset.

---

# Installation

Clone the repository.

git clone https://github.com/yourusername/layer10-memory.git

cd layer10-memory

Create a virtual environment.

python -m venv venv

Activate the environment.

Mac / Linux

source venv/bin/activate

Windows

venv\Scripts\activate

Install dependencies.

pip install -r requirements.txt

---

# Running the Pipeline

Step 1 — Ingest raw artifacts

python pipeline/ingest.py

Step 2 — Extract entities and claims

python pipeline/extract_claims.py

Step 3 — Deduplicate entities and claims

python pipeline/deduplicate.py

Step 4 — Build the memory graph

python pipeline/build_graph.py

---

# Launch Visualization

Run the Streamlit interface.

streamlit run app.py

The web interface will open automatically in your browser.

---

# Example Queries

Who owns the Infrastructure Modernization Initiative?

What is the current status of the pipeline migration?

Who worked with Andrew Fastow?

What decisions involved Kenneth Lay?

---

# Project Structure

layer10-memory/

pipeline/
 ingest.py
 extract_claims.py
 deduplicate.py
 build_graph.py

retrieval/
 query_engine.py

visualization/
 graph_view.py
 timeline.py

data/
 raw_emails/
 processed/

outputs/
 entities.json
 claims.json
 graph.json

app.py
requirements.txt
README.md

---

# Design Principles

Grounded Memory
Every claim must be traceable to a source artifact.

Immutable Artifacts
Raw emails are never modified after ingestion.

Time-aware Claims
The system records when facts become valid and when they change.

Reproducibility
The full pipeline can be rerun from raw corpus to visualization.

---

# Future Improvements

Embedding-based retrieval for better semantic matching

Improved entity canonicalization and alias detection

Incremental ingestion for real-time updates

Integration with Slack, Jira, and internal documentation systems

---

# Adapting to Layer10's Environment

In production environments the same architecture could extend to:

Email systems
Slack or Teams conversations
Jira or Linear issues
Internal documentation platforms

These sources would produce additional artifact types while maintaining the same **claim + evidence grounding model**.

---

# License

This project is intended for research and educational purposes.
