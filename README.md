# NYC Building Risk Intelligence

NYC Building Risk Intelligence is an end-to-end Data Engineering platform that combines multiple New York City datasets to create a unified risk view for buildings and properties.

The platform ingests operational and property data, cleans and standardizes it through Bronze, Silver, and Gold layers, resolves buildings across heterogeneous source systems, calculates building- and property-level risk scores, and publishes searchable results to Elasticsearch for downstream analytics and visualization.

The project is designed as a practical Data Engineering pipeline using Apache Airflow, Apache Spark, MinIO, Elasticsearch, Kibana, Docker, and Python.

---

## Project Goal

NYC building information is distributed across several datasets that use different identifiers, addresses, coordinates, and data structures.

The goal of this project is to combine these sources into a single analytical model capable of answering questions such as:

* How risky is a specific building?
* How risky is a property containing one or more buildings?
* Which recent complaints or violations contribute to the risk?
* Can a building be identified even when its BBL has changed or is missing?
* Can users search for a property or building by address and retrieve its current risk information?

The system creates two main analytical entities:

| Entity   | Description                                   |
| -------- | --------------------------------------------- |
| Building | Physical building identified primarily by BIN |
| Property | NYC tax lot identified by BBL                 |

Risk is calculated independently at the building level and property level and then made available through Elasticsearch.

---

# Architecture

```text
NYC Data Sources
      |
      v
+----------------+
|     BRONZE     |
| Raw JSON data  |
|     MinIO      |
+----------------+
      |
      v
+----------------+
|     SILVER     |
| Cleaned and    |
| standardized   |
| Spark / Parquet|
+----------------+
      |
      v
+-------------------------+
|   BUILDING IDENTITY     |
| BIN / BBL / Address     |
| resolution              |
+-------------------------+
      |
      v
+----------------+
|      GOLD      |
| Dimensions     |
| Bridge         |
| Facts          |
+----------------+
      |
      v
+-------------------------+
|     RISK MODELS         |
| Building Risk           |
| Property Risk           |
+-------------------------+
      |
      v
+----------------+
| SERVING LAYER  |
| Search-ready   |
| documents      |
+----------------+
      |
      v
+----------------+
| Elasticsearch  |
+----------------+
      |
      v
+----------------+
| Kibana / API   |
+----------------+
```

Apache Airflow orchestrates the pipelines. Spark jobs are executed in the project Spark environment and read/write data through the S3A interface to MinIO.

---

# Technology Stack

| Technology              | Role                                                  |
| ----------------------- | ----------------------------------------------------- |
| Python                  | Ingestion, pipeline logic and application code        |
| Apache Spark 3.4.0      | Distributed transformations and analytical processing |
| Apache Airflow          | Pipeline orchestration                                |
| MinIO                   | S3-compatible Data Lake                               |
| Elasticsearch 8.15.3    | Search and serving layer                              |
| Kibana                  | Visualization and analytical exploration              |
| Docker / Docker Compose | Local infrastructure                                  |
| Git / GitHub            | Version control                                       |
| NYC Open Data           | 311, HPD and DOB source data                          |
| PLUTO                   | NYC property and land-use reference data              |

---

# Data Sources

| Source         | Purpose                                                          |
| -------------- | ---------------------------------------------------------------- |
| NYC 311        | Complaints associated with buildings and properties              |
| HPD Violations | Housing Preservation and Development violations                  |
| DOB Violations | Department of Buildings violations                               |
| PLUTO          | Property, BBL, land use, building age and geographic information |

Each operational source is ingested independently and stored in the Bronze layer before transformation.

---

# Data Lake Layers

## Bronze

The Bronze layer stores the original source records with minimal modification.

Examples:

```text
bronze/311/
bronze/hpd_violations/
bronze/dob_violations/
bronze/pluto/
```

The Bronze layer provides reproducibility and allows Silver datasets to be rebuilt without calling the external APIs again.

---

## Silver

The Silver layer performs data cleaning, normalization, deduplication, date conversion, identifier normalization and geographic preparation.

Validated Silver results:

| Dataset    |  Bronze |  Silver | Duplicates Removed |
| ---------- | ------: | ------: | -----------------: |
| NYC 311    | 926,348 | 897,894 |             28,454 |
| HPD        | 966,224 | 939,343 |             26,881 |
| DOB        | 149,574 | 148,876 |                698 |
| PLUTO 26v2 | 858,284 | 858,284 |    0 duplicate BBL |

### NYC 311

Final Silver records:

```text
897,894
```

Data Quality results:

```text
Missing unique_key:   0
Missing created_date: 0
Missing BBL:          2,728
Missing coordinates: 42
```

The Bronze-to-Silver reconciliation is exact:

```text
926,348 - 28,454 = 897,894
```

---

### HPD

Final Silver records:

```text
939,343
```

Data Quality results:

```text
Missing violationid:    0
Missing inspectiondate: 0
Missing BBL:            0
Missing BIN:            813
Missing coordinates:    126

BBL from source:        938,521
BBL constructed:        822
BBL unresolved:         0
```

---

### DOB

Final Silver records:

```text
148,876
```

Data Quality results:

```text
Missing violation_number:     0
Missing violation_issue_date: 0
Missing BBL:                  137
Missing BIN:                  0
Missing coordinates:          287

BBL from source:              148,084
BBL constructed:              655
BBL unresolved:               137
```

The reconciliation is exact:

```text
149,574 - 698 = 148,876
```

---

### PLUTO

Validated snapshot:

```text
26v2
```

Final records:

```text
858,284
```

Data Quality results:

```text
Missing BBL:                 0
Distinct BBL:                858,284
Duplicate BBL:               0
BBL unresolved:              0
Missing coordinates:         937
Missing / unknown yearbuilt: 40,317
```

All PLUTO BBL values are unique.

---

# Building Identity Resolution

One of the main engineering challenges in the project is linking buildings to current NYC properties.

Buildings may contain historical BBL values, changed property assignments or inconsistent address information.

The Building Identity pipeline therefore resolves buildings through several matching strategies:

```text
DIRECT_BBL
BBL_HISTORY
EXACT_ADDRESS
ELASTICSEARCH
```

Validated result:

| Metric               |   Count |
| -------------------- | ------: |
| Canonical buildings  | 198,131 |
| Resolved buildings   | 197,633 |
| Unresolved buildings |     498 |
| Resolution coverage  |  99.75% |

Resolution methods:

| Method        | Buildings |
| ------------- | --------: |
| DIRECT_BBL    |   197,419 |
| BBL_HISTORY   |        51 |
| EXACT_ADDRESS |       154 |
| ELASTICSEARCH |         9 |
| UNRESOLVED    |       498 |

The final Building Identity dataset is stored at:

```text
s3a://nyc-building-risk/gold/building_identity/final
```

---

# Gold Data Model

The Gold layer transforms cleaned operational datasets into an analytics-ready dimensional model.

## Dimensions

### dim_property

```text
Rows:                 858,284
Distinct property_id: 858,284
Distinct BBL:         858,284
```

### dim_building

```text
Rows:                 198,131
Distinct building_id: 198,131
```

Buildings linked to properties:

```text
197,633
```

Buildings without resolved property:

```text
498
```

---

## Property-Building Bridge

The bridge represents resolved relationships between buildings and properties.

```text
Bridge rows:                197,633
Distinct pairs:             197,633
Distinct buildings:         197,633
Distinct properties:        171,734

Property FK orphans:        0
Building FK orphans:        0
dim_building property
orphans:                    0
```

The bridge therefore passed referential-integrity validation.

---

# Gold Facts

Three event-level fact tables are produced.

| Fact               |    Rows | Distinct Event IDs | Null Event IDs | Building FK Orphans | Property FK Orphans |
| ------------------ | ------: | -----------------: | -------------: | ------------------: | ------------------: |
| fact_311_event     | 897,894 |            897,894 |              0 |                   0 |                   0 |
| fact_hpd_violation | 939,343 |            939,343 |              0 |                   0 |                   0 |
| fact_dob_violation | 148,876 |            148,876 |              0 |                   0 |                   0 |

### 311 Resolution

311 records required additional entity resolution because not every complaint contained a directly usable building identifier.

Validated results:

```text
Total 311 records:             897,894

UNIQUE_BBL:                    753,872
BBL_EXACT_ADDRESS:              97,156
EXACT_ADDRESS_BOROUGH:             446
GEO:                              3,814
Still unresolved:               42,606

Resolved to building_id:       855,288
Resolved to property_id:       894,353

Building FK orphans:                 0
Property FK orphans:                 0
```

---

# Building Risk Model

The Building Risk model combines signals from:

```text
311 complaints
HPD violations
DOB violations
Violation severity
Violation recency
Building age
```

The model produces:

```text
building_risk_score
building_risk_level
```

and keeps the underlying component scores so the final risk can be explained rather than treated as a black-box result.

The validated Building serving population contains:

```text
198,131 buildings
```

---

# Property Risk Model

Property Risk combines building-level risk with property-level events.

The current strategy is:

```text
MAX(
    max_building_risk_score,
    property_only_risk_score
)
```

The validated model contains:

```text
Total properties:     858,284
Properties with risk: 184,973
Properties NO_DATA:   673,311
```

Risk score statistics for properties with risk:

```text
Minimum: 0.06
Maximum: 100
Average: 21.55
```

Risk distribution:

| Risk Level | Properties |
| ---------- | ---------: |
| CRITICAL   |      1,881 |
| HIGH       |      7,197 |
| MEDIUM     |     36,362 |
| LOW        |    139,533 |
| NO_DATA    |    673,311 |

Current thresholds:

```text
MEDIUM   >= 24.80
HIGH     >= 59.87
CRITICAL >= 79.63
```

Risk source distribution:

| Source                        | Properties |
| ----------------------------- | ---------: |
| NO_RISK_DATA                  |    673,311 |
| BUILDING_ONLY                 |    171,208 |
| PROPERTY_EVENTS_ONLY          |     13,239 |
| BUILDING_PLUS_PROPERTY_EVENTS |        526 |

---

# Serving Layer

The Serving Layer denormalizes analytical outputs into structures optimized for search and applications.

Two primary entity types are published:

```text
BUILDING
PROPERTY
```

Serving documents include identifiers, addresses, coordinates, risk scores, risk levels, model metadata and relevant analytical features.

Example metadata fields include:

```text
risk_as_of_date
entity_type
search_address
serving_model_version
serving_calculated_at
elasticsearch_model_version
```

---

# Elasticsearch

Validated Elasticsearch instance:

```text
Elasticsearch 8.15.3
```

Primary production indexes:

| Index               | Documents | Health    |
| ------------------- | --------: | --------- |
| building_risk_index |   198,131 | GREEN     |
| property_risk_index |   858,284 | GREEN     |
| pluto_address_index |   858,284 | Available |

The Elasticsearch document counts exactly match the Gold/Serving entity counts.

---

# Elasticsearch Sanity Validation

Sample Property documents were checked for:

```text
property_id
BBL
address
coordinates
property_risk_score
property_risk_level
property_risk_source
risk_as_of_date
model metadata
```

Examples confirmed that `BUILDING_ONLY` properties inherit the expected building risk and that properties with no risk data are represented explicitly as:

```text
property_risk_score  = null
property_risk_level  = NO_DATA
property_risk_source = NO_RISK_DATA
```

Resolved Building documents were also checked and contained consistent values for:

```text
building_id
BIN
property_id
resolved_bbl
current_address
building_risk_score
building_risk_level
risk_as_of_date
```

Validated resolved documents use:

```text
risk_as_of_date = 2026-09-03
```

---

# Known Caveat: Unresolved Buildings

The Building Identity process currently leaves:

```text
498 / 198,131 buildings
```

unresolved.

This represents approximately:

```text
0.25%
```

of the canonical building population.

For these records:

```text
resolution_status = UNRESOLVED
match_method       = UNRESOLVED
property_id        = null
resolved_bbl       = null
```

Sample validation also showed:

```text
risk_as_of_date = null
```

for unresolved Building documents.

These records are intentionally preserved instead of being dropped from the model. Building-level information such as BIN, address, coordinates and calculated building risk may still be available, while the relationship to a PLUTO property cannot currently be confirmed.

This is treated as a documented data limitation rather than a failed referential-integrity condition.

---

# Final Data Quality Validation

The following validation areas were completed successfully:

| Validation Area                | Result |
| ------------------------------ | ------ |
| End-to-End Pipeline            | ✅ PASS |
| Silver NYC 311                 | ✅ PASS |
| Silver HPD                     | ✅ PASS |
| Silver DOB                     | ✅ PASS |
| Silver PLUTO                   | ✅ PASS |
| Building Identity              | ✅ PASS |
| Gold Dimensions                | ✅ PASS |
| Bridge Referential Integrity   | ✅ PASS |
| Gold Facts                     | ✅ PASS |
| Building Risk                  | ✅ PASS |
| Property Risk                  | ✅ PASS |
| Serving Layer Counts           | ✅ PASS |
| Elasticsearch Building Count   | ✅ PASS |
| Elasticsearch Property Count   | ✅ PASS |
| Elasticsearch Property Samples | ✅ PASS |
| Elasticsearch Building Samples | ✅ PASS |

The only documented caveat is the known set of 498 unresolved buildings described above.

---

# Validated Snapshot

The final validation described in this README was performed against the September 2026 project snapshot.

Key validated counts:

```text
PLUTO Properties       858,284
Canonical Buildings    198,131
Resolved Buildings     197,633
Unresolved Buildings       498

311 Events             897,894
HPD Violations         939,343
DOB Violations         148,876

Elasticsearch Property 858,284
Elasticsearch Building 198,131
```

---

# Pipeline Orchestration

Major Airflow pipelines include:

```text
nyc_311_daily_pipeline
nyc_hpd_daily_pipeline
nyc_dob_daily_pipeline
nyc_pluto_snapshot_pipeline
nyc_building_identity_pipeline
nyc_gold_dimensions_pipeline
nyc_gold_facts_pipeline
nyc_property_risk_pipeline
```

Airflow launches the Spark jobs used for transformation and analytical processing.

Examples include:

```text
spark/silver/nyc_311/transform.py
spark/silver/hpd/transform.py
spark/silver/dob/transform.py
spark/silver/pluto/transform.py
spark/gold/building_identity.py
```

---

# Data Quality Philosophy

The project does not silently delete difficult records.

Instead, problematic or incomplete entities are preserved whenever they remain analytically useful.

Examples include:

```text
Missing BBL
Historical BBL
Address-only resolution
Geographic resolution
Unresolved Building Identity
NO_DATA risk classification
```

This allows the system to distinguish between:

```text
No known risk
No available data
Unable to resolve identity
Resolved entity with calculated risk
```

That distinction is important when the model is used for analytical or operational decision support.

---

# Current Project Status

The core Data Engineering platform is complete and validated:

```text
Ingestion       ✅
Bronze          ✅
Silver          ✅
Identity        ✅
Gold Model      ✅
Risk Models     ✅
Serving Layer   ✅
Elasticsearch   ✅
Data Quality    ✅
```

The remaining presentation/application work consists primarily of the final Kibana dashboard and the planned Telegram Bot interface.

---

# Planned Extensions

The next application layer will allow users to interact with the risk platform without accessing the underlying datasets directly.

The planned Telegram Bot will allow a user to submit a building or address and receive information such as:

```text
Building / Property
Address
Risk Score
Risk Level
Relevant risk indicators
```

Kibana will provide aggregated analytical views of risk by geography, risk level, complaints and violations.

---

# Repository

Project repository:

```text
https://github.com/doarshelgosh-design/nyc-building-risk-intelligence
```

---

## Author

Final project developed as part of a Data Engineering training program.

The project demonstrates a complete Data Engineering lifecycle:

```text
External APIs
→ Data ingestion
→ Data Lake
→ Spark transformations
→ Entity resolution
→ Dimensional modeling
→ Data Quality
→ Risk scoring
→ Serving layer
→ Elasticsearch
→ Analytics / Application layer
```
