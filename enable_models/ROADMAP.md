# Phase 1 EV Charging Station Analysis — Master Project Plan

## 0. Purpose of Phase 1

The global project objective is to recommend:

1. where new EV charging stations should be placed;
2. what characteristics those stations should have;
3. under the restriction that the proposed stations are ultrafast chargers.

Phase 1 does **not** attempt to solve the final recommendation problem yet.

The objective of Phase 1 is:

> Use the existing real-world charging network to understand which characteristics of a station and its surrounding location are associated with stronger observed performance.

The output is a **data analysis report**, not a predictive-model report.

It should answer questions such as:

* What distinguishes high-performing stations from low-performing stations?
* Does charging capacity matter?
* Does proximity to major roads matter?
* Does the traffic level of nearby roads matter?
* Do population and tourism matter?
* Do restaurants, retail, parking, hotels, fuel stations, and other amenities matter?
* Does competition from nearby charging stations matter?
* Do these relationships remain after accounting for obvious confounders such as charger power?
* Are there clear combinations of characteristics associated with better station performance?
* Which location characteristics therefore deserve to be considered when selecting future ultrafast sites?

The report must work at two levels:

**Business layer:** readable conclusions, plots, comparisons, effect sizes, trends, maps.

**Technical layer:** statistical evidence supporting those conclusions, such as correlations, nonlinear dependence measures, group tests, confidence intervals, and controlled analyses.

The technical layer supports the business story. It is not the story itself.

---

# 1. Fixed terminology

To avoid the terminology changing throughout the project, the following vocabulary should be used consistently.

## 1.1 Analytical unit

The main analytical unit will be:

> **station-period**

One row represents one physical charging station during one observation period.

We will use `period_id` rather than calling this a month until the meaning of `occupation_03_25` and `occupation_03_26` is formally confirmed.

Examples:

```text
station_id    period_id
ABF-00008     03_25
ABF-00008     03_26
```

This is preferable to prematurely assuming that `03_25` and `03_26` necessarily mean March 2025 and March 2026.

---

## 1.2 Performance label

The provisional primary performance label is:

> **station occupation rate**

At station level, this should be calculated as:

[
\text{occupation rate}
======================

\frac{\sum \text{charging hours}}
{\sum \text{available hours}}
]

rather than averaging connector-level occupation percentages.

The occupation tables contain charging hours, total hours, and an occupation field.

From the screenshots, values near `744` for the total field strongly suggest approximately one month of available hours, but this interpretation must be confirmed before the label definition is frozen.

Secondary performance measures can include:

* total charging hours;
* top-performing-station indicator;
* bottom-performing-station indicator.

If kWh, sessions, or revenue later become available, they can become additional labels. They should not be silently substituted for occupation.

---

## 1.3 Feature domains

Every analytical feature will belong to exactly one of these seven domains.

### Domain A — Station configuration

What was physically installed?

Examples:

* number of connectors;
* maximum connector power;
* total installed connector power;
* number of DC connectors;
* number of CCS connectors;
* number of ultrafast connectors;
* ultrafast share.

Source:

`station_configuration`.

---

### Domain B — Pricing

What does charging cost?

Examples:

* energy price in €/kWh;
* time price in €/minute;
* flat charging fee;
* representative-session cost;
* tariff type.

Source:

`prices`.

---

### Domain C — Local demand context

What type of population/economic/tourism environment surrounds the station?

Examples:

* population;
* households;
* tourism pressure;
* median income;
* income distribution.

Source:

`indicators`.

---

### Domain D — Amenity context

What useful destinations and services exist around the station?

Examples:

* restaurants;
* cafés;
* retail;
* supermarkets;
* parking;
* hotels;
* attractions;
* fuel stations.

Source:

`station_distances`.

The raw table contains hundreds of individual POI count/distance variables.

These will **not** all appear individually in the business report.

They will be reduced into a smaller set of interpretable amenity features.

---

### Domain E — Traffic and road access

How accessible is the location from the road network and how much traffic passes nearby?

Examples:

* nearest motorway distance;
* traffic on nearest motorway;
* number of major roads within a distance threshold;
* traffic exposure;
* immediate road classification;
* road use;
* road surface.

Sources:

* `station_highway_matrix`;
* `station_road_types`;
* underlying `traffic_*` tables when needed for validation or recomputation.

The database contains highway-specific distance and traffic variables for many Portuguese motorways.

---

### Domain F — Competition

How much charging infrastructure already exists around the location?

Examples:

* nearest competing station;
* number of stations within 1 km;
* number within 3 km;
* number of ultrafast competitors within 5 km;
* installed competing power within 3 km;
* distance to nearest ultrafast competitor.

This domain is **not currently stored directly**.

It can be derived using:

* `coordinates`;
* `station_configuration`.

Competition should therefore be considered a required derived feature domain rather than an optional idea.

---

### Domain G — Station identity and context

Information needed for grouping, mapping, and controls rather than direct site attractiveness.

Examples:

* station ID;
* period;
* municipality/city;
* operator;
* latitude;
* longitude.

Sources:

* `coordinates`;
* occupation tables.

---

# 2. Step 1 — Document the current Supabase schema

## Objective

Produce a complete and unambiguous inventory of what currently exists before transforming anything.

This is not yet feature engineering.

It answers:

> What exactly do we currently have?

---

## 2.1 Tables relevant to Phase 1

### `coordinates`

Current grain:

approximately one row per station.

Important fields:

* `station_id`
* `CIDADE`
* `MORADA`
* `LATITUDE`
* `LONGITUDE`

Role:

station identity and spatial location.

---

### `indicators`

Current grain:

one row per station.

Important fields:

* `population`
* `tourism_pressure`
* `num_households`
* `avg_income`
* `median_income`
* `p90_income`
* `gini_index`
* `num_taxpayers`

Role:

Domain C, local demand context.

---

### `occupation_03_25`

### `occupation_03_26`

Current grain:

appears to be EVSE/connector-level observations rather than one row per physical station.

Important fields:

* municipality;
* operator;
* point-of-interest category;
* connector mapping;
* charger class;
* `evseid_uve`;
* charging hours;
* total hours;
* occupation rate.

Role:

performance label.

Open question:

The exact meaning of `03_25` and `03_26` must be confirmed.

---

### `station_configuration`

Current grain:

one row per connector/socket.

Important fields:

* station ID;
* connector UID;
* charging type;
* voltage level;
* connector type;
* connector format;
* connector power.

Role:

Domain A, station configuration.

Current data-cleaning issue:

power is stored as text and examples contain decimal commas such as `7,4`.

---

### `prices`

Current grain:

multiple tariff components per connector.

Important fields:

* station ID;
* connector UID;
* tariff regime;
* tariff type;
* tariff text.

Examples include:

```text
€ 0.1/kWh
€ 0.015/min
€ 0.261/charge
```

Role:

Domain B, pricing.

Current issue:

the useful numerical quantities are embedded in text and require parsing.

---

### `station_distances`

Current grain:

one row per station.

Contains:

* POI counts;
* nearest POI distances;

for a very large number of POI categories.

Examples:

* restaurants;
* cafés;
* parking;
* supermarket;
* hotel;
* fuel;
* pharmacy;
* attractions;
* bus stations;
* etc.

Role:

Domain D, amenity context.

Current issue:

the table is far too wide and granular for meaningful direct interpretation.

Another important issue:

`-1` appears to represent "not found / unavailable", not an actual distance of negative one metres.

---

### `station_highway_matrix`

Current grain:

one row per station.

For every highway, it stores:

```text
<highway>_dist_m
<highway>_traffic
```

Role:

Domain E, traffic and road access.

Current issue:

`-1` is again being used as a sentinel for unavailable/not-applicable values.

The raw highway-specific matrix is also too wide for the client report.

---

### `station_road_types`

Current grain:

one row per station.

Fields:

* road classification;
* road use;
* road surface.

Role:

Domain E, traffic and road access.

---

### `traffic_*`

These tables contain the underlying motorway traffic segments and geometries.

Fields include:

* highway;
* subsegment;
* start node;
* end node;
* average 2024 traffic;
* geometry.

Role:

source data from which station-road traffic context can be derived or audited.

They are not expected to enter the Phase 1 analysis table directly.

---

### `traffic_fallbacks`

Contains fallback coordinates for highway nodes.

Role:

supporting traffic-processing table.

Not a direct report feature source.

---

## 2.2 Important structural problems already visible

The Step 1 inventory must explicitly document these.

### Problem 1 — Identifier alignment

Several different identifiers exist:

```text
station_id
station_ID
UID_TOMADA
UID DA TOMADA
evseid_uve
```

Before joining data, we must prove how these identifiers map to one physical station.

This is a blocking issue.

---

### Problem 2 — Different table grains

Examples:

```text
coordinates             station
indicators              station
station_configuration   connector
prices                  connector × tariff component
occupation              EVSE/connector-period
station_distances       station
highway_matrix          station
```

These tables cannot simply be joined together.

Each must first be transformed to the common **station-period** analytical grain.

---

### Problem 3 — Missing-value encoding

Several location tables use:

```text
-1
```

to represent absence/unavailability.

These values cannot be fed directly into statistics or ML as actual physical distances.

---

### Problem 4 — Wide POI representation

`station_distances` contains hundreds of raw POI categories.

Using every category directly would:

* make statistical results difficult to interpret;
* generate heavy redundancy;
* increase false discoveries;
* make the client report incomprehensible.

A deliberate POI reduction strategy is therefore required.

---

### Problem 5 — Limited temporal information

At present, the visible schema contains two occupation tables.

We must establish exactly:

* what periods they represent;
* whether additional periods exist elsewhere;
* whether station configuration and prices correspond to those periods or are current snapshots.

---

### Problem 6 — Unclear ultrafast sample size

The final project concerns ultrafast charging.

Before report design is finalized, we must calculate:

* total stations;
* ultrafast stations;
* ultrafast connectors;
* stations with at least one ≥150 kW connector;
* their distribution across operators and regions.

If there are very few ultrafast observations, this materially changes what Phase 1 can claim.

---

## Step 1 output

The dedicated Step 1 chat should produce:

### Artifact 1

`CURRENT_DATA_SCHEMA.md`

Containing:

* every relevant table;
* every relevant column;
* grain;
* identifier;
* data type;
* meaning;
* missing-value conventions;
* role in Phase 1;
* known issues.

### Artifact 2

`CURRENT_FEATURE_INVENTORY.csv`

One row per raw feature.

Suggested columns:

```text
source_table
source_column
raw_type
grain
feature_domain
description
usable_directly
transformation_required
missing_encoding
phase1_status
notes
```

### Completion criterion

Step 1 is complete when we can point at every Phase 1 source field and explain exactly what it represents.

---

# 3. Step 2 — Design the ideal Phase 1 analytical schema

## Objective

Define what we want before writing transformations.

The target architecture should be simple.

We do **not** need another complicated application database.

We need four analysis artifacts.

---

## 3.1 `station_static_features`

One row per physical station.

Contains Domains A–G that do not change by period.

Example structure:

### Identity

```text
station_id
city
latitude
longitude
```

### Domain A — Station configuration

```text
connector_count
max_power_kw
total_power_kw
mean_power_kw
dc_connector_count
ccs_connector_count
chademo_connector_count
ultrafast_connector_count
ultrafast_connector_share
is_ultrafast_station
```

### Domain B — Pricing

Final exact features will depend on what the tariff data supports, but likely:

```text
energy_price_eur_kwh
time_price_eur_min
flat_price_eur_charge
reference_session_cost_eur
```

### Domain C — Local demand context

```text
population
num_households
tourism_pressure
median_income
gini_index
```

### Domain D — Amenity context

A deliberately small group of interpretable features, for example:

```text
food_count
food_nearest_m

retail_count
retail_nearest_m

parking_count
parking_nearest_m

tourism_count
tourism_nearest_m

transport_services_count
transport_services_nearest_m
```

The exact POIs composing each category must be defined explicitly in Step 2.

### Domain E — Traffic and road access

```text
nearest_highway_dist_m
nearest_highway_traffic
max_accessible_highway_traffic
highways_within_2km
highways_within_5km
traffic_access_index

road_classification
road_use
road_surface
```

### Domain F — Competition

```text
nearest_competitor_m
competitors_within_1km
competitors_within_3km
competitors_within_5km

nearest_ultrafast_competitor_m
ultrafast_competitors_within_5km

competing_power_within_3km
```

---

## 3.2 `station_period_performance`

One row per:

```text
station_id × period_id
```

Contains:

```text
station_id
period_id
operator
charging_hours
available_hours
occupation_rate
```

Potential derived labels:

```text
is_top_20pct
is_bottom_20pct
performance_quintile
```

These are analysis labels, not new raw observations.

---

## 3.3 `phase1_analysis_table`

This is the final table used for statistics.

It is simply:

```text
station_period_performance
JOIN
station_static_features
```

Grain:

> one station-period per row.

Every analysis in Phase 1 must be reproducible from this table.

---

## 3.4 `feature_dictionary`

Every final feature gets a human-readable definition.

Example:

| feature                | domain                  | client label           | definition                                                           |
| ---------------------- | ----------------------- | ---------------------- | -------------------------------------------------------------------- |
| `max_power_kw`         | Station configuration   | Maximum charging power | Highest connector power installed at the station                     |
| `food_count`           | Amenity context         | Nearby food options    | Number of selected food-related POIs within the source search radius |
| `traffic_access_index` | Traffic and road access | Traffic exposure       | Derived combination of highway traffic and distance                  |

This dictionary will later prevent the report terminology from drifting.

---

## Step 2 output

`PHASE1_ANALYTICAL_SCHEMA.md`

and:

`PHASE1_FEATURE_DICTIONARY.csv`

### Completion criterion

Every final analytical column must have:

* a name;
* a definition;
* a source;
* a transformation;
* a feature domain;
* a reason for inclusion.

---

# 4. Step 3 — Gap analysis: what separates the current schema from the ideal schema?

This step compares Step 1 directly against Step 2.

Nothing gets implemented yet.

For every desired feature, we classify it as:

```text
READY
TRANSFORM
DERIVE
MISSING
NEEDS_DECISION
```

Examples:

| Desired feature        | Status         | Required action                      |
| ---------------------- | -------------- | ------------------------------------ |
| population             | READY          | direct from indicators               |
| max_power_kw           | TRANSFORM      | parse power and aggregate connectors |
| occupation_rate        | TRANSFORM      | aggregate occupation rows            |
| energy_price_eur_kwh   | TRANSFORM      | parse TARIFA                         |
| food_count             | DERIVE         | combine selected POI counts          |
| traffic_access_index   | DERIVE         | combine traffic and distance         |
| competitors_within_3km | DERIVE         | spatial computation                  |
| kWh                    | MISSING        | source not currently identified      |
| period meaning         | NEEDS_DECISION | confirm 03_25/03_26 semantics        |

---

## Step 3 output

`PHASE1_GAP_ANALYSIS.md`

plus a task table:

```text
task_id
desired_feature
status
source
required_operation
blocking
priority
validation_rule
```

### Completion criterion

There should be no feature in the ideal schema for which we cannot explain exactly how it will be produced.

---

# 5. Step 4 — Implement the transformations

This is the first implementation-heavy phase.

It should be completed incrementally.

## Task 4.1 — Audit identifiers

Establish physical-station mappings.

Validate:

```text
station_configuration → station
prices → station
occupation → station
```

Produce an exception table for unmatched IDs.

---

## Task 4.2 — Build station configuration features

Parse connector power.

Aggregate connector records to station level.

Validate:

```text
connector_count
max_power_kw
total_power_kw
ultrafast_connector_count
...
```

---

## Task 4.3 — Build performance labels

Transform occupation rows to:

```text
station_id × period_id
```

Use weighted station occupation:

[
\frac{\sum charging\ hours}{\sum available\ hours}
]

Validate against raw `TX_OCUPACAO`.

---

## Task 4.4 — Parse pricing

Turn tariff strings into numerical components.

Create explicit missing indicators where necessary.

---

## Task 4.5 — Clean local demand features

Validate ranges and missingness for socioeconomic indicators.

Do not standardize them yet for the report.

The business report should still be able to discuss actual units.

---

## Task 4.6 — Reduce POIs into amenity features

First define an explicit mapping:

```text
raw POI → amenity category
```

Then calculate category-level:

```text
count
nearest distance
```

Do not invent opaque scores yet.

Keep the intermediate features interpretable.

---

## Task 4.7 — Engineer traffic/access features

Replace sentinel `-1` values with proper missing values.

Then calculate the agreed traffic variables.

---

## Task 4.8 — Engineer competition

Using station coordinates and station configuration:

calculate station-to-station distances and nearby competing capacity.

---

## Task 4.9 — Join the analytical table

Produce:

`phase1_analysis_table.parquet`

---

## Task 4.10 — Data validation

Check:

* duplicate station-period rows;
* impossible coordinates;
* occupation outside expected bounds;
* connector powers;
* missing labels;
* unmatched station IDs;
* sample counts;
* ultrafast sample size;
* missingness by feature.

---

## Step 4 output

A reproducible feature-building pipeline plus the final Phase 1 analytical dataset.

### Completion criterion

One command should rebuild the entire Phase 1 analytical table from the source data.

---

# 6. Step 5 — Research and select the statistical methods

This deserves its own dedicated research chat.

We should not decide methods merely because they are familiar.

The question is:

> Which model-free or minimally model-dependent statistics give useful, defensible, and client-interpretable evidence about station performance?

We should specifically research and compare the following families.

---

## 5.1 Monotonic association

Candidate:

**Spearman rank correlation**

Question answered:

> As this feature increases, does performance generally increase or decrease?

Useful for:

* population;
* power;
* traffic;
* distance;
* POI counts;
* pricing.

---

## 5.2 Linear association

Candidate:

**Pearson correlation**

Question answered:

> Is there approximately linear association?

Useful primarily as a secondary diagnostic.

It should probably not be the headline metric.

---

## 5.3 Nonlinear dependence

Candidate:

**Mutual information**

Question answered:

> Does the feature contain information about station performance even when the relationship is not monotonic?

This can catch:

* thresholds;
* U-shaped relationships;
* other nonlinear patterns.

Its interpretability limitations must be studied.

---

## 5.4 Group differences

For categorical features such as:

```text
road type
operator
station class
```

Candidates:

* ANOVA when assumptions are acceptable;
* Kruskal-Wallis for non-parametric comparison.

But a p-value alone is insufficient.

We also need an **effect size**.

Candidates include:

* eta-squared;
* epsilon-squared.

---

## 5.5 Two-group effect size

For comparisons such as:

```text
ultrafast vs non-ultrafast
has nearby parking vs no nearby parking
```

Candidates:

* Cohen's d;
* Cliff's delta;
* rank-biserial correlation.

We need to choose a method appropriate to the actual target distribution.

---

## 5.6 Controlled association

This is particularly valuable.

Example question:

> Does tourism still have an association with occupation once charging power is accounted for?

Candidate methods:

* partial correlation;
* partial Spearman correlation;
* simple controlled regression.

This may protect us from misleading findings such as:

```text
higher price → higher occupation
```

when price is actually acting as a proxy for high-powered chargers.

---

## 5.7 Statistical uncertainty

Candidates:

**bootstrap confidence intervals**

Instead of saying:

```text
median difference = 6 percentage points
```

we can say:

```text
estimated difference = +6 pp
95% bootstrap CI = [+3, +9]
```

That is both statistically stronger and understandable to a client.

---

## 5.8 Feature redundancy

Before interpreting twenty correlated variables independently, investigate redundancy.

Candidates:

* feature-feature Spearman matrix;
* hierarchical clustering of correlated features;
* variance inflation for selected controlled models.

---

## Step 5 output

`STATISTICAL_METHODS.md`

For every chosen method:

```text
name
question it answers
mathematical meaning
assumptions
when we use it
when we do not use it
how it appears in the report
how it is calculated
how it is interpreted
```

### Completion criterion

We end with a **small fixed statistical toolkit**, not a collection of every metric available.

---

# 7. Step 6 — Apply the selected statistical toolkit to the analytical schema

Step 5 decides the tools.

Step 6 attaches them systematically to the feature domains from Step 2.

The final framework should resemble:

| Feature type           | Primary analysis               | Secondary analysis    |
| ---------------------- | ------------------------------ | --------------------- |
| continuous             | Spearman + segmented trend     | mutual information    |
| binary                 | group difference + effect size | bootstrap CI          |
| categorical            | group medians + Kruskal-Wallis | effect size           |
| potentially confounded | partial association            | controlled regression |
| redundant features     | feature correlation            | clustering            |

This prevents arbitrary analysis.

Every feature is processed according to its type.

---

## Step 6 output

`PHASE1_ANALYSIS_SPECIFICATION.md`

Containing one row per final feature:

```text
feature
domain
type
primary_analysis
secondary_analysis
control_variables_if_any
report_visual
report_question
```

### Completion criterion

Before running statistics, we know exactly what analysis every feature will receive and why.

---

# 8. Step 7 — Segmented performance analysis

This is what the user described as A/B-style analysis with potentially more than two groups.

This should become a major part of the report.

---

## 7.1 Numerical features

For genuinely continuous features, default to **five quantile groups** when sample size supports it:

```text
Q1
Q2
Q3
Q4
Q5
```

Example:

population from lowest 20% of station environments to highest 20%.

For each group calculate:

```text
number of stations
median occupation
mean occupation
interquartile range
top-20%-performer rate
bottom-20%-performer rate
```

Then calculate:

### Performance lift

[
\text{lift}
===========

\frac{\text{group performance}}
{\text{overall performance}}
]

Example:

> Stations in the highest traffic-access quintile have 1.42× the median occupation of the overall network.

---

## 7.2 Features with meaningful thresholds

Do not force quantiles when the business has natural cut-offs.

Example charging power:

```text
≤22 kW
23–49 kW
50–149 kW
≥150 kW
```

For distance to highway:

possible business thresholds might be:

```text
<1 km
1–3 km
3–5 km
>5 km
```

Thresholds should be justified rather than arbitrarily selected.

---

## 7.3 Binary features

Example:

```text
has restaurant nearby
no restaurant nearby
```

Use two-group analysis.

---

## 7.4 Categorical features

Example:

```text
road classification
```

Compare the existing categories directly, provided there is sufficient sample size.

Rare categories should be grouped explicitly into `Other`.

---

## 7.5 Top-performer presence

Create:

```text
is_top_20pct
```

Then for every segment calculate:

> What percentage of stations in this segment are top performers?

Example:

```text
Lowest tourism quintile:   8% top performers
Highest tourism quintile: 36% top performers
```

This is much easier to communicate than a raw correlation coefficient.

The underlying technical analysis can still show:

```text
Spearman ρ
effect size
confidence interval
p-value
```

---

## Step 7 output

For every major feature:

```text
segment table
performance distribution
top-performer presence
effect size
confidence interval
```

### Completion criterion

We can discuss every important feature in normal business language while retaining statistical evidence underneath.

---

# 9. Step 8 — Define the report skeleton

The report should have a fixed narrative.

## 8.1 Executive summary

Maximum approximately one page.

Contains only the major conclusions.

Example style:

> High-performing stations are disproportionately associated with X, Y, and Z. Charging capacity shows the strongest relationship with utilization, while road-access and amenity context explain meaningful additional variation. Price effects are less straightforward after controlling for charger characteristics.

No statistical methodology exposition here.

---

## 8.2 Objective and scope

Explain:

* the business question;
* what Phase 1 does;
* what Phase 1 does not claim.

---

## 8.3 Data and analytical population

Show:

* number of stations;
* operators;
* municipalities;
* periods;
* connectors;
* ultrafast stations;
* data coverage.

Include one network map.

---

## 8.4 Performance landscape

Show:

* distribution of occupation;
* median;
* spread;
* high and low performers;
* geographical distribution.

Purpose:

> Establish what "good" and "bad" performance currently looks like.

---

## 8.5 Station configuration and performance

Domain A.

Questions:

* Does installed power relate to utilization?
* Do stations with more connectors perform differently?
* How do charger classes compare?
* What happens specifically in the ultrafast group?

Main visual types:

* segmented trend;
* distribution comparison;
* effect-size summary.

---

## 8.6 Local demand context and performance

Domain C.

Questions:

* population;
* households;
* tourism;
* income.

Use:

* quintile trends;
* Spearman;
* nonlinear dependence;
* controlled association where appropriate.

---

## 8.7 Amenity context and performance

Domain D.

Questions such as:

* food;
* retail;
* parking;
* tourism amenities;
* transport/services.

Do not dump individual OSM categories.

---

## 8.8 Traffic and road access

Domain E.

Questions:

* does highway proximity matter?
* does nearby traffic volume matter?
* what road contexts perform better?
* is traffic volume more important than raw distance?

---

## 8.9 Competition

Domain F.

Questions:

* does nearby charging supply reduce observed utilization?
* does lack of nearby ultrafast supply identify opportunity?
* how does surrounding installed power relate to performance?

This section is directly relevant to future site placement.

---

## 8.10 Pricing

Domain B.

Treat cautiously.

Questions:

* are higher-priced stations associated with different utilization?
* does that relationship remain after controlling for station power/type?

Raw marginal price correlation should never be interpreted as a causal price effect.

---

## 8.11 What distinguishes high performers?

This combines all domains.

Create one summary table:

| Dimension | High-performing stations | Low-performing stations | Evidence strength |
| --------- | ------------------------ | ----------------------- | ----------------- |

This is likely the main client-facing analytical section.

---

## 8.12 Location archetypes

Only after the previous analyses support them.

Potential archetypes might include:

```text
highway/transit
retail/destination
tourism
urban/residential
```

But these are **not fixed in advance**.

The data should determine whether these distinctions are useful.

---

## 8.13 Implications for future ultrafast sites

Translate evidence into requirements.

Example format:

### Evidence

Higher traffic-access locations show stronger performance.

### Implication

Traffic exposure should be included in future candidate-site scoring.

### Not claimed

High traffic alone guarantees a successful site.

Every recommendation should follow this structure.

---

## 8.14 Limitations

Explicitly distinguish:

```text
association
prediction
causation
counterfactual inference
```

Phase 1 is primarily association analysis.

---

## 8.15 Technical appendix

Place here:

* exact formulas;
* correlations;
* mutual information;
* statistical tests;
* confidence intervals;
* feature mappings;
* missing-value treatment;
* full feature tables.

This keeps rigor without destroying readability.

---

# 10. Step 9 — Produce the final report and figures

Once Steps 1–8 are frozen enough, implementation becomes mechanical.

## 9.1 Analysis outputs

Generate:

```text
data_quality_summary.csv
network_performance_summary.csv

segment_analysis.csv
association_analysis.csv
controlled_analysis.csv
categorical_group_analysis.csv

high_vs_low_performers.csv
feature_redundancy.csv
```

---

## 9.2 Plot outputs

Organize by report section:

```text
figures/
    01_network/
    02_performance/
    03_configuration/
    04_demand_context/
    05_amenities/
    06_traffic/
    07_competition/
    08_pricing/
    09_high_performers/
```

---

## 9.3 One rule for every analytical claim

Every report insight should have five components:

### 1. Business question

Example:

> Does motorway traffic exposure relate to station performance?

### 2. Evidence

Example:

> Occupation increases across traffic-exposure quintiles.

### 3. Statistical support

Example:

> Spearman association, effect size, bootstrap confidence interval.

### 4. Visual

One readable chart.

### 5. Business interpretation

Example:

> Traffic exposure appears useful as a future candidate-site feature.

This rule prevents the report from becoming either:

* a statistics dump; or
* an unsupported business story.

---

# 11. Dedicated-chat breakdown

The project should now be tackled in exactly this sequence.

## Chat 1 — Current Data Inventory

Goal:

> Fully document what exists today.

Deliverables:

```text
CURRENT_DATA_SCHEMA.md
CURRENT_FEATURE_INVENTORY.csv
```

No feature engineering yet.

---

## Chat 2 — Phase 1 Analytical Schema

Goal:

> Decide exactly which final features Phase 1 should contain.

Deliverables:

```text
PHASE1_ANALYTICAL_SCHEMA.md
PHASE1_FEATURE_DICTIONARY.csv
```

This is where we debate POI categories, traffic features, competition features, prices, etc.

---

## Chat 3 — Schema Gap Analysis

Goal:

> Compare current data against desired features.

Deliverable:

```text
PHASE1_GAP_ANALYSIS.md
```

Every feature gets a concrete transformation plan.

---

## Chat 4 — Build the Phase 1 Dataset

Goal:

> Implement transformations incrementally.

Sub-chats/tasks may be used for:

```text
4A IDs
4B occupation
4C configuration
4D prices
4E POIs
4F traffic
4G competition
4H final join/validation
```

Final output:

```text
phase1_analysis_table.parquet
```

---

## Chat 5 — Statistical Methods Research

Goal:

> Properly research and understand which statistical techniques we should use.

Possible subjects:

```text
Spearman
Pearson
mutual information
effect sizes
Kruskal-Wallis / ANOVA
partial correlation
controlled regression
bootstrap confidence intervals
feature redundancy
```

Final output:

```text
STATISTICAL_METHODS.md
```

---

## Chat 6 — Phase 1 Analysis Specification

Goal:

> Attach the chosen methods systematically to every feature.

Final output:

```text
PHASE1_ANALYSIS_SPECIFICATION.md
```

No arbitrary statistics after this point.

---

## Chat 7 — Segments and Performance Presence

Goal:

> Define the quintiles, business thresholds, categorical groups, and high/low-performance analyses.

Final output:

```text
SEGMENTATION_AND_PERFORMANCE_ANALYSIS.md
```

Then run the analysis.

---

## Chat 8 — Final Report Architecture

Goal:

> Freeze the current report skeleton using the analyses that actually worked.

Final output:

```text
REPORT_SKELETON.md
```

This happens **after** we know what analyses produce useful evidence.

The skeleton above is the current vision, not an immutable final version.

---

## Chat 9 — Final Phase 1 Report

Goal:

> Create every final table, figure, written interpretation, appendix, and conclusion.

Outputs:

```text
REPORT.md / REPORT.pdf
figures/
tables/
technical_appendix/
```

Phase 1 then ends.

Only after this should the project transition into systematic predictive-model evaluation.

---

# 12. Overall dependency chain

The project can now be thought of as:

```text
WHAT DO WE HAVE?
        │
        ▼
1. Current schema inventory
        │
        ▼
WHAT DO WE WANT?
        │
        ▼
2. Ideal analytical schema
        │
        ▼
WHAT IS MISSING?
        │
        ▼
3. Gap analysis
        │
        ▼
BUILD IT
        │
        ▼
4. Phase 1 analytical dataset
        │
        ▼
HOW SHOULD WE ANALYSE IT?
        │
        ▼
5. Statistical-method research
        │
        ▼
6. Analysis specification
        │
        ▼
HOW DO WE TURN NUMBERS INTO BUSINESS COMPARISONS?
        │
        ▼
7. Segmented performance analysis
        │
        ▼
WHAT STORY DOES THE EVIDENCE SUPPORT?
        │
        ▼
8. Report structure
        │
        ▼
9. Final report
```

This is the current Phase 1 project vision.

The most important principle is that **we do not jump ahead**.

For example:

* we do not calculate hundreds of correlations before deciding the analytical schema;
* we do not choose POI scores while the ideal feature schema is still unresolved;
* we do not design final report claims before seeing which statistical analyses actually provide stable evidence;
* we do not begin model-performance comparisons until the exploratory report is complete.

Each stage produces a concrete artifact that becomes the input to the next stage.
