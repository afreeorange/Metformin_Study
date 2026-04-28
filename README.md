# Among adults with T2DM initiating Metformin monotherapy, what demographic and clinical characteristics are associated with the _timing_ of second-line antidiabetic therapy initiation?

## Game Plan

### Introduction

Why are we doing this?

### Descriptive Analysis

- Distributions of
  - Time to Escalation (outcome)
  - Demographics
  - Comorbidities
  - Diabetes Duration before index date
- Top Second-line Drugs
- Comorbidity co-occurrence
- Time to Escalation (outcome of interest!)
  - Demographics
  - Comorbidities
  - Diabetes Duration

### Results

TODO: Search the Stratified Cox results and look for good CIs and p-values. Separate into two lists based on HRs: slow and fast escalation. Make bulleted lists of each (and report HRs)

---

## Stuff to add to paper

Why is this study important?

"SynPUF's synthetic dating and left truncation make the metformin-after-diagnosis lag uninterpretable as a clinical quantity." -- apropos the REALLY CRAY CRAY diabetes Duration.

Timeline!

---

## Development

### Cohort Definition

`Scripts/cohort_raw.sql` generates `Data/cohort_raw.csv`.

[Here is the cohort definition](https://atlas-demo.ohdsi.org/#/cohortdefinition/1796944/definition) in OHDSI ATLAS (_please do not edit_!)

[Updated definition](https://atlas-demo.ohdsi.org/#/cohortdefinition/1797130/definition)

`Scripts/cohort_raw.json` captures the last known 'working' state of the cohort definition in ATLAS and can be used to generate this definition ([copypasta](https://atlas-demo.ohdsi.org/#/cohortdefinition/1796944/export) and hit "Reload".)

### HbA1C Problems

Based on the query below, we're looking for [Hemoglobin; glycosylated (A1C)](https://athena.ohdsi.org/search-terms/terms/2212392) as a _measurement_ with Concept ID `2212392` — [other concepts pertaining to HbA1C measurements](https://atlas-demo.ohdsi.org/#/conceptset/1887624/expression) were not present in the `measurements` table (at least in SynPUF 5%) after running this query (thank you Amelia!)

```sql
SELECT TOP 100
    c.concept_name,
    c.concept_id,
    COUNT(*) AS measurement_count
FROM measurement m
JOIN concept c
    ON m.measurement_concept_id = c.concept_id
GROUP BY c.concept_name, c.concept_id
ORDER BY measurement_count DESC;
```

This yields the Concept ID we want to focus on (as shown below) but the `measurement` table has _all_ `NULL` records for the values; we're not going to use it for this reason.

![](https://public.nikhil.io/grad.nikhil.io/hba1c-query-comp-epi.jpeg)

## Notes

Observation period is from Jan 15 2009 -- December 15 2009 (334 days). This is based on [richness/availability](https://atlas-demo.ohdsi.org/#/datasources) of data in SynPUF 5%.

### Second Line Drugs

Drugs and their parent 'classes'. Here's [the Concept Set in ATLAS](https://atlas-demo.ohdsi.org/#/conceptset/1890187/expression) (_do not edit!_)

| Parent        | Drug                                                       |
| ------------- | ---------------------------------------------------------- |
| DPP-4i        | sitagliptin, saxagliptin, linagliptin, alogliptin          |
| GLP-1 RA      | semaglutide, liraglutide, dulaglutide, exenatide           |
| Insulin       | insulin glargine, insulin detemir, insulin degludec        |
| SGLT-2i       | empagliflozin, dapagliflozin, canagliflozin, ertugliflozin |
| Sulfonylureas | glipizide, glyburide, glimepiride                          |
| TZDs          | pioglitazone, rosiglitazone                                |

## References

- [Comorbidities associated with Type II Diabetes](https://link.springer.com/article/10.1186/s12916-019-1373-y). TLDR: Hypertension and Depression in females, Hypertension and CHD in males, in those orders.
- [_Comparative risk of serious hypoglycemia with oral antidiabetic monotherapy: a retrospective cohort study_](https://pmc.ncbi.nlm.nih.gov/articles/PMC5770147/)

## Authors

Nikhil Anand and Giselle Feng

## License

[WTFPL](https://www.wtfpl.net/)
