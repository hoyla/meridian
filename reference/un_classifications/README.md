# UNSD classification workbooks — build-only inputs

Raw UN Statistics Division correspondence tables. These are **build-only**:
`classifications.py` reads them to (re)generate the derived lookups in the
parent `reference/` folder — `cn8_sitc.csv` and `cn8_bec.csv`. The briefing
publication path reads **only those derived CSVs**, never these workbooks
(see `classifications.assert_classifications_available`).

They are committed here so a clean checkout can rebuild the lookups without a
machine-local reference folder. (They previously lived outside the repo at
`~/Code/un-classifications/`; a folder move silently emptied the
classifications, collapsing every group into SITC section 9 — hence the move
in-repo and the fail-loud preflight.)

| File | Used for | Read by |
|------|----------|---------|
| `hs2022_sitc4.xlsx` | HS 2022 → SITC Rev 4 (primary) | `classifications.build()` |
| `hs2017_sitc4.xlsx` | HS 2017 → SITC Rev 4 (fallback) | `classifications.build()` |
| `HS-SITC-BEC_Correlations_2022.xlsx` | HS 2022 → BEC Rev 4 (end-use) | `classifications.build_bec()` |

- Source: <https://unstats.un.org/unsd/classifications/Econ/tables/>, downloaded 2026-06-20.
- Vintages: HS 2022 / SITC Rev 4 / BEC Rev 4. Refresh when HS 2027 ships and the
  EU CN rebases (add the HS2027→SITC correspondence ahead of HS2022 in the chain).

To regenerate the derived CSVs after refreshing a workbook:

```sh
python classifications.py   # rebuilds cn8_sitc.csv and cn8_bec.csv (needs the DB)
```
