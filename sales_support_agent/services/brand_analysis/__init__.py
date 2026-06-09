"""Executive > Brand Analysis — templated acquisition-grade report builder.

Drop a brand's financial "file dump" (P&L, Balance Sheet, Trial Balance, GL,
prior-year), parse what's present, run a fixed deterministic grading
methodology (weighted A–F composite over 8 dimensions), and produce a
standardized executive acquisition report — rendered on-screen and exportable
as .docx, with a browsable per-org history.

Module map (mirrors services/advertising):
  schema.py      — dataclasses + numeric helpers + benchmark tables
  intake.py      — multi-file parse (xlsx/csv/pdf) + line-item mapping
  scoring.py     — deterministic metrics, weighted scorecard, red flags
  confidence.py  — missing-data list + confidence level
  llm.py         — LLM narrative over already-computed metrics (graceful)
  report.py      — orchestrates intake -> scoring -> confidence -> llm
  report_page.py — on-screen HTML report + upload + history views
  docx_export.py — server-side .docx export
  storage.py     — DB-backed run history + kv_store source/docx blobs
"""
