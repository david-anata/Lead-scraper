"""Fulfillment Rate Sheet generator.

Free-form prospect intake -> LLM-extracted profile -> shipping rate matrix
(Anata WMS, mock fallback) -> hosted, printable rate sheet in the sales-deck
style. Storage rides on AutomationRun (run_type="fulfillment_rate_sheet").
"""
