from __future__ import annotations

import unittest

from sales_support_agent.services.gmail_drafts import create_bulk_draft_payloads


class GmailDraftPayloadTests(unittest.TestCase):
    def test_builds_payloads_from_templates_and_csv_columns(self) -> None:
        csv_bytes = (
            b"email,first_name,company,custom_angle\n"
            b"pat@example.com,Pat,Acme,growth on Amazon\n"
        )

        result = create_bulk_draft_payloads(
            csv_bytes=csv_bytes,
            sales_objective="help {{company}} win more Amazon margin",
            subject_template="Quick idea for {{company}}",
            body_template="Hi {{first_name}},\n\nReaching out about {{custom_angle}} and {{objective}}.",
        )

        self.assertEqual(result["rows_total"], 1)
        self.assertEqual(len(result["prepared_rows"]), 1)
        prepared = result["prepared_rows"][0]
        self.assertEqual(prepared["email"], "pat@example.com")
        self.assertEqual(prepared["subject"], "Quick idea for Acme")
        self.assertIn("Pat", prepared["body"])
        self.assertIn("growth on Amazon", prepared["body"])
        self.assertIn("help {{company}} win more Amazon margin", prepared["body"])

    def test_uses_subject_and_body_columns_when_templates_are_blank(self) -> None:
        csv_bytes = (
            b"email,subject,body\n"
            b"pat@example.com,Hello there,Body copy here\n"
        )

        result = create_bulk_draft_payloads(
            csv_bytes=csv_bytes,
            sales_objective="",
            subject_template="",
            body_template="",
        )

        prepared = result["prepared_rows"][0]
        self.assertEqual(prepared["subject"], "Hello there")
        self.assertEqual(prepared["body"], "Body copy here")

    def test_reports_missing_email_or_missing_template_output(self) -> None:
        csv_bytes = (
            b"company,first_name\n"
            b"Acme,Pat\n"
        )

        result = create_bulk_draft_payloads(
            csv_bytes=csv_bytes,
            sales_objective="",
            subject_template="",
            body_template="",
        )

        self.assertEqual(result["prepared_rows"], [])
        self.assertEqual(len(result["failed_rows"]), 1)
        self.assertIn("Missing email", result["failed_rows"][0]["error"])


if __name__ == "__main__":
    unittest.main()
