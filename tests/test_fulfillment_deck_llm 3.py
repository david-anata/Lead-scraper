from __future__ import annotations

import io
import sys
import types
import unittest
from pathlib import Path
from unittest import mock

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from sales_support_agent.services.fulfillment_deck.intake import build_extraction_context
from sales_support_agent.services.fulfillment_deck.llm import (
    ExtractionMeta,
    extract_prospect_profile,
)
from sales_support_agent.services.fulfillment_deck.schema import (
    ProductRates,
    ProductSpec,
    ProspectProfile,
    RateMatrix,
    RateQuote,
    SectionFlags,
    ZoneRates,
)
from sales_support_agent.services.fulfillment_deck.sections import decide_sections

_NO_KEY_ENV = {"ANTHROPIC_API_KEY": ""}


def _tiny_xlsx_bytes() -> bytes:
    import openpyxl

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.append(["sku", "weight_lb"])
    ws.append(["SERUM-01", 1.2])
    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


class IntakeTests(unittest.TestCase):
    def test_notes_csv_and_xlsx_all_present(self):
        csv_bytes = b"product,length,width\nSuper Serum,4,4\n"
        context, warnings = build_extraction_context(
            "Met them at the trade show. Volume looks real.",
            [("skus.csv", csv_bytes), ("weights.xlsx", _tiny_xlsx_bytes())],
            "",
        )
        self.assertIn("=== SALES NOTES ===", context)
        self.assertIn("Met them at the trade show", context)
        self.assertIn("=== FILE: skus.csv ===", context)
        self.assertIn("Super Serum,4,4", context)
        self.assertIn("=== FILE: weights.xlsx ===", context)
        self.assertIn("SERUM-01", context)
        self.assertEqual(warnings, [])

    def test_unsupported_extension_warns_and_skips(self):
        context, warnings = build_extraction_context("notes", [("deck.pdf", b"%PDF-junk")], "")
        self.assertNotIn("deck.pdf", context)
        self.assertTrue(any("Unsupported file type: deck.pdf" in w for w in warnings))

    def test_broken_bytes_warn_instead_of_raising(self):
        context, warnings = build_extraction_context(
            "notes", [("corrupt.xlsx", b"\x00\x01not a zip at all")], ""
        )
        self.assertIn("=== SALES NOTES ===", context)
        self.assertTrue(any("corrupt.xlsx" in w for w in warnings))

    def test_website_fetch_strips_tags(self):
        html = (
            "<html><head><style>.x{color:red}</style>"
            "<script>var secret = 1;</script></head>"
            "<body><h1>Acme Goods</h1><p>We ship serums   nationwide</p></body></html>"
        )
        fake_resp = mock.Mock()
        fake_resp.text = html
        with mock.patch("requests.get", return_value=fake_resp) as get:
            context, warnings = build_extraction_context("", [], "https://acme.example")
        get.assert_called_once()
        self.assertEqual(get.call_args.kwargs.get("timeout"), 10)
        self.assertIn("=== WEBSITE: https://acme.example ===", context)
        self.assertIn("Acme Goods", context)
        self.assertIn("We ship serums nationwide", context)  # whitespace collapsed
        self.assertNotIn("var secret", context)
        self.assertNotIn("color:red", context)
        self.assertNotIn("<h1>", context)
        self.assertEqual(warnings, [])

    def test_website_fetch_failure_warns_not_raises(self):
        with mock.patch("requests.get", side_effect=ConnectionError("boom")):
            context, warnings = build_extraction_context("notes here", [], "https://down.example")
        self.assertIn("notes here", context)
        self.assertNotIn("=== WEBSITE", context)
        self.assertTrue(any("https://down.example" in w for w in warnings))


class LlmExtractionTests(unittest.TestCase):
    def _fallback_context(self) -> str:
        return (
            "=== SALES NOTES ===\n"
            "Company: Acme Wellness\n"
            "Website: https://acme.example\n"
            "Email: jane@acme.example\n"
            "Monthly order volume: 5,000\n"
            "Super Serum — 4 x 4 x 6 in, 1.2 lb, ~3000 units/mo\n"
        )

    def test_no_key_uses_fallback_with_warning(self):
        with mock.patch.dict("os.environ", _NO_KEY_ENV):
            profile, meta = extract_prospect_profile(self._fallback_context(), api_key=None)
        self.assertIsInstance(profile, ProspectProfile)
        self.assertIsInstance(meta, ExtractionMeta)
        self.assertEqual(meta.model, "none")
        self.assertTrue(any("ANTHROPIC_API_KEY" in w for w in meta.warnings))

    def test_fallback_parses_kv_lines_and_product_dims(self):
        with mock.patch.dict("os.environ", _NO_KEY_ENV):
            profile, _meta = extract_prospect_profile(self._fallback_context())
        self.assertEqual(profile.company, "Acme Wellness")
        self.assertEqual(profile.website, "https://acme.example")
        self.assertEqual(profile.contact_email, "jane@acme.example")
        self.assertEqual(profile.monthly_order_volume, 5000)
        self.assertEqual(len(profile.products), 1)
        product = profile.products[0]
        self.assertEqual(product.name, "Super Serum")
        self.assertTrue(product.has_full_package_spec)
        self.assertEqual(product.length_in, 4.0)
        self.assertEqual(product.width_in, 4.0)
        self.assertEqual(product.height_in, 6.0)
        self.assertEqual(product.weight_lb, 1.2)
        self.assertEqual(product.monthly_units, 3000)

    def _mock_anthropic(self, response_text=None, raises=None):
        fake_module = types.ModuleType("anthropic")
        if raises is not None:
            fake_client = mock.Mock()
            fake_client.messages.create.side_effect = raises
        else:
            fake_message = types.SimpleNamespace(
                content=[types.SimpleNamespace(text=response_text)],
                model="claude-haiku-4-5-20251001",
                usage=types.SimpleNamespace(input_tokens=42, output_tokens=17),
            )
            fake_client = mock.Mock()
            fake_client.messages.create.return_value = fake_message
        fake_module.Anthropic = mock.Mock(return_value=fake_client)
        return mock.patch.dict(sys.modules, {"anthropic": fake_module})

    def test_mocked_llm_json_wrapped_in_prose_is_parsed(self):
        text = (
            "Sure! Here is the profile you asked for:\n"
            '{"company": "Acme Wellness", "brand": "Acme", '
            '"products": [{"name": "Super Serum", "length_in": 4, "width_in": 4, '
            '"height_in": 6, "weight_lb": 1.2, "monthly_units": 3000}], '
            '"monthly_order_volume": 5000, "source_confidence": "high"}\n'
            "Let me know if you need anything else."
        )
        with self._mock_anthropic(response_text=text):
            profile, meta = extract_prospect_profile("ctx", api_key="test-key")
        self.assertEqual(profile.company, "Acme Wellness")
        self.assertEqual(profile.source_confidence, "high")
        self.assertEqual(len(profile.products), 1)
        self.assertTrue(profile.products[0].has_full_package_spec)
        self.assertEqual(meta.model, "claude-haiku-4-5-20251001")
        self.assertEqual(meta.input_tokens, 42)
        self.assertEqual(meta.output_tokens, 17)

    def test_mocked_llm_raising_falls_back_without_raising(self):
        with self._mock_anthropic(raises=RuntimeError("api down")):
            profile, meta = extract_prospect_profile(
                "=== SALES NOTES ===\nCompany: Acme Wellness\n", api_key="test-key"
            )
        self.assertEqual(profile.company, "Acme Wellness")  # fallback parser ran
        self.assertTrue(any("failed" in w.lower() for w in meta.warnings))

    def test_absurd_dims_clamped_and_warned(self):
        text = (
            '{"company": "Big Stuff Co", "products": [{"name": "Mega Crate", '
            '"length_in": 200, "width_in": 200, "height_in": 200, "weight_lb": 900}]}'
        )
        with self._mock_anthropic(response_text=text):
            profile, meta = extract_prospect_profile("ctx", api_key="test-key")
        self.assertEqual(len(profile.products), 1)
        product = profile.products[0]
        self.assertIsNone(product.length_in)
        self.assertIsNone(product.weight_lb)
        self.assertFalse(product.has_full_package_spec)
        self.assertTrue(any("Mega Crate" in w and "dims" in w for w in meta.warnings))


def _matrix_with_product() -> RateMatrix:
    spec = ProductSpec(name="Super Serum", length_in=4, width_in=4, height_in=6, weight_lb=1.2)
    zone = ZoneRates(zone=4, dest_zip="30301", dest_label="Atlanta, GA",
                     quotes=(RateQuote(carrier="USPS", service="Ground Advantage", rate_usd=6.10),))
    return RateMatrix(products=(ProductRates(product=spec, zones=(zone,)),))


class SectionsTests(unittest.TestCase):
    def test_decide_sections_table(self):
        empty_matrix = RateMatrix(products=())
        full_matrix = _matrix_with_product()
        cases = [
            # (label, profile, matrix, expected overrides on SectionFlags defaults)
            (
                "bare profile, empty matrix -> only cover/about",
                ProspectProfile(),
                empty_matrix,
                {},
            ),
            (
                "matrix with products -> rate_matrix + zone_map",
                ProspectProfile(),
                full_matrix,
                {"rate_matrix": True, "zone_map": True},
            ),
            (
                "monthly_order_volume -> volume_economics",
                ProspectProfile(monthly_order_volume=5000),
                empty_matrix,
                {"volume_economics": True},
            ),
            (
                "product monthly_units alone -> volume_economics",
                ProspectProfile(products=(ProductSpec(name="X", monthly_units=300),)),
                empty_matrix,
                {"volume_economics": True},
            ),
            (
                "current_costs_note -> cost_comparison",
                ProspectProfile(current_costs_note="paying ~$9.80 avg per parcel"),
                empty_matrix,
                {"cost_comparison": True},
            ),
            (
                "destinations_note -> destinations",
                ProspectProfile(destinations_note="mostly West Coast, some Canada"),
                empty_matrix,
                {"destinations": True},
            ),
            (
                "everything on",
                ProspectProfile(
                    monthly_order_volume=5000,
                    current_costs_note="$9.80 avg",
                    destinations_note="West Coast",
                ),
                full_matrix,
                {
                    "rate_matrix": True,
                    "zone_map": True,
                    "volume_economics": True,
                    "cost_comparison": True,
                    "destinations": True,
                },
            ),
        ]
        for label, profile, matrix, overrides in cases:
            with self.subTest(label):
                expected = SectionFlags(**{
                    "cover": True, "about_anata": True,
                    "rate_matrix": False, "zone_map": False,
                    "volume_economics": False, "cost_comparison": False,
                    "destinations": False,
                    **overrides,
                })
                self.assertEqual(decide_sections(profile, matrix), expected)


if __name__ == "__main__":
    unittest.main()
