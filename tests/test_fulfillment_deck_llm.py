from __future__ import annotations

import base64
import io
import json
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
    generate_narrative,
)
from sales_support_agent.services.fulfillment_deck.schema import (
    NarrativeBlock,
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


def _mock_anthropic(response_text=None, raises=None, model_name="claude-sonnet-4-6"):
    """Patch sys.modules['anthropic'] with a fake. Returns (patcher, fake_client)."""
    fake_module = types.ModuleType("anthropic")
    fake_client = mock.Mock()
    if raises is not None:
        fake_client.messages.create.side_effect = raises
    else:
        fake_message = types.SimpleNamespace(
            content=[types.SimpleNamespace(text=response_text)],
            model=model_name,
            usage=types.SimpleNamespace(input_tokens=42, output_tokens=17),
        )
        fake_client.messages.create.return_value = fake_message
    fake_module.Anthropic = mock.Mock(return_value=fake_client)
    return mock.patch.dict(sys.modules, {"anthropic": fake_module}), fake_client


class IntakeTests(unittest.TestCase):
    def test_notes_csv_and_xlsx_all_present(self):
        csv_bytes = b"product,length,width\nSuper Serum,4,4\n"
        context, attachments, warnings = build_extraction_context(
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
        self.assertEqual(attachments, [])
        self.assertEqual(warnings, [])

    def test_unsupported_extension_warns_and_skips(self):
        context, attachments, warnings = build_extraction_context(
            "notes", [("deck.pptx", b"not-a-deck")], ""
        )
        self.assertNotIn("deck.pptx", context)
        self.assertEqual(attachments, [])
        self.assertTrue(any("Unsupported file type: deck.pptx" in w for w in warnings))

    def test_broken_bytes_warn_instead_of_raising(self):
        context, attachments, warnings = build_extraction_context(
            "notes", [("corrupt.xlsx", b"\x00\x01not a zip at all")], ""
        )
        self.assertIn("=== SALES NOTES ===", context)
        self.assertEqual(attachments, [])
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
            context, attachments, warnings = build_extraction_context("", [], "https://acme.example")
        get.assert_called_once()
        self.assertEqual(get.call_args.kwargs.get("timeout"), 10)
        self.assertIn("=== WEBSITE: https://acme.example ===", context)
        self.assertIn("Acme Goods", context)
        self.assertIn("We ship serums nationwide", context)  # whitespace collapsed
        self.assertNotIn("var secret", context)
        self.assertNotIn("color:red", context)
        self.assertNotIn("<h1>", context)
        self.assertEqual(attachments, [])
        self.assertEqual(warnings, [])

    def test_website_fetch_failure_warns_not_raises(self):
        with mock.patch("requests.get", side_effect=ConnectionError("boom")):
            context, attachments, warnings = build_extraction_context(
                "notes here", [], "https://down.example"
            )
        self.assertIn("notes here", context)
        self.assertNotIn("=== WEBSITE", context)
        self.assertEqual(attachments, [])
        self.assertTrue(any("https://down.example" in w for w in warnings))


class IntakeAttachmentTests(unittest.TestCase):
    def test_small_pdf_becomes_attachment(self):
        pdf = b"%PDF-1.4 fake"
        context, attachments, warnings = build_extraction_context(
            "notes", [("brand-deck.pdf", pdf)], ""
        )
        self.assertEqual(len(attachments), 1)
        att = attachments[0]
        self.assertEqual(att["name"], "brand-deck.pdf")
        self.assertEqual(att["kind"], "pdf")
        self.assertEqual(att["media_type"], "application/pdf")
        self.assertEqual(base64.b64decode(att["data_b64"]), pdf)
        self.assertNotIn("brand-deck.pdf", context)  # not flattened into text
        self.assertEqual(warnings, [])

    def test_png_becomes_image_attachment(self):
        png = b"\x89PNG\r\n\x1a\nfakebytes"
        _context, attachments, warnings = build_extraction_context(
            "", [("product-shot.PNG", png)], ""
        )
        self.assertEqual(len(attachments), 1)
        att = attachments[0]
        self.assertEqual(att["kind"], "image")
        self.assertEqual(att["media_type"], "image/png")
        self.assertEqual(base64.b64decode(att["data_b64"]), png)
        self.assertEqual(warnings, [])

    def test_jpeg_media_type(self):
        _context, attachments, _warnings = build_extraction_context(
            "", [("photo.jpg", b"jpegbytes"), ("photo2.jpeg", b"jpegbytes2")], ""
        )
        self.assertEqual([a["media_type"] for a in attachments], ["image/jpeg", "image/jpeg"])

    def test_more_than_four_attachments_hits_budget(self):
        files = [(f"deck{i}.pdf", b"%PDF-1.4 fake") for i in range(5)]
        _context, attachments, warnings = build_extraction_context("", files, "")
        self.assertEqual(len(attachments), 4)
        self.assertEqual([a["name"] for a in attachments],
                         ["deck0.pdf", "deck1.pdf", "deck2.pdf", "deck3.pdf"])
        self.assertTrue(any("deck4.pdf" in w and "attachment budget" in w for w in warnings))

    def test_pdf_over_8mb_skipped_with_warning(self):
        big = b"%" * 9_000_000
        _context, attachments, warnings = build_extraction_context(
            "", [("huge.pdf", big)], ""
        )
        self.assertEqual(attachments, [])
        self.assertTrue(any("huge.pdf" in w and "skipped" in w for w in warnings))

    def test_total_byte_budget_enforced(self):
        # Three 7MB images: third pushes past 18MB total and is skipped.
        seven_mb = b"x" * 7_000_000
        files = [("a.png", seven_mb), ("b.png", seven_mb), ("c.png", seven_mb)]
        _context, attachments, warnings = build_extraction_context("", files, "")
        self.assertEqual(len(attachments), 2)
        self.assertTrue(any("c.png" in w and "attachment budget" in w for w in warnings))

    def test_csv_still_lands_in_context_alongside_attachments(self):
        csv_bytes = b"product,length\nSuper Serum,4\n"
        context, attachments, warnings = build_extraction_context(
            "notes", [("skus.csv", csv_bytes), ("deck.pdf", b"%PDF-1.4 fake")], ""
        )
        self.assertIn("=== FILE: skus.csv ===", context)
        self.assertIn("Super Serum,4", context)
        self.assertEqual(len(attachments), 1)
        self.assertEqual(attachments[0]["kind"], "pdf")
        self.assertEqual(warnings, [])


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
        self.assertFalse(product.dims_estimated)
        self.assertEqual(product.length_in, 4.0)
        self.assertEqual(product.width_in, 4.0)
        self.assertEqual(product.height_in, 6.0)
        self.assertEqual(product.weight_lb, 1.2)
        self.assertEqual(product.monthly_units, 3000)

    def test_fallback_regex_parses_cost_per_parcel(self):
        context = "=== SALES NOTES ===\nCompany: Acme\nCurrently paying $9.80/parcel with UPS\n"
        with mock.patch.dict("os.environ", _NO_KEY_ENV):
            profile, _meta = extract_prospect_profile(context)
        self.assertEqual(profile.current_cost_per_parcel_usd, 9.8)

    def test_fallback_regex_parses_per_label_phrasing(self):
        context = "around $10.50 per label these days"
        with mock.patch.dict("os.environ", _NO_KEY_ENV):
            profile, _meta = extract_prospect_profile(context)
        self.assertEqual(profile.current_cost_per_parcel_usd, 10.5)

    def test_mocked_llm_json_wrapped_in_prose_is_parsed(self):
        text = (
            "Sure! Here is the profile you asked for:\n"
            '{"company": "Acme Wellness", "brand": "Acme", '
            '"products": [{"name": "Super Serum", "length_in": 4, "width_in": 4, '
            '"height_in": 6, "weight_lb": 1.2, "monthly_units": 3000}], '
            '"monthly_order_volume": 5000, "source_confidence": "high"}\n'
            "Let me know if you need anything else."
        )
        patcher, _client = _mock_anthropic(response_text=text)
        with patcher:
            profile, meta = extract_prospect_profile("ctx", api_key="test-key")
        self.assertEqual(profile.company, "Acme Wellness")
        self.assertEqual(profile.source_confidence, "high")
        self.assertEqual(len(profile.products), 1)
        self.assertTrue(profile.products[0].has_full_package_spec)
        self.assertEqual(meta.model, "claude-sonnet-4-6")
        self.assertEqual(meta.input_tokens, 42)
        self.assertEqual(meta.output_tokens, 17)

    def test_mocked_llm_raising_falls_back_without_raising(self):
        patcher, _client = _mock_anthropic(raises=RuntimeError("api down"))
        with patcher:
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
        patcher, _client = _mock_anthropic(response_text=text)
        with patcher:
            profile, meta = extract_prospect_profile("ctx", api_key="test-key")
        self.assertEqual(len(profile.products), 1)
        product = profile.products[0]
        self.assertIsNone(product.length_in)
        self.assertIsNone(product.weight_lb)
        self.assertFalse(product.has_full_package_spec)
        self.assertTrue(any("Mega Crate" in w and "dims" in w for w in meta.warnings))

    def test_attachments_become_document_image_and_text_blocks(self):
        attachments = [
            {"name": "deck.pdf", "kind": "pdf", "media_type": "application/pdf",
             "data_b64": base64.b64encode(b"%PDF-1.4 fake").decode("ascii")},
            {"name": "shot.png", "kind": "image", "media_type": "image/png",
             "data_b64": base64.b64encode(b"pngbytes").decode("ascii")},
        ]
        patcher, client = _mock_anthropic(response_text='{"company": "Acme"}')
        with patcher:
            extract_prospect_profile("ctx text", attachments, api_key="test-key")
        kwargs = client.messages.create.call_args.kwargs
        content = kwargs["messages"][0]["content"]
        self.assertIsInstance(content, list)
        self.assertEqual(len(content), 3)
        doc = content[0]
        self.assertEqual(doc["type"], "document")
        self.assertEqual(doc["source"]["type"], "base64")
        self.assertEqual(doc["source"]["media_type"], "application/pdf")
        self.assertEqual(doc["source"]["data"], attachments[0]["data_b64"])
        img = content[1]
        self.assertEqual(img["type"], "image")
        self.assertEqual(img["source"]["media_type"], "image/png")
        self.assertEqual(img["source"]["data"], attachments[1]["data_b64"])
        self.assertEqual(content[2], {"type": "text", "text": "ctx text"})

    def test_no_attachments_keeps_plain_string_content(self):
        patcher, client = _mock_anthropic(response_text='{"company": "Acme"}')
        with patcher:
            extract_prospect_profile("just text", api_key="test-key")
        kwargs = client.messages.create.call_args.kwargs
        self.assertEqual(kwargs["messages"][0]["content"], "just text")

    def test_estimated_dims_flagged_and_warned(self):
        text = json.dumps({
            "company": "GlowCo",
            "products": [
                {"name": "Face Mist", "length_in": 4, "width_in": 4, "height_in": 8,
                 "weight_lb": 0.8, "dims_estimated": True},
                {"name": "Serum", "length_in": 3, "width_in": 3, "height_in": 5,
                 "weight_lb": 0.5, "dims_estimated": False},
            ],
        })
        patcher, _client = _mock_anthropic(response_text=text)
        with patcher:
            profile, meta = extract_prospect_profile("ctx", api_key="test-key")
        self.assertTrue(profile.products[0].dims_estimated)
        self.assertFalse(profile.products[1].dims_estimated)
        estimate_warnings = [w for w in meta.warnings if "Estimated package specs" in w]
        self.assertEqual(len(estimate_warnings), 1)
        self.assertIn("Face Mist", estimate_warnings[0])
        self.assertNotIn("Serum", estimate_warnings[0])
        self.assertIn("confirm with prospect", estimate_warnings[0])
        # Fully-specced (even if estimated) products no longer get the missing-dims warning.
        self.assertFalse(any("No usable dims" in w for w in meta.warnings))

    def test_truly_null_dims_still_get_missing_warning(self):
        text = json.dumps({
            "company": "MysteryCo",
            "products": [{"name": "Unknown Thing", "length_in": None, "width_in": None,
                          "height_in": None, "weight_lb": None}],
        })
        patcher, _client = _mock_anthropic(response_text=text)
        with patcher:
            _profile, meta = extract_prospect_profile("ctx", api_key="test-key")
        self.assertTrue(any("No usable dims" in w and "Unknown Thing" in w for w in meta.warnings))

    def test_cost_per_parcel_from_llm_json(self):
        text = '{"company": "Acme", "current_cost_per_parcel_usd": 9.8}'
        patcher, _client = _mock_anthropic(response_text=text)
        with patcher:
            profile, _meta = extract_prospect_profile("ctx", api_key="test-key")
        self.assertEqual(profile.current_cost_per_parcel_usd, 9.8)

    def test_default_model_is_sonnet(self):
        patcher, client = _mock_anthropic(response_text='{"company": "Acme"}')
        with patcher, mock.patch.dict("os.environ", {"FULFILLMENT_DECK_MODEL": ""}):
            extract_prospect_profile("ctx", api_key="test-key")
        self.assertEqual(client.messages.create.call_args.kwargs["model"], "claude-sonnet-4-6")

    def test_model_env_override(self):
        patcher, client = _mock_anthropic(response_text='{"company": "Acme"}')
        with patcher, mock.patch.dict("os.environ", {"FULFILLMENT_DECK_MODEL": "claude-haiku-4-5"}):
            extract_prospect_profile("ctx", api_key="test-key")
        self.assertEqual(client.messages.create.call_args.kwargs["model"], "claude-haiku-4-5")

    def test_explicit_model_arg_beats_env(self):
        patcher, client = _mock_anthropic(response_text='{"company": "Acme"}')
        with patcher, mock.patch.dict("os.environ", {"FULFILLMENT_DECK_MODEL": "claude-haiku-4-5"}):
            extract_prospect_profile("ctx", api_key="test-key", model="claude-opus-4-6")
        self.assertEqual(client.messages.create.call_args.kwargs["model"], "claude-opus-4-6")


def _matrix_with_product() -> RateMatrix:
    spec = ProductSpec(name="Super Serum", length_in=4, width_in=4, height_in=6, weight_lb=1.2)
    zone = ZoneRates(zone=4, dest_zip="30301", dest_label="Atlanta, GA",
                     quotes=(RateQuote(carrier="USPS", service="Ground Advantage", rate_usd=6.10),))
    return RateMatrix(products=(ProductRates(product=spec, zones=(zone,)),))


class NarrativeTests(unittest.TestCase):
    SAVINGS = {
        "current_per_parcel": 9.8,
        "anata_blended_per_parcel": 7.3,
        "monthly_orders": 3000,
        "monthly_savings": 7500.0,
        "annual_savings": 90000.0,
    }

    def _profile(self) -> ProspectProfile:
        return ProspectProfile(
            company="GlowCo Inc",
            brand="GlowCo",
            monthly_order_volume=3000,
            current_carrier="UPS",
            products=(ProductSpec(name="Serum", length_in=4, width_in=4, height_in=6, weight_lb=1.2),),
        )

    def test_fallback_mentions_brand_and_cites_savings_numbers(self):
        with mock.patch.dict("os.environ", _NO_KEY_ENV):
            block = generate_narrative(self._profile(), _matrix_with_product(), self.SAVINGS)
        self.assertIsInstance(block, NarrativeBlock)
        self.assertEqual(block.model, "none")
        self.assertIn("GlowCo", block.executive_summary)
        self.assertTrue(block.executive_summary.strip())
        self.assertIn("$9.80", block.savings_text)
        self.assertIn("$7.30", block.savings_text)
        self.assertIn("7,500", block.savings_text)
        self.assertIn("90,000", block.savings_text)
        self.assertIn("3,000", block.savings_text)
        self.assertGreaterEqual(len(block.bullets), 2)
        self.assertLessEqual(len(block.bullets), 4)

    def test_fallback_savings_none_gives_empty_savings_text(self):
        with mock.patch.dict("os.environ", _NO_KEY_ENV):
            block = generate_narrative(self._profile(), _matrix_with_product(), None)
        self.assertEqual(block.savings_text, "")
        self.assertTrue(block.executive_summary.strip())
        self.assertGreaterEqual(len(block.bullets), 2)

    def test_fallback_bare_profile_never_blank(self):
        with mock.patch.dict("os.environ", _NO_KEY_ENV):
            block = generate_narrative(ProspectProfile(), RateMatrix(products=()), None)
        self.assertTrue(block.executive_summary.strip())
        self.assertGreaterEqual(len(block.bullets), 2)
        self.assertEqual(block.model, "none")

    def test_mocked_llm_populates_fields_and_records_model(self):
        text = json.dumps({
            "executive_summary": "GlowCo ships 3,000 orders a month of skincare.",
            "savings_text": "You would save $7,500/month.",
            "bullets": ["Fast zone 2 coverage", "Multi-carrier rate shopping"],
        })
        patcher, client = _mock_anthropic(response_text=text)
        with patcher:
            block = generate_narrative(
                self._profile(), _matrix_with_product(), self.SAVINGS, api_key="test-key"
            )
        self.assertEqual(block.executive_summary, "GlowCo ships 3,000 orders a month of skincare.")
        self.assertEqual(block.savings_text, "You would save $7,500/month.")
        self.assertEqual(block.bullets, ("Fast zone 2 coverage", "Multi-carrier rate shopping"))
        self.assertEqual(block.model, "claude-sonnet-4-6")
        self.assertEqual(block.input_tokens, 42)
        self.assertEqual(block.output_tokens, 17)
        # The prompt got the already-computed facts, not raw notes.
        sent = client.messages.create.call_args.kwargs["messages"][0]["content"]
        self.assertIn("GlowCo", sent)
        self.assertIn("anata_blended_per_parcel", sent)

    def test_mocked_llm_savings_none_forces_empty_savings_text(self):
        text = json.dumps({
            "executive_summary": "GlowCo ships a lot.",
            "savings_text": "You save $999,999!",  # hallucinated — must be dropped
            "bullets": ["a", "b"],
        })
        patcher, _client = _mock_anthropic(response_text=text)
        with patcher:
            block = generate_narrative(
                self._profile(), _matrix_with_product(), None, api_key="test-key"
            )
        self.assertEqual(block.savings_text, "")

    def test_mocked_llm_raising_falls_back_never_raises(self):
        patcher, _client = _mock_anthropic(raises=RuntimeError("api down"))
        with patcher:
            block = generate_narrative(
                self._profile(), _matrix_with_product(), self.SAVINGS, api_key="test-key"
            )
        self.assertEqual(block.model, "none")
        self.assertIn("GlowCo", block.executive_summary)
        self.assertIn("$9.80", block.savings_text)

    def test_mocked_llm_unparseable_falls_back(self):
        patcher, _client = _mock_anthropic(response_text="not json at all")
        with patcher:
            block = generate_narrative(
                self._profile(), _matrix_with_product(), None, api_key="test-key"
            )
        self.assertEqual(block.model, "none")
        self.assertTrue(block.executive_summary.strip())


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
