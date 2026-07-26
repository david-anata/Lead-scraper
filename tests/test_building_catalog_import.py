from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts.import_building_catalog import (
    CONFIRMATION,
    DEFAULT_CATALOG,
    apply_catalog,
    load_catalog,
    main,
    preview,
)


class BuildingCatalogImportTests(unittest.TestCase):
    def test_canva_catalog_is_review_gated_and_unpublished(self) -> None:
        catalog = load_catalog(DEFAULT_CATALOG)
        plan = preview(catalog)

        self.assertEqual(plan["writes"], {"spaces": 8, "offerings": 6})
        self.assertTrue(all(plan["invariants"].values()))
        self.assertEqual(plan["safety"]["review_status"], "owner_review_required")
        self.assertFalse(plan["safety"]["publish_allowed"])
        self.assertEqual(
            plan["evidence"]["bullpen-membership"]["evidence_price_reference"],
            "$150/month per person",
        )

    def test_loader_rejects_any_public_draft_record(self) -> None:
        raw = json.loads(DEFAULT_CATALOG.read_text(encoding="utf-8"))
        raw["offerings"][0]["is_published"] = True
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "unsafe.json"
            path.write_text(json.dumps(raw), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "unpublished"):
                load_catalog(path)

    @patch("scripts.import_building_catalog.api_put")
    def test_apply_writes_spaces_before_offerings(self, api_put) -> None:
        catalog = load_catalog(DEFAULT_CATALOG)
        result = apply_catalog(catalog, "https://agent.example", "secret")

        self.assertEqual(result["mode"], "applied")
        self.assertEqual(api_put.call_count, 14)
        first_paths = [call.args[1] for call in api_put.call_args_list[:8]]
        remaining_paths = [call.args[1] for call in api_put.call_args_list[8:]]
        self.assertTrue(all("/spaces/" in path for path in first_paths))
        self.assertTrue(all("/offerings/" in path for path in remaining_paths))

    def test_apply_requires_confirmation_and_api_key(self) -> None:
        self.assertEqual(main(["--apply"]), 1)
        with patch.dict("os.environ", {}, clear=True):
            self.assertEqual(
                main(["--apply", "--confirm", CONFIRMATION]),
                1,
            )


if __name__ == "__main__":
    unittest.main()
