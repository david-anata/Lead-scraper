"""Tests for the Amazon brand control check (outbound_amazon.py).

This check is the one that reaches a live prospect with a claim about their own
storefront, so the tests here are mostly about the ways we could be confidently
wrong: looking at the wrong country's store, matching the wrong brand, counting a
legitimate distributor as a hijacker, comparing a third party price against a
struck through list price, or putting a brittle number in an opening line.

Everything is offline. The network side is a fake client that answers the three
questions the puller can ask: what comes up for a search term, who is selling on
a listing, and who is advertising against the brand name.
"""

from __future__ import annotations

import unittest

import outbound_amazon as oa


class _Money(float):
    """A price that reads the same whether the module expects 50.18 or {"value": 50.18}."""

    def get(self, key, default=None):
        return self[key] if key in ("value", "amount", "raw") else default

    def __getitem__(self, key):
        if key == "raw":
            return f"{float(self):.2f}"
        if key in ("value", "amount"):
            return float(self)
        raise KeyError(key)

    def __contains__(self, key):
        return key in ("value", "amount", "raw")


class _Name(str):
    """A seller name that also answers to {"name": ...}."""

    _KEYS = ("name", "id", "raw", "brand", "title", "seller_name")

    def get(self, key, default=None):
        return str(self) if key in self._KEYS else default

    def __getitem__(self, key):
        if isinstance(key, str):
            if key in self._KEYS:
                return str(self)
            raise KeyError(key)
        return str.__getitem__(self, key)


class _Condition(_Name):
    def get(self, key, default=None):
        if key == "is_new":
            return str(self).lower().startswith("new")
        return str(self) if key in ("title", "name", "raw") else default

    def __getitem__(self, key):
        if isinstance(key, str):
            if key == "is_new":
                return str(self).lower().startswith("new")
            if key in ("title", "name", "raw"):
                return str(self)
            raise KeyError(key)
        return str.__getitem__(self, key)


def _payload(items, keys):
    """An API response envelope, keyed under every plausible name for the list.

    The items live once, so a module that reads "search_results" and one that
    reads "results" both see the same rows rather than twice as many.
    """
    body = {"request_info": {"success": True}}
    rows = list(items)
    for key in keys:
        body[key] = rows
    return body


def _search_payload(items):
    return _payload(items, ("search_results", "results", "organic_results", "products", "listings"))


def _offers_payload(items):
    return _payload(items, ("offers", "product_offers", "offer_results", "sellers"))


class _Listing(dict):
    """A search result that answers to whichever seller key the module reads.

    The offers live once, so a module that reads "offers" and a module that reads
    "sellers" both see three sellers rather than six.
    """

    _SELLER_KEYS = ("sellers", "other_sellers", "third_party_sellers", "seller_offers")

    def __missing__(self, key):
        if key in self._SELLER_KEYS:
            return dict.get(self, "offers", [])
        raise KeyError(key)

    def get(self, key, default=None):
        if key in self._SELLER_KEYS and "offers" in self:
            return dict.get(self, "offers", default)
        return dict.get(self, key, default)

    def __contains__(self, key):
        if key in self._SELLER_KEYS:
            return dict.__contains__(self, "offers")
        return dict.__contains__(self, key)


def _offer(seller, price, condition="new"):
    return {
        "seller": _Name(seller),
        "seller_name": _Name(seller),
        "name": _Name(seller),
        "merchant": _Name(seller),
        "price": _Money(price),
        "offer_price": _Money(price),
        "condition": _Condition(condition),
    }


def _listing(**over):
    """The real Rho shape: a brand sold listing with a struck through list price."""
    base = _Listing(
        asin="B0RHO00001",
        title="Rho NAD+ 3-Pack Daily Capsules",
        name="Rho NAD+ 3-Pack Daily Capsules",
        brand="Rho",
        brand_name="Rho",
        category="Health & Household",
        categories=["Health & Household"],
        # what the brand actually sells at right now
        price=_Money(50.18),
        brand_price=_Money(50.18),
        current_price=_Money(50.18),
        buybox_price=_Money(50.18),
        # the struck through number Amazon shows next to it
        list_price=_Money(55.95),
        was_price=_Money(55.95),
        strikethrough_price=_Money(55.95),
        in_stock=True,
        stock=40,
        sponsored=False,
        offers=[_offer("Rho", 50.18)],
    )
    base.update(over)
    return base


class FakeClient:
    """Stands in for the live Amazon fetcher.

    Whatever the module names its methods, they resolve to one of three answers,
    so the tests do not silently pass by never being called.
    """

    def __init__(self, listings=None, sponsored=None, error=None):
        self.listings = list(listings or [])
        self.sponsored_names = list(sponsored or [])
        self.error = error
        self.calls = []

    def _boom(self, name):
        self.calls.append(name)
        if self.error:
            raise self.error

    def _search(self, *args, **kw):
        self._boom("search")
        return _search_payload(self.listings)

    def _find(self, args, kw):
        wanted = [a for a in list(args) + list(kw.values()) if isinstance(a, str)]
        for listing in self.listings:
            if listing.get("asin") in wanted:
                return listing
        return None

    def _offers(self, *args, **kw):
        self._boom("offers")
        found = self._find(args, kw)
        if found is not None:
            return _offers_payload(found.get("offers", []))
        out = []
        for listing in self.listings:
            out.extend(listing.get("offers", []))
        return _offers_payload(out)

    def _sponsored(self, *args, **kw):
        self._boom("sponsored")
        return _payload([_Name(n) for n in self.sponsored_names],
                        ("sponsored_products", "ads", "results"))

    def _product(self, *args, **kw):
        self._boom("product")
        found = self._find(args, kw)
        if found is None:
            found = self.listings[0] if self.listings else _Listing()
        body = _Listing(found)
        return {"request_info": {"success": True}, "product": body}

    def __getattr__(self, name):
        if name.startswith("_"):
            raise AttributeError(name)
        lowered = name.lower()
        if "sponsor" in lowered or "advert" in lowered or "ads" in lowered:
            return self._sponsored
        if "offer" in lowered or "seller" in lowered:
            return self._offers
        if "product" in lowered and "products" not in lowered:
            return self._product
        return self._search


def _settings(**over):
    base = {
        "amazon.enabled": True,
        "amazon.min_unknown_sellers": 2,
        "amazon.max_listings_checked": 5,
        "amazon.max_name_variants": 3,
        "amazon.min_confidence": "medium",
        "amazon.price_erosion_min_pct": 2,
        "amazon.finding_max_age_days": 30,
    }
    base.update(over)
    return base


def _template_parts(key):
    """Split a reason template on {product} so tests survive a retune of the words."""
    head, _, tail = oa._REASON_TEMPLATES[key].partition("{product}")
    return head, tail


def _reason_product(reason, key):
    """Pull back out whichever product name the module dropped into the line."""
    head, tail = _template_parts(key)
    return reason[len(head):len(reason) - len(tail)]


_HOME_STORE = {"amazon.follow_country_marketplace": True}


class TestMarketplaceRouting(unittest.TestCase):
    def test_everyone_is_checked_against_the_us_store_by_default(self):
        """We sell US marketplace expansion, so amazon.com is the question we are
        asking of every brand. A UK brand missing from amazon.com is a lead, not
        a mistake, and routing it to amazon.co.uk would hide exactly that."""
        for code in ("GB", "UK", "gb", "AU", "CA", "DE", ""):
            self.assertEqual(oa.marketplace_for(code), "amazon.com")

    def test_home_store_routing_is_available_but_off(self):
        """Kept as a switch because 'how are they doing where they already sell'
        is a real question, just not the one this campaign asks."""
        self.assertIs(oa.AMAZON_DEFAULT_SETTINGS["amazon.follow_country_marketplace"], False)
        self.assertEqual(oa.marketplace_for("GB", _HOME_STORE), "amazon.co.uk")
        self.assertEqual(oa.marketplace_for("UK", _HOME_STORE), "amazon.co.uk")
        self.assertEqual(oa.marketplace_for("AU", _HOME_STORE), "amazon.com.au")
        self.assertEqual(oa.marketplace_for("CA", _HOME_STORE), "amazon.ca")

    def test_unknown_country_falls_back_to_the_us_store(self):
        self.assertEqual(oa.marketplace_for("DE", _HOME_STORE), "amazon.com")
        self.assertEqual(oa.marketplace_for("", _HOME_STORE), "amazon.com")

    def test_a_string_false_from_the_settings_form_reads_as_off(self):
        """Settings come back from a web form as text, so "false" must not be
        truthy or every overseas brand silently switches store."""
        self.assertEqual(
            oa.marketplace_for("GB", {"amazon.follow_country_marketplace": "false"}),
            "amazon.com")

    def test_marketplace_table_covers_the_countries_we_sell_into(self):
        for code in ("US", "GB", "UK", "CA", "AU"):
            self.assertIn(code, oa.AMAZON_MARKETPLACES)


class TestBrandCandidates(unittest.TestCase):
    def test_domain_stem_comes_first(self):
        """Merchant names carry junk. This is a real record: the merchant name is
        'up Charge - WearForm.com' and searching that on Amazon returns nothing,
        while the domain stem 'wearform' finds the brand."""
        out = oa.brand_candidates("up Charge - WearForm.com", "wearform.com")
        self.assertEqual(out[0], "wearform")

    def test_www_and_tld_are_stripped_from_the_stem(self):
        out = oa.brand_candidates("Whatever", "www.wearform.com")
        self.assertEqual(out[0], "wearform")

    def test_leading_be_is_stripped_so_behuppy_also_tries_huppy(self):
        """behuppy.com sells under the name Huppy on Amazon. Searching only the
        domain stem finds nothing at all and we would call them absent."""
        out = oa.brand_candidates("Huppy", "behuppy.com")
        self.assertIn("behuppy", out)
        self.assertIn("huppy", out)

    def test_trailing_region_and_entity_tokens_are_dropped(self):
        """'Glow Recipe UK Ltd' is never the Amazon brand name. The legal suffix
        turns a search that would have matched into a zero result absent claim."""
        out = oa.brand_candidates("Glow Recipe UK Ltd", "glowrecipeshop.com")
        self.assertEqual(out[0], "glowrecipeshop")
        self.assertIn("glow recipe", out)

    def test_candidates_are_lowercased_deduped_and_capped(self):
        out = oa.brand_candidates("WearForm", "wearform.com", limit=2)
        self.assertLessEqual(len(out), 2)
        self.assertEqual(out, [c.lower() for c in out])
        self.assertEqual(len(out), len(set(out)))
        self.assertNotIn("", out)

    def test_missing_merchant_name_still_gives_the_stem(self):
        self.assertEqual(oa.brand_candidates("", "wearform.com"), ["wearform"])


class TestSellerClassification(unittest.TestCase):
    def test_real_distributors_are_retailers_not_hijackers(self):
        """iHerb is an authorized Rho distributor. Counting it as an unknown third
        party seller means we email a brand about a leak that is their own
        distribution deal, which ends the conversation immediately."""
        self.assertEqual(oa.classify_seller("iHerb", ["rho"]), "retailer")
        self.assertEqual(oa.classify_seller("iHerb.com", ["rho"]), "retailer")
        self.assertEqual(oa.classify_seller("Walmart", ["rho"]), "retailer")

    def test_the_brand_selling_its_own_listing_is_the_brand(self):
        self.assertEqual(oa.classify_seller("Rho", ["rho"]), "brand")
        self.assertEqual(oa.classify_seller("RHO", ["rho"]), "brand")

    def test_used_condition_is_never_a_leak(self):
        """A used copy of a product is a resale, not an unauthorized seller, and
        opening with it makes us look like we cannot read a listing."""
        self.assertEqual(oa.classify_seller("ZenithGoods Trading", ["rho"], condition="used"), "used")
        self.assertEqual(oa.classify_seller("ZenithGoods Trading", ["rho"], condition="Used - Like New"), "used")
        self.assertEqual(oa.classify_seller("iHerb", ["rho"], condition="refurbished"), "used")

    def test_anyone_else_is_unknown(self):
        self.assertEqual(oa.classify_seller("ZenithGoods Trading", ["rho"]), "unknown")
        self.assertEqual(oa.classify_seller("ZenithGoods Trading", ["rho"], condition="New"), "unknown")

    def test_known_retailers_include_the_ones_we_keep_seeing(self):
        for name in ("iherb", "walmart", "target", "vitamin shoppe", "boots", "holland & barrett"):
            self.assertIn(name, oa.KNOWN_RETAILERS)


class TestMatchConfidence(unittest.TestCase):
    def test_brand_field_match_is_high(self):
        result = {"brand": "Rho", "title": "Rho NAD+ Daily Capsules", "category": "Health & Household"}
        self.assertEqual(oa.match_confidence(["rho"], result), "high")

    def test_substring_only_is_low_so_the_lead_is_skipped(self):
        """'Nutra Bundle featuring rho style capsules' contains the brand name but
        is somebody else's product. Emailing a brand about a listing that is not
        theirs is unrecoverable, so a substring alone has to score low."""
        result = {"brand": "Nutra Labs", "title": "Nutra Bundle featuring rho style capsules"}
        self.assertEqual(oa.match_confidence(["rho"], result), "low")

    def test_one_word_brand_never_returns_medium(self):
        """A one word name like 'huppy' appears at the front of plenty of titles
        that are not theirs, so a title prefix on its own is not enough."""
        result = {"title": "Huppy Toothpaste Tablets Refill"}
        self.assertNotEqual(oa.match_confidence(["huppy"], result), "medium")

    def test_two_common_words_never_returns_medium(self):
        result = {"title": "The Shop Cotton Tote Bag"}
        self.assertNotEqual(oa.match_confidence(["the shop"], result), "medium")

    def test_distinctive_multi_word_title_prefix_is_medium(self):
        result = {"title": "WearForm Apparel Compression Sleeve"}
        self.assertEqual(oa.match_confidence(["wearform apparel"], result), "medium")


class TestPreciseFigureGate(unittest.TestCase):
    def test_currency_symbols_are_blocked(self):
        """A price we scraped an hour ago is wrong by the time they read it, and a
        wrong price in the first line is the fastest way to lose the reply."""
        for line in ("Someone is listing at $48.00", "Someone is listing at £48", "Someone is listing at €48"):
            with self.assertRaises(oa.PreciseFigureError):
                oa.assert_no_precise_figures(line)

    def test_counts_above_two_are_blocked(self):
        with self.assertRaises(oa.PreciseFigureError):
            oa.assert_no_precise_figures("There are 7 other sellers on your listing")

    def test_a_digit_inside_the_product_name_is_allowed(self):
        """Their product is literally called 'NAD+ 3-Pack'. Naming their own
        product back to them is not a brittle claim, so the product name comes
        out of the line before the gate looks at it."""
        oa.assert_no_precise_figures(
            "Someone is listing your NAD+ 3-Pack below your own price on Amazon.",
            product="NAD+ 3-Pack",
        )

    def test_vague_language_passes(self):
        oa.assert_no_precise_figures("There are a handful of other sellers on your listing")
        oa.assert_no_precise_figures("A couple of 2 sellers showed up")

    def test_error_is_a_value_error(self):
        self.assertTrue(issubclass(oa.PreciseFigureError, ValueError))


class TestReasonTemplates(unittest.TestCase):
    def test_every_template_survives_the_precise_figure_gate(self):
        """Templates are edited by hand. One number slipping into one of them ships
        a brittle claim to every prospect that trigger fires for."""
        for key, tmpl in oa._REASON_TEMPLATES.items():
            line = tmpl.format(product="Daily Capsules")
            oa.assert_no_precise_figures(line, product="Daily Capsules")
            self.assertTrue(line.strip().endswith("?"), key)


class TestBrandControlAbsent(unittest.TestCase):
    def test_zero_results_is_an_absent_finding_not_a_skip(self):
        """Nothing on Amazon under their own name is the strongest thing we can
        open with. An empty search result set used to look like a failed pull and
        get thrown away."""
        client = FakeClient(listings=[])
        out = oa.brand_control("Rho", "rho.com", "US", client=client, settings=_settings())
        self.assertTrue(out["findings"]["absent"])
        self.assertEqual(out["skipped_reason"], "")
        self.assertEqual(out["reason"], oa._REASON_TEMPLATES["absent"])
        self.assertGreaterEqual(out["score"], 4)
        self.assertIn(out["tier"], ("A", "B"))

    def test_an_overseas_brand_is_still_checked_against_the_us_store(self):
        """A UK brand absent from amazon.com is the pitch, not an error. This is
        the whole reason we do not follow them to their home marketplace."""
        client = FakeClient(listings=[])
        out = oa.brand_control("Rho", "rho.com", "GB", client=client, settings=_settings())
        self.assertEqual(out["marketplace"], "amazon.com")
        self.assertTrue(out["findings"]["absent"])
        self.assertTrue(out["checked_at"])

    def test_the_home_store_switch_still_reaches_brand_control(self):
        client = FakeClient(listings=[])
        out = oa.brand_control("Rho", "rho.com", "GB", client=client,
                               settings=_settings(**_HOME_STORE))
        self.assertEqual(out["marketplace"], "amazon.co.uk")


class TestBrandControlConfidence(unittest.TestCase):
    def test_low_confidence_skips_with_no_reason(self):
        """A weak match must never produce an opening line. Better to send nothing
        than to tell a brand about a listing that belongs to someone else."""
        other = _listing(
            asin="B0OTHER001",
            title="Nutra Bundle featuring rho style capsules",
            name="Nutra Bundle featuring rho style capsules",
            brand="Nutra Labs",
            brand_name="Nutra Labs",
            offers=[_offer("ZenithGoods Trading", 30.00), _offer("Prime Nutra Deals", 31.00)],
        )
        client = FakeClient(listings=[other])
        out = oa.brand_control("Rho", "rho.com", "US", client=client, settings=_settings())
        self.assertNotEqual(out["skipped_reason"], "")
        self.assertEqual(out["reason"], "")
        self.assertIn(out["confidence"], ("low", "none"))


class TestBrandControlSellers(unittest.TestCase):
    def _run(self, offers, **kw):
        client = FakeClient(listings=[_listing(offers=[_offer("Rho", 50.18)] + offers)])
        return oa.brand_control("Rho", "rho.com", "US", client=client, settings=_settings(**kw))

    def test_unknown_sellers_drive_the_leak_score(self):
        out = self._run([
            _offer("ZenithGoods Trading", 51.00),
            _offer("Prime Nutra Deals", 52.00),
            _offer("Vital Stock Supply", 53.00),
        ])
        listing = out["findings"]["listings"][0]
        self.assertEqual(listing["sellers_unknown"], 3)
        head, tail = _template_parts("resellers")
        self.assertTrue(out["reason"].startswith(head))
        self.assertTrue(out["reason"].endswith(tail))
        oa.assert_no_precise_figures(out["reason"], product=_reason_product(out["reason"], "resellers"))

    def test_only_unknown_sellers_count_toward_the_leak(self):
        """Three authorized retailers on a listing is healthy distribution. If they
        scored the same as three unknown sellers we would send a hijacking email to
        a brand whose distribution is working exactly as intended."""
        retailers = self._run([
            _offer("iHerb", 51.00),
            _offer("Walmart", 52.00),
            _offer("Vitamin Shoppe", 53.00),
        ])
        unknowns = self._run([
            _offer("ZenithGoods Trading", 51.00),
            _offer("Prime Nutra Deals", 52.00),
            _offer("Vital Stock Supply", 53.00),
        ])
        listing = retailers["findings"]["listings"][0]
        self.assertEqual(listing["sellers_unknown"], 0)
        self.assertEqual(listing["sellers_retailer"], 3)
        self.assertLess(retailers["score"], unknowns["score"])
        head, _tail = _template_parts("resellers")
        self.assertFalse(retailers["reason"].startswith(head))

    def test_used_offers_do_not_count_as_a_leak(self):
        out = self._run([
            _offer("ZenithGoods Trading", 20.00, condition="used - good"),
            _offer("Prime Nutra Deals", 21.00, condition="used - acceptable"),
        ])
        listing = out["findings"]["listings"][0]
        self.assertEqual(listing["sellers_unknown"], 0)
        self.assertEqual(listing["sellers_used"], 2)


class TestBrandControlPriceErosion(unittest.TestCase):
    """The real Rho listing: they sell at 50.18 against a 55.95 struck through
    list price, and the cheapest unknown third party sits at 48.00."""

    def _run(self):
        listing = _listing(offers=[_offer("Rho", 50.18), _offer("ZenithGoods Trading", 48.00)])
        client = FakeClient(listings=[listing])
        return oa.brand_control("Rho", "rho.com", "US", client=client, settings=_settings())

    def test_erosion_is_measured_against_the_brands_own_price(self):
        """Measuring against the struck through list price turns a 4 percent
        undercut into a 14 percent one. The brand knows their own numbers, so an
        inflated claim in the opening line ends the conversation."""
        found = self._run()["findings"]["listings"][0]
        self.assertAlmostEqual(found["brand_price"], 50.18, places=2)
        self.assertAlmostEqual(found["cheapest"], 48.00, places=2)
        pct = (found["brand_price"] - found["cheapest"]) / found["brand_price"] * 100.0
        self.assertGreater(pct, 3.0)
        self.assertLess(pct, 6.0)
        self.assertFalse(13.0 < pct < 15.0)

    def test_the_list_price_never_becomes_the_brand_price(self):
        found = self._run()["findings"]["listings"][0]
        self.assertNotAlmostEqual(found["brand_price"], 55.95, places=2)

    def test_undercut_reason_names_no_number(self):
        out = self._run()
        head, tail = _template_parts("underpriced")
        self.assertTrue(out["reason"].startswith(head))
        self.assertTrue(out["reason"].endswith(tail))
        oa.assert_no_precise_figures(out["reason"], product=_reason_product(out["reason"], "underpriced"))


class TestBrandControlSponsored(unittest.TestCase):
    def test_sponsored_competitors_produce_a_clean_reason(self):
        rival = _Listing(
            asin="B0RIVAL001",
            title="Rival Labs Co NAD Booster",
            name="Rival Labs Co NAD Booster",
            brand="Rival Labs Co",
            brand_name="Rival Labs Co",
            sponsored=True,
            in_stock=True,
            price=39.00,
            offers=[],
        )
        client = FakeClient(listings=[_listing(), rival], sponsored=["Rival Labs Co", "Other Brand"])
        out = oa.brand_control("Rho", "rho.com", "US", client=client, settings=_settings())
        self.assertTrue(out["findings"]["sponsored_competitors"])
        self.assertEqual(out["reason"], oa._REASON_TEMPLATES["sponsored"])
        oa.assert_no_precise_figures(out["reason"])


class TestBrandControlDegrades(unittest.TestCase):
    def test_a_network_error_becomes_a_skip(self):
        """A dead fetch must never kill the run, and it must never be read as the
        brand being absent from Amazon."""
        client = FakeClient(listings=[_listing()], error=RuntimeError("connection reset"))
        out = oa.brand_control("Rho", "rho.com", "US", client=client, settings=_settings())
        self.assertNotEqual(out["skipped_reason"], "")
        self.assertEqual(out["reason"], "")
        self.assertFalse(out["findings"]["absent"])

    def test_disabled_check_skips_without_a_reason(self):
        client = FakeClient(listings=[_listing()])
        out = oa.brand_control("Rho", "rho.com", "US", client=client, settings=_settings(**{"amazon.enabled": False}))
        self.assertNotEqual(out["skipped_reason"], "")
        self.assertEqual(out["reason"], "")

    def test_result_shape_is_always_complete(self):
        """Callers write these straight into a lead row, so a missing key is a 500
        on the outbound page rather than a bad line in an email."""
        client = FakeClient(listings=[_listing()])
        out = oa.brand_control("Rho", "rho.com", "US", client=client, settings=_settings())
        for key in ("score", "tier", "reason", "signals", "confidence", "findings",
                    "excluded", "skipped_reason", "checked_at", "marketplace"):
            self.assertIn(key, out)
        self.assertIn(out["tier"], ("A", "B", "C", "X"))
        self.assertIn(out["confidence"], ("high", "medium", "low", "none"))
        self.assertIsInstance(out["signals"], list)
        for key in ("listings", "sponsored_competitors", "absent"):
            self.assertIn(key, out["findings"])


if __name__ == "__main__":
    unittest.main()


class ClayFactsTests(unittest.TestCase):
    """Clay writes the sentence now, so the facts we hand it must be safe.

    The no-precise-figures rule used to be enforced in code on the finished
    line. Once an AI writes the line, the only place left to enforce it is the
    input: send words, not counts, and nothing precise can leak.
    """

    def _result(self, **over):
        base = {
            "marketplace": "amazon.com", "confidence": "high", "skipped_reason": "",
            "findings": {
                "absent": False,
                "sponsored_competitors": ["Cata-Kor", "Toniiq"],
                "listings": [{"title": "Rho Nutrition Liposomal NAD+ Liquid Supplement",
                              "brand_price": 50.18, "cheapest": 48.00,
                              "sellers_unknown": 18, "sellers_retailer": 1,
                              "sellers_used": 0, "in_stock": True}],
            },
        }
        base.update(over)
        return base

    def test_no_count_or_price_is_ever_handed_to_clay(self):
        """The whole guarantee. A model given "18" will eventually write "18",
        and a brand that reads 19 and counts 14 stops reading there."""
        facts = oa.clay_facts(self._result())
        for key, value in facts.items():
            if key == "amz_marketplace":
                continue
            self.assertFalse(any(ch.isdigit() for ch in str(value)),
                             f"{key} leaked a figure to Clay: {value!r}")

    def test_counts_arrive_as_words(self):
        facts = oa.clay_facts(self._result())
        self.assertEqual(facts["amz_sellers_band"], "a lot of other sellers")
        self.assertEqual(facts["amz_rivals_on_name"], "a few competitors")

    def test_bands_move_with_the_count(self):
        def band(n):
            r = self._result()
            r["findings"]["listings"][0]["sellers_unknown"] = n
            return oa.clay_facts(r)["amz_sellers_band"]
        self.assertEqual(band(0), "")
        self.assertEqual(band(1), "another seller")
        self.assertEqual(band(3), "a couple of other sellers")
        self.assertEqual(band(9), "a handful of other sellers")
        self.assertEqual(band(40), "a lot of other sellers")

    def test_clay_is_told_the_one_thing_worth_opening_with(self):
        """Handing Clay everything we know produces a sentence that lists
        findings. One situation produces a sentence about one thing."""
        self.assertEqual(oa.clay_facts(self._result())["amz_situation"], "undercut")

    def test_resellers_without_undercutting_reads_as_resellers(self):
        r = self._result()
        r["findings"]["listings"][0]["cheapest"] = 55.00  # nobody below the brand
        self.assertEqual(oa.clay_facts(r)["amz_situation"], "resellers")

    def test_absent_is_its_own_situation(self):
        r = self._result(findings={"absent": True, "listings": [], "sponsored_competitors": []})
        self.assertEqual(oa.clay_facts(r)["amz_situation"], "absent")

    def test_a_skipped_brand_gives_clay_nothing_to_write_from(self):
        """Clay must not invent an opener for a brand we could not match."""
        r = self._result(skipped_reason="no confident match", confidence="low")
        self.assertEqual(oa.clay_facts(r)["amz_situation"], "")

    def test_junk_in_gives_blanks_not_a_crash(self):
        for bad in (None, {}, {"findings": None}, "nope"):
            facts = oa.clay_facts(bad)
            self.assertEqual(set(facts), set(oa.CLAY_FACT_COLUMNS))
            self.assertEqual(facts["amz_situation"], "")

    def test_the_column_list_matches_the_csv(self):
        """Drift here means Clay silently receives a column it cannot map."""
        import outbound_pipeline as op
        self.assertEqual(op.AMAZON_FACT_COLUMNS, oa.CLAY_FACT_COLUMNS)
        for col in oa.CLAY_FACT_COLUMNS:
            self.assertIn(col, op.CLAY_CSV_COLUMNS)
