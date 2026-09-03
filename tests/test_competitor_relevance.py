"""Regression coverage for client-safe competitor evidence."""

from sales_support_agent.services.deck.competitor_relevance import (
    build_discovery_query,
    qualify_competitors,
)
from sales_support_agent.services.helium10 import XrayProduct


def _product(asin: str, title: str, brand: str, category: str) -> XrayProduct:
    return XrayProduct(
        display_order=1,
        asin=asin,
        title=title,
        url=f"https://www.amazon.com/dp/{asin}",
        image_url="",
        brand=brand,
        price=24.99,
        price_label="$24.99",
        revenue=10000,
        revenue_label="$10,000",
        units_sold=400,
        units_label="400",
        bsr=1000,
        bsr_label="#1,000",
        rating=4.5,
        rating_label="4.5",
        review_count=100,
        category=category,
        seller_country="",
        size_tier="",
        fulfillment="FBA",
        dimensions="",
        weight="",
    )


def test_furniture_paint_excludes_unrelated_chalk_products():
    target = _product("B000000001", "Dixie Belle Chalk Mineral Paint for Furniture", "Dixie Belle", "Furniture Paint")
    candidates = [
        _product("B000000002", "Premium Chalk Paint for Furniture and Cabinets", "Paint Co", "Furniture Paint"),
        _product("B000000003", "Liquid Chalk for Weightlifting and Climbing", "Gym Co", "Climbing Chalk"),
        _product("B000000004", "Dustless Classroom Chalk Sticks", "School Co", "Classroom Chalk"),
    ]

    qualified, decisions, assessment = qualify_competitors(
        target=target,
        candidates=candidates,
        operator_category_label="Furniture paint",
    )

    assert [product.asin for product in qualified] == ["B000000002"]
    assert decisions[1].status == "excluded"
    assert "conflicting_product_type:athletic_chalk" in decisions[1].reason_codes
    assert decisions[2].status == "excluded"
    assert assessment.status == "blocked"


def test_discovery_query_uses_product_type_not_brand():
    query = build_discovery_query(
        title="Dixie Belle Chalk Mineral Paint for Furniture",
        brand="Dixie Belle",
        category="Furniture Paint",
        operator_label="Furniture paint",
    )

    assert query.startswith("furniture paint")
    assert "dixie" not in query
