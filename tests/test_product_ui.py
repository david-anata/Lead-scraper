"""Contract tests for the shared Anata Agent visual foundation."""

from sales_support_agent.services.admin_nav import render_agent_nav_styles
from sales_support_agent.services.product_ui import render_product_ui_styles


def test_product_ui_uses_canonical_website_roles() -> None:
    css = render_product_ui_styles()
    for value in ("#f9f7f3", "#2b3644", "#34445c", "#85bbda", "#5e9fc4", "#bfa889"):
        assert value in css
    assert '"Roboto"' in css
    assert '"Montserrat"' in css


def test_product_ui_defines_accessible_interaction_states() -> None:
    css = render_product_ui_styles()
    assert ":focus-visible" in css
    assert "prefers-reduced-motion: reduce" in css
    assert "min-height: 42px" in css


def test_global_navigation_includes_product_foundation() -> None:
    css = render_agent_nav_styles()
    assert "--agent-background" in css
    assert ".topbar" in css
