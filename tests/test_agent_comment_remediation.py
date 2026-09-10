"""Regression checks for the corrected operator choices and sign-in boundaries."""

from datetime import date
from html.parser import HTMLParser

from sales_support_agent.services.hr.pages import _pay_period_options
from sales_support_agent.services.hr.payroll import semimonthly_period
from sales_support_agent.services.admin_dashboard import render_login_page


class Options(HTMLParser):
    def __init__(self):
        super().__init__()
        self.values = []
        self.selected = []

    def handle_starttag(self, tag, attrs):
        if tag == "option":
            attrs = dict(attrs)
            self.values.append(attrs["value"])
            if "selected" in attrs:
                self.selected.append(attrs["value"])


def test_calendar_choices_keep_leap_period_and_cross_year_payday():
    period = semimonthly_period(date(2028, 2, 29))
    rendered = _pay_period_options(period)
    options = Options()
    options.feed(rendered)
    assert options.selected == ["2028-02-16"]
    assert len(options.values) == len(set(options.values)) == 72
    assert "Feb 29, 2028" in rendered
    assert "Pay Jan 05, 2029" in rendered
    assert all(date.fromisoformat(value).day in {1, 16} for value in options.values)


def test_login_recovery_keeps_custom_endpoint_and_escapes_error():
    rendered = render_login_page(show_email_form=True, show_google_button=True,
                                 password_form_action="/admin/break-glass",
                                 error_message="<script>bad</script>")
    assert 'action="/admin/break-glass"' in rendered
    assert '<details class="login-recovery">' in rendered
    assert '<script>bad</script>' not in rendered
    assert '&lt;script&gt;bad&lt;/script&gt;' in rendered
    assert 'action="/admin/auth/email"' in rendered
    assert 'href="/admin/auth/google"' in rendered
