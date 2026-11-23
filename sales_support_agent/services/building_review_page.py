"""Public, phone-first front desk page for The Anata Building."""

from __future__ import annotations

import html
from typing import Any

from sales_support_agent.services.building_visitor_reviews import (
    BUILDING_ADDRESS,
    GOOGLE_REDIRECT_SECONDS,
    GOOGLE_REVIEW_URL,
    VISIT_REASONS,
)

_CSS = """
:root{color-scheme:light;--ink:#151719;--muted:#485762;--paper:#f7f8f6;--line:#e7ecef;--sky:#85bbda;--deep:#397a9f;}
*{box-sizing:border-box;}
body{margin:0;background:var(--paper);color:var(--ink);font-family:Inter,"Segoe UI",sans-serif;font-size:16px;line-height:1.5;}
body::before{content:"";display:block;height:6px;background:var(--sky);}
.wrap{max-width:440px;margin:0 auto;padding:28px 20px calc(40px + env(safe-area-inset-bottom));}
.card{background:#fff;border:1px solid var(--line);border-radius:20px;padding:28px 20px 24px;box-shadow:0 18px 40px rgba(17,24,32,.06);}
.logo{display:block;width:168px;height:auto;margin:0 auto 18px;}
.eyebrow{margin:0;color:var(--deep);font-family:Montserrat,Inter,sans-serif;font-size:12px;font-weight:800;letter-spacing:.14em;text-transform:uppercase;text-align:center;}
h1{margin:10px 0 0;font-family:Montserrat,Inter,sans-serif;font-size:clamp(30px,8vw,36px);font-weight:800;letter-spacing:-.04em;line-height:1.12;text-align:center;text-wrap:balance;}
.lede{margin:12px 0 0;color:var(--muted);text-align:center;text-wrap:balance;}
.alert{margin:16px 0 0;padding:12px 14px;border-radius:12px;background:#f8ece9;color:#6f332c;font-weight:700;}
form{margin:22px 0 0;}
.star-row{display:flex;gap:8px;}
.star-btn,.star-choice{position:relative;flex:1;display:flex;flex-direction:column;align-items:center;justify-content:center;gap:2px;min-height:76px;border:1px solid var(--line);border-radius:16px;background:#fff;color:var(--ink);font:inherit;font-weight:800;cursor:pointer;-webkit-tap-highlight-color:transparent;}
.star-btn{appearance:none;}
.star-choice{cursor:pointer;}
.star-choice input{position:absolute;inset:0;margin:0;opacity:0;cursor:pointer;}
.glyph{font-size:28px;line-height:1;color:#d7a31a;}
.num{font-family:Montserrat,Inter,sans-serif;font-size:13px;}
.star-btn:hover,.star-btn:focus-visible,.star-choice:focus-within{border-color:var(--deep);outline:3px solid rgba(133,187,218,.7);outline-offset:2px;}
.star-choice:has(input:checked){background:var(--ink);border-color:var(--ink);color:#fff;}
.star-choice:has(input:checked) .glyph{color:#f2d48a;}
.star-btn:active{transform:scale(.98);}
fieldset{margin:0;padding:0;border:0;}
legend{margin:0 0 8px;font-size:13px;font-weight:800;}
.optional{color:var(--muted);font-weight:600;}
label.field, .stack{display:grid;gap:6px;margin-top:16px;}
.label{font-size:13px;font-weight:800;}
input[type="text"],textarea{width:100%;border:1px solid #c9d0d4;border-radius:12px;background:#fff;color:var(--ink);font:inherit;padding:12px 14px;}
textarea{min-height:120px;resize:vertical;}
input:focus,textarea:focus{outline:3px solid rgba(133,187,218,.7);border-color:var(--deep);}
.choices{display:grid;gap:8px;margin-top:8px;}
.choice{display:flex;align-items:center;gap:10px;min-height:48px;padding:8px 12px;border:1px solid var(--line);border-radius:12px;font-weight:700;}
.choice input{width:20px;height:20px;margin:0;accent-color:var(--ink);}
.button{display:flex;align-items:center;justify-content:center;width:100%;min-height:56px;margin-top:18px;border:0;border-radius:14px;background:var(--ink);color:#fff;font-family:Montserrat,Inter,sans-serif;font-size:17px;font-weight:800;text-align:center;text-decoration:none;cursor:pointer;}
.button:hover,.button:focus-visible{background:#000;outline:3px solid rgba(133,187,218,.7);outline-offset:3px;}
.note{margin:12px 0 0;color:var(--muted);font-size:14px;text-align:center;}
.address{margin:18px 0 0;color:var(--muted);font-size:13px;font-style:normal;text-align:center;line-height:1.45;}
.hp{position:absolute;left:-9999px;width:1px;height:1px;overflow:hidden;}
@media (prefers-reduced-motion: reduce){
  .star-btn:active{transform:none;}
}
"""

_SCRIPT = """
document.querySelectorAll("form").forEach((form) => {
  form.addEventListener("submit", () => {
    window.setTimeout(() => {
      form.querySelectorAll("button").forEach((button) => {
        button.disabled = true;
      });
    }, 0);
  });
});
"""


def _esc(value: Any) -> str:
    return html.escape(str(value or ""), quote=True)


def _shell(*, title: str, heading: str, lede: str, body: str, refresh_url: str = "") -> str:
    refresh = ""
    if refresh_url:
        refresh = (
            f'<meta http-equiv="refresh" content="{GOOGLE_REDIRECT_SECONDS};url={_esc(refresh_url)}">'
        )
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <meta name="robots" content="noindex">
  <meta name="theme-color" content="#f7f8f6">
  <link rel="canonical" href="/review">
  {refresh}
  <title>{_esc(title)}</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700;800&family=Montserrat:wght@700;800&display=swap" rel="stylesheet">
  <style>{_CSS}</style>
</head>
<body>
  <main class="wrap">
    <section class="card" aria-labelledby="review-heading">
      <img class="logo" src="/static/building/anata-logo.png" width="168" height="69" alt="Anata">
      <p class="eyebrow">The Anata Building</p>
      <h1 id="review-heading">{_esc(heading)}</h1>
      <p class="lede">{_esc(lede)}</p>
      {body}
    </section>
    <address class="address">{_esc(BUILDING_ADDRESS)}</address>
  </main>
  <script>{_SCRIPT}</script>
</body>
</html>"""


def _honeypot() -> str:
    return (
        '<div class="hp" aria-hidden="true">'
        '<label for="hp_company">Company</label>'
        '<input id="hp_company" name="hp_company" type="text" tabindex="-1" autocomplete="off" value="">'
        "</div>"
    )


def _stars_picker() -> str:
    buttons = []
    for star in range(1, 6):
        label = "1 star" if star == 1 else f"{star} stars"
        buttons.append(
            f'<button class="star-btn" type="submit" name="stars" value="{star}" aria-label="{label}">'
            '<span class="glyph" aria-hidden="true">★</span>'
            f'<span class="num">{star}</span></button>'
        )
    return (
        f'<form method="post" action="/review">{_honeypot()}'
        f'<div class="star-row" role="group" aria-label="How many stars?">{"".join(buttons)}</div>'
        "</form>"
    )


def _star_editor(selected: int) -> str:
    choices = []
    for star in range(1, 6):
        checked = " checked" if star == selected else ""
        label = "1 star" if star == 1 else f"{star} stars"
        choices.append(
            '<label class="star-choice">'
            f'<input type="radio" name="stars" value="{star}" aria-label="{label}" required{checked}>'
            '<span class="glyph" aria-hidden="true">★</span>'
            f'<span class="num">{star}</span></label>'
        )
    return (
        '<fieldset><legend>Your stars</legend>'
        f'<div class="star-row">{"".join(choices)}</div></fieldset>'
    )


def _reason_choices(selected: list[str]) -> str:
    boxes = []
    for key, label in VISIT_REASONS.items():
        checked = " checked" if key in selected else ""
        boxes.append(
            '<label class="choice">'
            f'<input type="checkbox" name="visit_reason" value="{_esc(key)}"{checked}>'
            f'<span>{_esc(label)}</span></label>'
        )
    return (
        '<fieldset class="stack"><legend>What were you here for? '
        '<span class="optional">Optional</span></legend>'
        f'<div class="choices">{"".join(boxes)}</div></fieldset>'
    )


def render_choose(*, error: str = "") -> str:
    alert = f'<p class="alert" role="alert">{_esc(error)}</p>' if error else ""
    return _shell(
        title="How was your visit? · The Anata Building",
        heading="How was your visit?",
        lede="Tap a star. It takes a second, and it helps the people who run the building.",
        body=alert + _stars_picker(),
    )


def render_google() -> str:
    button = (
        f'<a class="button" href="{_esc(GOOGLE_REVIEW_URL)}">Leave a Google review</a>'
        '<p class="note">Google opens in a couple of seconds. The button works right away.</p>'
    )
    return _shell(
        title="Thank you · The Anata Building",
        heading="Thank you. That means a lot.",
        lede="We're glad you stopped in. If you have a moment, a Google review helps other people find the building.",
        body=button,
        refresh_url=GOOGLE_REVIEW_URL,
    )


def render_feedback_form(
    *,
    review_id: str,
    stars: int,
    comment: str = "",
    visitor_name: str = "",
    contact: str = "",
    reasons: list[str] | None = None,
    error: str = "",
) -> str:
    alert = f'<p class="alert" role="alert">{_esc(error)}</p>' if error else ""
    body = f"""{alert}
<form method="post" action="/review">
  {_honeypot()}
  <input type="hidden" name="step" value="feedback">
  <input type="hidden" name="review_id" value="{_esc(review_id)}">
  {_star_editor(stars)}
  <label class="field" for="comment"><span class="label">What could we do better?</span>
    <textarea id="comment" name="comment" maxlength="2000">{_esc(comment)}</textarea>
  </label>
  <label class="field" for="visitor_name"><span class="label">Name <span class="optional">Optional</span></span>
    <input id="visitor_name" name="visitor_name" type="text" maxlength="120" autocomplete="name" value="{_esc(visitor_name)}">
  </label>
  <label class="field" for="contact"><span class="label">Email or phone <span class="optional">Optional</span></span>
    <input id="contact" name="contact" type="text" maxlength="160" autocomplete="on" value="{_esc(contact)}">
  </label>
  {_reason_choices(list(reasons or []))}
  <button class="button" type="submit">Send to the building team</button>
</form>"""
    return _shell(
        title="Tell us what we could do better · The Anata Building",
        heading="Thank you for telling us.",
        lede="The building team reads every note.",
        body=body,
    )


def render_thanks() -> str:
    return _shell(
        title="Thank you · The Anata Building",
        heading="Thank you.",
        lede="We got it. The building team will read this.",
        body="",
    )


def render_limited() -> str:
    return _shell(
        title="Please wait · The Anata Building",
        heading="Please wait a moment.",
        lede="A lot of notes just came in from the front desk. Try again in a few minutes.",
        body="",
    )


def render_too_large() -> str:
    return _shell(
        title="Note too long · The Anata Building",
        heading="That note is too long.",
        lede="Please shorten it and send it again.",
        body='<a class="button" href="/review">Start again</a>',
    )
