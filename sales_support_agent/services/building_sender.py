"""Who Building mail comes from, and who is always copied.

David set this on 2026-07-31: every Building message goes out as
building@anatainc.com and copies him and Val, so nothing customer-facing
happens without the two people who run the building seeing it.

Kept as one module rather than repeated at each send site, because the value of
"always copied" is that there is no send path that quietly forgets.
"""

from __future__ import annotations

import os

#: The monitored Building inbox. Overridable for a staging service.
BUILDING_FROM_ADDRESS = os.getenv("BUILDING_FROM_ADDRESS", "building@anatainc.com")

#: Always copied on customer-facing Building mail.
BUILDING_ALWAYS_CC = ("david@anatainc.com", "val@anatainc.com")


def building_from_address() -> str:
    return BUILDING_FROM_ADDRESS.strip() or "building@anatainc.com"


def building_cc(*, exclude=()) -> list[str]:
    """The standing copy list, minus anyone already on the message.

    Excluding the recipients matters: a message to David should not also copy
    David, and a customer must never see an internal address duplicated.
    """

    skip = {str(address or "").strip().lower() for address in exclude}
    return [
        address
        for address in BUILDING_ALWAYS_CC
        if address.lower() not in skip
    ]
