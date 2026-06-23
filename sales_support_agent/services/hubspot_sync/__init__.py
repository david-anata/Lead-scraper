"""HubSpot sales mirror sync (read-first).

Pulls deals and their associated companies, contacts, and line items from
HubSpot into the local mirror tables so the Sales Priorities deal board can
render without hitting the API on every page load. HubSpot stays canonical.
"""

from sales_support_agent.services.hubspot_sync.service import (
    HubSpotSyncResult,
    sync_hubspot_sales,
)

__all__ = ["HubSpotSyncResult", "sync_hubspot_sales"]
