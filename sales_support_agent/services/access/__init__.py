"""Access control (RBAC) for agent.anatainc.com.

Multi-user authorization: custom roles with per-individual-tool permissions,
plus invite and request->approval onboarding. The cookie carries identity
(email) only; authorization is resolved from the DB on every request so role
changes and suspensions take effect immediately.

Modules:
  catalog.py — the canonical tool list (drives role editor, guards, nav)
  store.py   — DB CRUD for users/roles/invites/requests + permission resolution
"""
