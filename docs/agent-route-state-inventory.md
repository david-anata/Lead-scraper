# Agent route and migration inventory

Generated from the mounted FastAPI application. Re-run
`python scripts/generate_agent_route_inventory.py --output docs/agent-route-state-inventory.md`
after adding, removing, or moving a route.

Routes inventoried: **559**

| Family | Route | Method | Access | Renderer / handler | Primary job | Phase |
|---|---|---|---|---|---|---|
| Access / transition | `/admin/access` | `GET` | Authenticated + route permission | `users_page` | Read, navigate, or download | 3 |
| Access / transition | `/admin/access/invite/{token}` | `GET` | Authenticated + route permission | `invite_landing` | Read, navigate, or download | 3 |
| Access / transition | `/admin/access/invites` | `GET` | Authenticated + route permission | `invites_page` | Read, navigate, or download | 3 |
| Access / transition | `/admin/access/invites/new` | `POST` | Authenticated + route permission | `create_invite` | Mutation; preserve confirmation/audit contract | 3 |
| Access / transition | `/admin/access/invites/{invite_id}/revoke` | `POST` | Authenticated + route permission | `revoke_invite` | Mutation; preserve confirmation/audit contract | 3 |
| Access / transition | `/admin/access/requests` | `GET` | Authenticated + route permission | `requests_page` | Read, navigate, or download | 3 |
| Access / transition | `/admin/access/requests/{request_id}/approve` | `POST` | Authenticated + route permission | `approve_request` | Mutation; preserve confirmation/audit contract | 3 |
| Access / transition | `/admin/access/requests/{request_id}/deny` | `POST` | Authenticated + route permission | `deny_request` | Mutation; preserve confirmation/audit contract | 3 |
| Access / transition | `/admin/access/roles` | `GET` | Authenticated + route permission | `roles_page` | Read, navigate, or download | 3 |
| Access / transition | `/admin/access/roles/new` | `GET` | Authenticated + route permission | `new_role_form` | Read, navigate, or download | 3 |
| Access / transition | `/admin/access/roles/new` | `POST` | Authenticated + route permission | `create_role` | Mutation; preserve confirmation/audit contract | 3 |
| Access / transition | `/admin/access/roles/{role_id}/delete` | `POST` | Authenticated + route permission | `delete_role` | Mutation; preserve confirmation/audit contract | 3 |
| Access / transition | `/admin/access/roles/{role_id}/edit` | `GET` | Authenticated + route permission | `edit_role_form` | Read, navigate, or download | 3 |
| Access / transition | `/admin/access/roles/{role_id}/edit` | `POST` | Authenticated + route permission | `update_role` | Mutation; preserve confirmation/audit contract | 3 |
| Access / transition | `/admin/access/users/{user_id}/access` | `GET` | Authenticated + route permission | `user_access_form` | Read, navigate, or download | 3 |
| Access / transition | `/admin/access/users/{user_id}/access` | `POST` | Authenticated + route permission | `update_user_access` | Mutation; preserve confirmation/audit contract | 3 |
| Access / transition | `/admin/access/users/{user_id}/role` | `POST` | Authenticated + route permission | `set_role` | Mutation; preserve confirmation/audit contract | 3 |
| Access / transition | `/admin/access/users/{user_id}/status` | `POST` | Authenticated + route permission | `set_status` | Mutation; preserve confirmation/audit contract | 3 |
| Access / transition | `/admin/auth/callback` | `GET` | Public or token-gated | `google_callback` | Read, navigate, or download | 3 |
| Access / transition | `/admin/auth/google` | `GET` | Public or token-gated | `google_login_start` | Read, navigate, or download | 3 |
| Access / transition | `/admin/login` | `GET` | Public or token-gated | `admin_login_page` | Read, navigate, or download | 3 |
| Access / transition | `/admin/login` | `POST` | Public or token-gated | `admin_login_submit` | Mutation; preserve confirmation/audit contract | 3 |
| Access / transition | `/admin/settings` | `GET` | Authenticated + route permission | `settings_page` | Read, navigate, or download | 3 |
| Access / transition | `/admin/settings/inboxes` | `GET` | Authenticated + route permission | `settings_inboxes` | Read, navigate, or download | 3 |
| Access / transition | `/admin/settings/inboxes/callback` | `GET` | Authenticated + route permission | `connect_inbox_callback` | Read, navigate, or download | 3 |
| Access / transition | `/admin/settings/inboxes/connect` | `GET` | Authenticated + route permission | `connect_inbox` | Read, navigate, or download | 3 |
| Access / transition | `/admin/settings/inboxes/disconnect` | `POST` | Authenticated + route permission | `disconnect_inbox` | Mutation; preserve confirmation/audit contract | 3 |
| Admin / shared | `/admin` | `GET` | Authenticated + route permission | `admin_dashboard` | Read, navigate, or download | 3–6 |
| Admin / shared | `/admin/api/content/status` | `GET` | Authenticated + route permission | `content_status` | Read, navigate, or download | 3–6 |
| Admin / shared | `/admin/api/deck-runs` | `GET` | Authenticated + route permission | `admin_deck_runs` | Read, navigate, or download | 3–6 |
| Admin / shared | `/admin/api/deck-runs/{run_id}` | `DELETE` | Authenticated + route permission | `admin_delete_deck_run` | Mutation; preserve confirmation/audit contract | 3–6 |
| Admin / shared | `/admin/api/deck-runs/{run_id}/attach-deal` | `POST` | Authenticated + route permission | `admin_attach_deck_run_to_deal` | Mutation; preserve confirmation/audit contract | 3–6 |
| Admin / shared | `/admin/api/digital-shelf/generate-deck` | `POST` | Authenticated + route permission | `admin_digital_shelf_generate_deck` | Mutation; preserve confirmation/audit contract | 3–6 |
| Admin / shared | `/admin/api/generate-deck` | `POST` | Authenticated + route permission | `admin_generate_deck` | Mutation; preserve confirmation/audit contract | 3–6 |
| Admin / shared | `/admin/api/lead-runs/{run_id}` | `GET` | Authenticated + route permission | `admin_lead_run_status` | Read, navigate, or download | 3–6 |
| Admin / shared | `/admin/api/lead-runs/{run_id}/download` | `GET` | Authenticated + route permission | `admin_lead_run_download` | Read, navigate, or download | 3–6 |
| Admin / shared | `/admin/api/outbound/amazon-scan` | `GET` | Authenticated + route permission | `outbound_amazon_scan_status` | Read, navigate, or download | 3–6 |
| Admin / shared | `/admin/api/outbound/amazon-scan` | `POST` | Authenticated + route permission | `outbound_amazon_scan` | Mutation; preserve confirmation/audit contract | 3–6 |
| Admin / shared | `/admin/api/outbound/brands.csv` | `GET` | Authenticated + route permission | `outbound_brands_csv` | Read, navigate, or download | 3–6 |
| Admin / shared | `/admin/api/outbound/nurture` | `POST` | Authenticated + route permission | `outbound_nurture_enroll` | Mutation; preserve confirmation/audit contract | 3–6 |
| Admin / shared | `/admin/api/outbound/push` | `POST` | Authenticated + route permission | `outbound_push_to_clay` | Mutation; preserve confirmation/audit contract | 3–6 |
| Admin / shared | `/admin/api/outbound/release` | `POST` | Authenticated + route permission | `outbound_release` | Mutation; preserve confirmation/audit contract | 3–6 |
| Admin / shared | `/admin/api/outbound/settings` | `POST` | Authenticated + route permission | `outbound_save_settings` | Mutation; preserve confirmation/audit contract | 3–6 |
| Admin / shared | `/admin/api/run-lead-build` | `POST` | Authenticated + route permission | `admin_run_lead_build` | Mutation; preserve confirmation/audit contract | 3–6 |
| Admin / shared | `/admin/api/sync-dashboard` | `POST` | Authenticated + route permission | `admin_sync_dashboard` | Mutation; preserve confirmation/audit contract | 3–6 |
| Admin / shared | `/admin/api/sync-dashboard/status` | `GET` | Authenticated + route permission | `admin_sync_dashboard_status` | Read, navigate, or download | 3–6 |
| Admin / shared | `/admin/api/website-ops/actions/execute-approved` | `POST` | Authenticated + route permission | `admin_website_ops_execute_approved` | Mutation; preserve confirmation/audit contract | 3–6 |
| Admin / shared | `/admin/api/website-ops/feedback` | `POST` | Authenticated + route permission | `admin_website_ops_feedback_submit` | Mutation; preserve confirmation/audit contract | 3–6 |
| Admin / shared | `/admin/api/website-ops/feedback/{feedback_id}/review` | `POST` | Authenticated + route permission | `admin_website_ops_feedback_review` | Mutation; preserve confirmation/audit contract | 3–6 |
| Admin / shared | `/admin/api/website-ops/run` | `POST` | Authenticated + route permission | `admin_website_ops_run` | Mutation; preserve confirmation/audit contract | 3–6 |
| Admin / shared | `/admin/api/website-ops/status` | `GET` | Authenticated + route permission | `admin_website_ops_status` | Read, navigate, or download | 3–6 |
| Admin / shared | `/admin/assets/navigation.css` | `GET` | Authenticated + route permission | `navigation_stylesheet` | Read, navigate, or download | 3–6 |
| Admin / shared | `/admin/break-glass` | `GET` | Authenticated + route permission | `admin_break_glass_page` | Read, navigate, or download | 3–6 |
| Admin / shared | `/admin/break-glass` | `POST` | Authenticated + route permission | `admin_break_glass_submit` | Mutation; preserve confirmation/audit contract | 3–6 |
| Admin / shared | `/admin/content` | `GET` | Authenticated + route permission | `content_control_room` | Read, navigate, or download | 3–6 |
| Admin / shared | `/admin/content/runs/{run_id}` | `GET` | Authenticated + route permission | `content_run_detail` | Read, navigate, or download | 3–6 |
| Admin / shared | `/admin/logout` | `GET` | Authenticated + route permission | `admin_logout` | Read, navigate, or download | 3–6 |
| Admin / shared | `/admin/outbound/brands` | `GET` | Authenticated + route permission | `outbound_brands_page` | Read, navigate, or download | 3–6 |
| Admin / shared | `/admin/outbound/lead-ops` | `GET` | Authenticated + route permission | `outbound_lead_ops` | Read, navigate, or download | 3–6 |
| Admin / shared | `/admin/outbound/leads` | `GET` | Authenticated + route permission | `outbound_leads` | Read, navigate, or download | 3–6 |
| Admin / shared | `/admin/outbound/leak-report/{domain}` | `GET` | Authenticated + route permission | `outbound_leak_report` | Read, navigate, or download | 3–6 |
| Admin / shared | `/admin/outbound/scoreboard` | `GET` | Authenticated + route permission | `outbound_scoreboard` | Read, navigate, or download | 3–6 |
| Admin / shared | `/admin/pending` | `GET` | Authenticated + route permission | `access_pending` | Read, navigate, or download | 3–6 |
| Advertising | `/admin/advertising/audit` | `GET` | Authenticated + route permission | `audit_page` | Read, navigate, or download | 6 |
| Advertising | `/admin/advertising/audit/goals` | `POST` | Authenticated + route permission | `save_goals` | Mutation; preserve confirmation/audit contract | 6 |
| Advertising | `/admin/advertising/audit/run` | `POST` | Authenticated + route permission | `run` | Mutation; preserve confirmation/audit contract | 6 |
| Advertising | `/admin/advertising/audit/run/confirm` | `POST` | Authenticated + route permission | `run_confirm` | Mutation; preserve confirmation/audit contract | 6 |
| Advertising | `/admin/advertising/audit/{run_id}/bulk/{ad_type}.xlsx` | `GET` | Authenticated + route permission | `download_bulk` | Read, navigate, or download | 6 |
| Advertising | `/admin/advertising/audit/{run_id}/plan.xlsx` | `GET` | Authenticated + route permission | `download_plan` | Read, navigate, or download | 6 |
| Advertising | `/admin/advertising/bulk-profitability` | `GET` | Authenticated + route permission | `bulk_profitability_page` | Read, navigate, or download | 6 |
| Advertising | `/admin/advertising/clients` | `GET` | Authenticated + route permission | `clients_page` | Read, navigate, or download | 6 |
| Advertising | `/admin/advertising/clients/new` | `POST` | Authenticated + route permission | `clients_new` | Mutation; preserve confirmation/audit contract | 6 |
| Advertising | `/admin/advertising/clients/{client_id}` | `POST` | Authenticated + route permission | `clients_save` | Mutation; preserve confirmation/audit contract | 6 |
| Advertising | `/admin/advertising/clients/{client_id}/archive` | `POST` | Authenticated + route permission | `clients_archive` | Mutation; preserve confirmation/audit contract | 6 |
| Advertising | `/admin/advertising/profit-calculator` | `GET` | Authenticated + route permission | `profit_calculator_page` | Read, navigate, or download | 6 |
| Building | `/admin/building` | `GET` | Authenticated + route permission | `building_control_room` | Read, navigate, or download | 7 |
| Building | `/admin/building/agreement-readiness` | `GET` | Authenticated + route permission | `agreement_readiness_page` | Read, navigate, or download | 7 |
| Building | `/admin/building/agreement-readiness/packages` | `POST` | Authenticated + route permission | `prepare_package_from_admin` | Mutation; preserve confirmation/audit contract | 7 |
| Building | `/admin/building/agreement-readiness/packages/transition` | `POST` | Authenticated + route permission | `transition_package_from_admin` | Mutation; preserve confirmation/audit contract | 7 |
| Building | `/admin/building/agreement-readiness/payments/transition` | `POST` | Authenticated + route permission | `transition_payment_from_admin` | Mutation; preserve confirmation/audit contract | 7 |
| Building | `/admin/building/agreement-readiness/templates` | `POST` | Authenticated + route permission | `save_template_from_admin` | Mutation; preserve confirmation/audit contract | 7 |
| Building | `/admin/building/agreement-readiness/templates/transition` | `POST` | Authenticated + route permission | `transition_template_from_admin` | Mutation; preserve confirmation/audit contract | 7 |
| Building | `/admin/building/billing/accounts` | `POST` | Authenticated + route permission | `save_billing_account_from_control_room` | Mutation; preserve confirmation/audit contract | 7 |
| Building | `/admin/building/billing/adjustments` | `POST` | Authenticated + route permission | `request_adjustment_from_control_room` | Mutation; preserve confirmation/audit contract | 7 |
| Building | `/admin/building/billing/adjustments/{adjustment_id}/approve` | `POST` | Authenticated + route permission | `approve_adjustment_from_control_room` | Mutation; preserve confirmation/audit contract | 7 |
| Building | `/admin/building/billing/adjustments/{adjustment_id}/evidence` | `POST` | Authenticated + route permission | `record_adjustment_evidence_from_control_room` | Mutation; preserve confirmation/audit contract | 7 |
| Building | `/admin/building/billing/collections/refresh` | `POST` | Authenticated + route permission | `refresh_collections_from_control_room` | Mutation; preserve confirmation/audit contract | 7 |
| Building | `/admin/building/billing/collections/{case_id}/remind` | `POST` | Authenticated + route permission | `remind_collection_from_control_room` | Mutation; preserve confirmation/audit contract | 7 |
| Building | `/admin/building/billing/collections/{case_id}/transition` | `POST` | Authenticated + route permission | `transition_collection_from_control_room` | Mutation; preserve confirmation/audit contract | 7 |
| Building | `/admin/building/billing/schedules` | `POST` | Authenticated + route permission | `save_billing_schedule_from_control_room` | Mutation; preserve confirmation/audit contract | 7 |
| Building | `/admin/building/billing/schedules/{schedule_id}/approve` | `POST` | Authenticated + route permission | `approve_billing_schedule_from_control_room` | Mutation; preserve confirmation/audit contract | 7 |
| Building | `/admin/building/billing/schedules/{schedule_id}/invoice` | `POST` | Authenticated + route permission | `create_invoice_from_control_room` | Mutation; preserve confirmation/audit contract | 7 |
| Building | `/admin/building/calendar/sync` | `POST` | Authenticated + route permission | `sync_calendar_from_control_room` | Mutation; preserve confirmation/audit contract | 7 |
| Building | `/admin/building/campaigns` | `POST` | Authenticated + route permission | `save_campaign_from_control_room` | Mutation; preserve confirmation/audit contract | 7 |
| Building | `/admin/building/campaigns/{campaign_id}/approve` | `POST` | Authenticated + route permission | `approve_campaign_from_control_room` | Mutation; preserve confirmation/audit contract | 7 |
| Building | `/admin/building/campaigns/{campaign_id}/preview` | `POST` | Authenticated + route permission | `preview_campaign_from_control_room` | Mutation; preserve confirmation/audit contract | 7 |
| Building | `/admin/building/campaigns/{campaign_id}/retry` | `POST` | Authenticated + route permission | `retry_campaign_failures_from_control_room` | Mutation; preserve confirmation/audit contract | 7 |
| Building | `/admin/building/campaigns/{campaign_id}/review` | `POST` | Authenticated + route permission | `review_campaign_from_control_room` | Mutation; preserve confirmation/audit contract | 7 |
| Building | `/admin/building/campaigns/{campaign_id}/schedule` | `POST` | Authenticated + route permission | `schedule_campaign_from_control_room` | Mutation; preserve confirmation/audit contract | 7 |
| Building | `/admin/building/campaigns/{campaign_id}/send` | `POST` | Authenticated + route permission | `send_campaign_from_control_room` | Mutation; preserve confirmation/audit contract | 7 |
| Building | `/admin/building/campaigns/{campaign_id}/test-send` | `POST` | Authenticated + route permission | `test_send_campaign_from_control_room` | Mutation; preserve confirmation/audit contract | 7 |
| Building | `/admin/building/campaigns/{campaign_id}/unschedule` | `POST` | Authenticated + route permission | `unschedule_campaign_from_control_room` | Mutation; preserve confirmation/audit contract | 7 |
| Building | `/admin/building/catalog/arena/prepare` | `POST` | Authenticated + route permission | `prepare_verified_arena_catalog_from_control_room` | Mutation; preserve confirmation/audit contract | 7 |
| Building | `/admin/building/checklists/items/{item_id}/status` | `POST` | Authenticated + route permission | `update_checklist_item_from_control_room` | Mutation; preserve confirmation/audit contract | 7 |
| Building | `/admin/building/checklists/{checklist_id}/items` | `POST` | Authenticated + route permission | `add_checklist_item_from_control_room` | Mutation; preserve confirmation/audit contract | 7 |
| Building | `/admin/building/contacts` | `POST` | Authenticated + route permission | `save_contact_from_control_room` | Mutation; preserve confirmation/audit contract | 7 |
| Building | `/admin/building/contacts/merge` | `POST` | Authenticated + route permission | `merge_contacts_from_control_room` | Mutation; preserve confirmation/audit contract | 7 |
| Building | `/admin/building/contacts/merge/preview` | `POST` | Authenticated + route permission | `preview_contact_merge_from_control_room` | Mutation; preserve confirmation/audit contract | 7 |
| Building | `/admin/building/contacts/{contact_id}/relationships/{relationship_id}/review` | `POST` | Authenticated + route permission | `review_relationship_from_control_room` | Mutation; preserve confirmation/audit contract | 7 |
| Building | `/admin/building/content` | `GET` | Authenticated + route permission | `content_admin_page` | Read, navigate, or download | 7 |
| Building | `/admin/building/content` | `POST` | Authenticated + route permission | `save_content_from_admin` | Mutation; preserve confirmation/audit contract | 7 |
| Building | `/admin/building/content/{kind}/{record_id}/review` | `POST` | Authenticated + route permission | `review_content_from_admin` | Mutation; preserve confirmation/audit contract | 7 |
| Building | `/admin/building/contracts` | `GET` | Authenticated + route permission | `contract_index` | Read, navigate, or download | 7 |
| Building | `/admin/building/contracts/packages` | `POST` | Authenticated + route permission | `prepare_contract` | Mutation; preserve confirmation/audit contract | 7 |
| Building | `/admin/building/contracts/{agreement_id}` | `GET` | Authenticated + route permission | `contract_detail` | Read, navigate, or download | 7 |
| Building | `/admin/building/contracts/{agreement_id}/payments/transition` | `POST` | Authenticated + route permission | `transition_contract_payment` | Mutation; preserve confirmation/audit contract | 7 |
| Building | `/admin/building/contracts/{agreement_id}/transition` | `POST` | Authenticated + route permission | `transition_contract` | Mutation; preserve confirmation/audit contract | 7 |
| Building | `/admin/building/event-reviews` | `POST` | Authenticated + route permission | `create_event_review_from_control_room` | Mutation; preserve confirmation/audit contract | 7 |
| Building | `/admin/building/inquiries` | `POST` | Authenticated + route permission | `create_assisted_inquiry_from_control_room` | Mutation; preserve confirmation/audit contract | 7 |
| Building | `/admin/building/inquiries/{inquiry_id}/lifecycle` | `POST` | Authenticated + route permission | `update_inquiry_lifecycle_from_control_room` | Mutation; preserve confirmation/audit contract | 7 |
| Building | `/admin/building/inquiries/{inquiry_id}/retry-hubspot` | `POST` | Authenticated + route permission | `retry_inquiry_hubspot_from_control_room` | Mutation; preserve confirmation/audit contract | 7 |
| Building | `/admin/building/inquiries/{inquiry_id}/schedule-tour` | `POST` | Authenticated + route permission | `schedule_tour_inquiry_from_control_room` | Mutation; preserve confirmation/audit contract | 7 |
| Building | `/admin/building/launch-readiness/decisions/{decision_key}` | `POST` | Authenticated + route permission | `record_arena_launch_decision` | Mutation; preserve confirmation/audit contract | 7 |
| Building | `/admin/building/offerings` | `POST` | Authenticated + route permission | `save_offering_from_control_room` | Mutation; preserve confirmation/audit contract | 7 |
| Building | `/admin/building/privacy/contacts/{contact_id}/correct` | `POST` | Authenticated + route permission | `correct_contact_admin` | Mutation; preserve confirmation/audit contract | 7 |
| Building | `/admin/building/privacy/contacts/{contact_id}/export` | `GET` | Authenticated + route permission | `export_contact_admin` | Read, navigate, or download | 7 |
| Building | `/admin/building/privacy/contacts/{contact_id}/suppress` | `POST` | Authenticated + route permission | `suppress_contact_admin` | Mutation; preserve confirmation/audit contract | 7 |
| Building | `/admin/building/privacy/requests` | `POST` | Authenticated + route permission | `create_request_admin` | Mutation; preserve confirmation/audit contract | 7 |
| Building | `/admin/building/privacy/requests/{request_id}/transition` | `POST` | Authenticated + route permission | `transition_request_admin` | Mutation; preserve confirmation/audit contract | 7 |
| Building | `/admin/building/rate-plans` | `POST` | Authenticated + route permission | `save_rate_plan_from_control_room` | Mutation; preserve confirmation/audit contract | 7 |
| Building | `/admin/building/rate-plans/arena-commercial-baseline` | `POST` | Authenticated + route permission | `prepare_arena_commercial_baseline` | Mutation; preserve confirmation/audit contract | 7 |
| Building | `/admin/building/rate-plans/{rate_plan_id}/approve` | `POST` | Authenticated + route permission | `approve_rate_plan_from_control_room` | Mutation; preserve confirmation/audit contract | 7 |
| Building | `/admin/building/rate-plans/{rate_plan_id}/reconcile-source-conflicts` | `POST` | Authenticated + route permission | `reconcile_rate_plan_source_conflicts` | Mutation; preserve confirmation/audit contract | 7 |
| Building | `/admin/building/rate-plans/{rate_plan_id}/retire` | `POST` | Authenticated + route permission | `retire_rate_plan_from_control_room` | Mutation; preserve confirmation/audit contract | 7 |
| Building | `/admin/building/reservations` | `POST` | Authenticated + route permission | `create_reservation_from_control_room` | Mutation; preserve confirmation/audit contract | 7 |
| Building | `/admin/building/reservations/{reservation_id}/agreements` | `POST` | Authenticated + route permission | `record_agreement_from_control_room` | Mutation; preserve confirmation/audit contract | 7 |
| Building | `/admin/building/reservations/{reservation_id}/customer-status-access` | `POST` | Authenticated + route permission | `prepare_customer_status_access_from_control_room` | Mutation; preserve confirmation/audit contract | 7 |
| Building | `/admin/building/reservations/{reservation_id}/deposits` | `POST` | Authenticated + route permission | `record_deposit_from_control_room` | Mutation; preserve confirmation/audit contract | 7 |
| Building | `/admin/building/reservations/{reservation_id}/proposals` | `POST` | Authenticated + route permission | `record_proposal_from_control_room` | Mutation; preserve confirmation/audit contract | 7 |
| Building | `/admin/building/reservations/{reservation_id}/tours` | `POST` | Authenticated + route permission | `create_tour_from_control_room` | Mutation; preserve confirmation/audit contract | 7 |
| Building | `/admin/building/reservations/{reservation_id}/transition` | `POST` | Authenticated + route permission | `transition_reservation_from_control_room` | Mutation; preserve confirmation/audit contract | 7 |
| Building | `/admin/building/roster-imports/preview` | `POST` | Authenticated + route permission | `preview_roster_import_from_control_room` | Mutation; preserve confirmation/audit contract | 7 |
| Building | `/admin/building/roster-imports/{import_id}/apply` | `POST` | Authenticated + route permission | `apply_roster_import_from_control_room` | Mutation; preserve confirmation/audit contract | 7 |
| Building | `/admin/building/roster-imports/{import_id}/cancel` | `POST` | Authenticated + route permission | `cancel_roster_import_from_control_room` | Mutation; preserve confirmation/audit contract | 7 |
| Building | `/admin/building/segments` | `POST` | Authenticated + route permission | `save_segment_from_control_room` | Mutation; preserve confirmation/audit contract | 7 |
| Building | `/admin/building/service-requests` | `POST` | Authenticated + route permission | `create_service_request_from_control_room` | Mutation; preserve confirmation/audit contract | 7 |
| Building | `/admin/building/service-requests/{service_request_id}/transition` | `POST` | Authenticated + route permission | `transition_service_request_from_control_room` | Mutation; preserve confirmation/audit contract | 7 |
| Building | `/admin/building/spaces` | `POST` | Authenticated + route permission | `save_space_from_control_room` | Mutation; preserve confirmation/audit contract | 7 |
| Building | `/admin/building/spaces/{space_id}/media` | `POST` | Authenticated + route permission | `save_space_media_from_control_room` | Mutation; preserve confirmation/audit contract | 7 |
| Building | `/admin/building/spaces/{space_id}/media/{media_id}/remove` | `POST` | Authenticated + route permission | `remove_space_media_from_control_room` | Mutation; preserve confirmation/audit contract | 7 |
| Building | `/admin/building/tours/{tour_id}` | `POST` | Authenticated + route permission | `update_tour_from_control_room` | Mutation; preserve confirmation/audit contract | 7 |
| Executive / Brand | `/admin/executive` | `GET` | Authenticated + route permission | `admin_executive_dashboard` | Read, navigate, or download | 6 |
| Executive / Brand | `/admin/executive/brand-analysis` | `GET` | Authenticated + route permission | `landing` | Read, navigate, or download | 6 |
| Executive / Brand | `/admin/executive/brand-analysis/discover` | `GET` | Authenticated + route permission | `discover` | Read, navigate, or download | 6 |
| Executive / Brand | `/admin/executive/brand-analysis/discover/add` | `POST` | Authenticated + route permission | `discover_add` | Mutation; preserve confirmation/audit contract | 6 |
| Executive / Brand | `/admin/executive/brand-analysis/discover/run` | `POST` | Authenticated + route permission | `discover_run` | Mutation; preserve confirmation/audit contract | 6 |
| Executive / Brand | `/admin/executive/brand-analysis/pipeline` | `GET` | Authenticated + route permission | `pipeline` | Read, navigate, or download | 6 |
| Executive / Brand | `/admin/executive/brand-analysis/run` | `POST` | Authenticated + route permission | `run` | Mutation; preserve confirmation/audit contract | 6 |
| Executive / Brand | `/admin/executive/brand-analysis/{report_id}` | `DELETE` | Authenticated + route permission | `delete_report` | Mutation; preserve confirmation/audit contract | 6 |
| Executive / Brand | `/admin/executive/brand-analysis/{report_id}` | `GET` | Authenticated + route permission | `view` | Read, navigate, or download | 6 |
| Executive / Brand | `/admin/executive/brand-analysis/{report_id}/competitive` | `PATCH` | Authenticated + route permission | `update_competitive` | Mutation; preserve confirmation/audit contract | 6 |
| Executive / Brand | `/admin/executive/brand-analysis/{report_id}/contact` | `PATCH` | Authenticated + route permission | `update_contact` | Mutation; preserve confirmation/audit contract | 6 |
| Executive / Brand | `/admin/executive/brand-analysis/{report_id}/context-notes` | `PATCH` | Authenticated + route permission | `update_context_notes` | Mutation; preserve confirmation/audit contract | 6 |
| Executive / Brand | `/admin/executive/brand-analysis/{report_id}/deal` | `PATCH` | Authenticated + route permission | `update_deal` | Mutation; preserve confirmation/audit contract | 6 |
| Executive / Brand | `/admin/executive/brand-analysis/{report_id}/download` | `GET` | Authenticated + route permission | `download` | Read, navigate, or download | 6 |
| Executive / Brand | `/admin/executive/brand-analysis/{report_id}/edit` | `GET` | Authenticated + route permission | `edit` | Read, navigate, or download | 6 |
| Executive / Brand | `/admin/executive/brand-analysis/{report_id}/enrich` | `GET` | Authenticated + route permission | `enrich` | Read, navigate, or download | 6 |
| Executive / Brand | `/admin/executive/brand-analysis/{report_id}/note` | `PATCH` | Authenticated + route permission | `update_note` | Mutation; preserve confirmation/audit contract | 6 |
| Executive / Brand | `/admin/executive/brand-analysis/{report_id}/rerun` | `POST` | Authenticated + route permission | `rerun` | Mutation; preserve confirmation/audit contract | 6 |
| Executive / Brand | `/admin/executive/brand-analysis/{report_id}/social` | `PATCH` | Authenticated + route permission | `update_social` | Mutation; preserve confirmation/audit contract | 6 |
| Executive / Brand | `/admin/executive/brand-analysis/{report_id}/stage` | `PATCH` | Authenticated + route permission | `update_stage` | Mutation; preserve confirmation/audit contract | 6 |
| Finance | `/admin/finances` | `GET` | Authenticated + route permission | `finance_overview` | Read, navigate, or download | 9 |
| Finance | `/admin/finances/` | `GET` | Authenticated + route permission | `finance_overview` | Read, navigate, or download | 9 |
| Finance | `/admin/finances/actions/{event_id}/installment` | `POST` | Authenticated + route permission | `schedule_installment` | Mutation; preserve confirmation/audit contract | 9 |
| Finance | `/admin/finances/actions/{event_id}/partial` | `POST` | Authenticated + route permission | `record_partial_payment` | Mutation; preserve confirmation/audit contract | 9 |
| Finance | `/admin/finances/alerts` | `GET` | Authenticated + route permission | `finance_alerts` | Read, navigate, or download | 9 |
| Finance | `/admin/finances/alerts/dismiss-all` | `POST` | Authenticated + route permission | `dismiss_all_alerts` | Mutation; preserve confirmation/audit contract | 9 |
| Finance | `/admin/finances/alerts/dismiss/{alert_id}` | `POST` | Authenticated + route permission | `dismiss_alert` | Mutation; preserve confirmation/audit contract | 9 |
| Finance | `/admin/finances/ap` | `GET` | Authenticated + route permission | `ap_list` | Read, navigate, or download | 9 |
| Finance | `/admin/finances/ap/new` | `GET` | Authenticated + route permission | `ap_new_form` | Read, navigate, or download | 9 |
| Finance | `/admin/finances/ap/new` | `POST` | Authenticated + route permission | `ap_new_submit` | Mutation; preserve confirmation/audit contract | 9 |
| Finance | `/admin/finances/ap/{event_id}/delete` | `POST` | Authenticated + route permission | `ap_delete` | Mutation; preserve confirmation/audit contract | 9 |
| Finance | `/admin/finances/ap/{event_id}/edit` | `GET` | Authenticated + route permission | `ap_edit_form` | Read, navigate, or download | 9 |
| Finance | `/admin/finances/ap/{event_id}/edit` | `POST` | Authenticated + route permission | `ap_edit_submit` | Mutation; preserve confirmation/audit contract | 9 |
| Finance | `/admin/finances/ar` | `GET` | Authenticated + route permission | `ar_list` | Read, navigate, or download | 9 |
| Finance | `/admin/finances/ar/new` | `GET` | Authenticated + route permission | `ar_new_form` | Read, navigate, or download | 9 |
| Finance | `/admin/finances/ar/new` | `POST` | Authenticated + route permission | `ar_new_submit` | Mutation; preserve confirmation/audit contract | 9 |
| Finance | `/admin/finances/ar/{event_id}/delete` | `POST` | Authenticated + route permission | `ar_delete` | Mutation; preserve confirmation/audit contract | 9 |
| Finance | `/admin/finances/ar/{event_id}/edit` | `GET` | Authenticated + route permission | `ar_edit_form` | Read, navigate, or download | 9 |
| Finance | `/admin/finances/ar/{event_id}/edit` | `POST` | Authenticated + route permission | `ar_edit_submit` | Mutation; preserve confirmation/audit contract | 9 |
| Finance | `/admin/finances/assistant/confirm` | `POST` | Authenticated + route permission | `finance_assistant_confirm` | Mutation; preserve confirmation/audit contract | 9 |
| Finance | `/admin/finances/assistant/preview` | `POST` | Authenticated + route permission | `finance_assistant_preview` | Mutation; preserve confirmation/audit contract | 9 |
| Finance | `/admin/finances/audit` | `GET` | Authenticated + route permission | `finance_audit_page` | Read, navigate, or download | 9 |
| Finance | `/admin/finances/audit/clear-dismissals` | `POST` | Authenticated + route permission | `audit_clear_dismissals_endpoint` | Mutation; preserve confirmation/audit contract | 9 |
| Finance | `/admin/finances/audit/dismiss` | `POST` | Authenticated + route permission | `audit_dismiss_endpoint` | Mutation; preserve confirmation/audit contract | 9 |
| Finance | `/admin/finances/bookkeeping` | `GET` | Authenticated + route permission | `finance_bookkeeping_page` | Read, navigate, or download | 9 |
| Finance | `/admin/finances/bookkeeping/file` | `POST` | Authenticated + route permission | `bookkeeping_file_endpoint` | Mutation; preserve confirmation/audit contract | 9 |
| Finance | `/admin/finances/bookkeeping/file-all` | `POST` | Authenticated + route permission | `bookkeeping_file_all_endpoint` | Mutation; preserve confirmation/audit contract | 9 |
| Finance | `/admin/finances/bookkeeping/file-merchant` | `POST` | Authenticated + route permission | `bookkeeping_file_merchant_endpoint` | Mutation; preserve confirmation/audit contract | 9 |
| Finance | `/admin/finances/bookkeeping/rules/{rule_id}/delete` | `POST` | Authenticated + route permission | `bookkeeping_rule_delete_endpoint` | Mutation; preserve confirmation/audit contract | 9 |
| Finance | `/admin/finances/calendar` | `GET` | Authenticated + route permission | `calendar_page` | Read, navigate, or download | 9 |
| Finance | `/admin/finances/chart-data` | `GET` | Authenticated + route permission | `chart_data` | Read, navigate, or download | 9 |
| Finance | `/admin/finances/chart-data-daily` | `GET` | Authenticated + route permission | `chart_data_daily` | Read, navigate, or download | 9 |
| Finance | `/admin/finances/collections` | `GET` | Authenticated + route permission | `finance_collections_page` | Read, navigate, or download | 9 |
| Finance | `/admin/finances/collections/contact` | `POST` | Authenticated + route permission | `collections_contact_endpoint` | Mutation; preserve confirmation/audit contract | 9 |
| Finance | `/admin/finances/collections/mark` | `POST` | Authenticated + route permission | `collections_mark_endpoint` | Mutation; preserve confirmation/audit contract | 9 |
| Finance | `/admin/finances/collections/send` | `POST` | Authenticated + route permission | `collections_send_endpoint` | Mutation; preserve confirmation/audit contract | 9 |
| Finance | `/admin/finances/commitments/{commitment_id}/transition-confirm` | `POST` | Authenticated + route permission | `commitment_transition_confirm` | Mutation; preserve confirmation/audit contract | 9 |
| Finance | `/admin/finances/commitments/{commitment_id}/transition-preview` | `POST` | Authenticated + route permission | `commitment_transition_preview` | Mutation; preserve confirmation/audit contract | 9 |
| Finance | `/admin/finances/cutover` | `GET` | Authenticated + route permission | `finance_cutover_page` | Read, navigate, or download | 9 |
| Finance | `/admin/finances/events/{event_id}` | `PATCH` | Authenticated + route permission | `patch_event` | Mutation; preserve confirmation/audit contract | 9 |
| Finance | `/admin/finances/forecast` | `GET` | Authenticated + route permission | `finance_forecast` | Read, navigate, or download | 9 |
| Finance | `/admin/finances/health` | `GET` | Authenticated + route permission | `cashflow_health` | Read, navigate, or download | 9 |
| Finance | `/admin/finances/import` | `GET` | Authenticated + route permission | `schedule_import_page` | Read, navigate, or download | 9 |
| Finance | `/admin/finances/import/apply` | `POST` | Authenticated + route permission | `schedule_import_apply` | Mutation; preserve confirmation/audit contract | 9 |
| Finance | `/admin/finances/income-patterns/{pattern_key}/decision` | `POST` | Authenticated + route permission | `update_income_pattern_decision` | Mutation; preserve confirmation/audit contract | 9 |
| Finance | `/admin/finances/ledger` | `GET` | Authenticated + route permission | `ledger_page` | Read, navigate, or download | 9 |
| Finance | `/admin/finances/ledger/export` | `GET` | Authenticated + route permission | `ledger_export` | Read, navigate, or download | 9 |
| Finance | `/admin/finances/matches/confirm` | `POST` | Authenticated + route permission | `matches_confirm_endpoint` | Mutation; preserve confirmation/audit contract | 9 |
| Finance | `/admin/finances/matches/undo/{run_id}` | `POST` | Authenticated + route permission | `matches_undo_endpoint` | Mutation; preserve confirmation/audit contract | 9 |
| Finance | `/admin/finances/plaid/accounts/{account_id}/cash-role` | `POST` | Authenticated + route permission | `plaid_set_account_cash_role` | Mutation; preserve confirmation/audit contract | 9 |
| Finance | `/admin/finances/plaid/exchange` | `POST` | Authenticated + route permission | `plaid_exchange` | Mutation; preserve confirmation/audit contract | 9 |
| Finance | `/admin/finances/plaid/items/{item_id}/disconnect` | `POST` | Authenticated + route permission | `plaid_disconnect_item` | Mutation; preserve confirmation/audit contract | 9 |
| Finance | `/admin/finances/plaid/items/{item_id}/link-token` | `POST` | Authenticated + route permission | `plaid_update_link_token` | Mutation; preserve confirmation/audit contract | 9 |
| Finance | `/admin/finances/plaid/items/{item_id}/refresh` | `POST` | Authenticated + route permission | `plaid_refresh_item` | Mutation; preserve confirmation/audit contract | 9 |
| Finance | `/admin/finances/plaid/link-token` | `POST` | Authenticated + route permission | `plaid_link_token` | Mutation; preserve confirmation/audit contract | 9 |
| Finance | `/admin/finances/plaid/oauth-return` | `GET` | Authenticated + route permission | `plaid_oauth_return` | Read, navigate, or download | 9 |
| Finance | `/admin/finances/plaid/refresh` | `POST` | Authenticated + route permission | `plaid_refresh_all` | Mutation; preserve confirmation/audit contract | 9 |
| Finance | `/admin/finances/plan/move` | `POST` | Authenticated + route permission | `plan_move_endpoint` | Mutation; preserve confirmation/audit contract | 9 |
| Finance | `/admin/finances/plan/order/reset` | `POST` | Authenticated + route permission | `plan_order_reset_endpoint` | Mutation; preserve confirmation/audit contract | 9 |
| Finance | `/admin/finances/qbo` | `GET` | Authenticated + route permission | `qbo_settings_page` | Read, navigate, or download | 9 |
| Finance | `/admin/finances/qbo/disconnect` | `POST` | Authenticated + route permission | `qbo_disconnect` | Mutation; preserve confirmation/audit contract | 9 |
| Finance | `/admin/finances/reconcile` | `GET` | Authenticated + route permission | `reconcile_page` | Read, navigate, or download | 9 |
| Finance | `/admin/finances/reconcile/accept-pattern` | `POST` | Authenticated + route permission | `reconcile_accept_pattern` | Mutation; preserve confirmation/audit contract | 9 |
| Finance | `/admin/finances/recurring` | `GET` | Authenticated + route permission | `recurring_list` | Read, navigate, or download | 9 |
| Finance | `/admin/finances/recurring/generate` | `POST` | Authenticated + route permission | `recurring_generate` | Mutation; preserve confirmation/audit contract | 9 |
| Finance | `/admin/finances/recurring/new` | `GET` | Authenticated + route permission | `recurring_new_form` | Read, navigate, or download | 9 |
| Finance | `/admin/finances/recurring/new` | `POST` | Authenticated + route permission | `recurring_new_submit` | Mutation; preserve confirmation/audit contract | 9 |
| Finance | `/admin/finances/recurring/roll-forward` | `POST` | Authenticated + route permission | `recurring_roll_forward` | Mutation; preserve confirmation/audit contract | 9 |
| Finance | `/admin/finances/recurring/{template_id}/delete` | `POST` | Authenticated + route permission | `recurring_delete` | Mutation; preserve confirmation/audit contract | 9 |
| Finance | `/admin/finances/recurring/{template_id}/edit` | `GET` | Authenticated + route permission | `recurring_edit_form` | Read, navigate, or download | 9 |
| Finance | `/admin/finances/recurring/{template_id}/edit` | `POST` | Authenticated + route permission | `recurring_edit_submit` | Mutation; preserve confirmation/audit contract | 9 |
| Finance | `/admin/finances/review` | `GET` | Authenticated + route permission | `review_page_endpoint` | Read, navigate, or download | 9 |
| Finance | `/admin/finances/review/apply` | `POST` | Authenticated + route permission | `review_apply_endpoint` | Mutation; preserve confirmation/audit contract | 9 |
| Finance | `/admin/finances/review/cleanup-preview` | `POST` | Authenticated + route permission | `review_cleanup_preview_endpoint` | Mutation; preserve confirmation/audit contract | 9 |
| Finance | `/admin/finances/review/follow-up` | `POST` | Authenticated + route permission | `review_follow_up_endpoint` | Mutation; preserve confirmation/audit contract | 9 |
| Finance | `/admin/finances/review/preview` | `POST` | Authenticated + route permission | `review_preview_endpoint` | Mutation; preserve confirmation/audit contract | 9 |
| Finance | `/admin/finances/review/snooze` | `POST` | Authenticated + route permission | `review_snooze_endpoint` | Mutation; preserve confirmation/audit contract | 9 |
| Finance | `/admin/finances/review/undo/{batch_id}` | `POST` | Authenticated + route permission | `review_undo_endpoint` | Mutation; preserve confirmation/audit contract | 9 |
| Finance | `/admin/finances/savings/{opportunity_key}/review` | `POST` | Authenticated + route permission | `record_savings_review_action` | Mutation; preserve confirmation/audit contract | 9 |
| Finance | `/admin/finances/scenario` | `GET` | Authenticated + route permission | `scenario_get` | Read, navigate, or download | 9 |
| Finance | `/admin/finances/scenario` | `POST` | Authenticated + route permission | `scenario_post` | Mutation; preserve confirmation/audit contract | 9 |
| Finance | `/admin/finances/settings/cash-floor` | `POST` | Authenticated + route permission | `update_cash_floor` | Mutation; preserve confirmation/audit contract | 9 |
| Finance | `/admin/finances/setup` | `GET` | Authenticated + route permission | `finance_setup_page` | Read, navigate, or download | 9 |
| Finance | `/admin/finances/smart-review` | `POST` | Authenticated + route permission | `run_smart_cfo_review` | Mutation; preserve confirmation/audit contract | 9 |
| Finance | `/admin/finances/sync-clickup` | `POST` | Authenticated + route permission | `sync_clickup` | Mutation; preserve confirmation/audit contract | 9 |
| Finance | `/admin/finances/sync-connected-sources` | `POST` | Authenticated + route permission | `sync_connected_sources` | Mutation; preserve confirmation/audit contract | 9 |
| Finance | `/admin/finances/sync-qbo` | `POST` | Authenticated + route permission | `sync_qbo` | Mutation; preserve confirmation/audit contract | 9 |
| Finance | `/admin/finances/sync-qbo-actuals` | `POST` | Authenticated + route permission | `sync_qbo_actuals` | Mutation; preserve confirmation/audit contract | 9 |
| Finance | `/admin/finances/sync-qbo-invoices` | `POST` | Authenticated + route permission | `sync_qbo_invoices_only` | Mutation; preserve confirmation/audit contract | 9 |
| Finance | `/admin/finances/upload` | `GET` | Authenticated + route permission | `upload_form` | Read, navigate, or download | 9 |
| Finance | `/admin/finances/upload` | `POST` | Authenticated + route permission | `upload_submit` | Mutation; preserve confirmation/audit contract | 9 |
| Finance | `/admin/finances/vendors` | `POST` | Authenticated + route permission | `create_vendor_endpoint` | Mutation; preserve confirmation/audit contract | 9 |
| Finance | `/admin/finances/vendors/{vendor_id}` | `POST` | Authenticated + route permission | `update_vendor_endpoint` | Mutation; preserve confirmation/audit contract | 9 |
| Finance | `/admin/finances/vendors/{vendor_id}/delete` | `POST` | Authenticated + route permission | `delete_vendor_endpoint` | Mutation; preserve confirmation/audit contract | 9 |
| Finance | `/admin/finances/whats-coming` | `GET` | Authenticated + route permission | `whats_coming_page` | Read, navigate, or download | 9 |
| Finance | `/admin/finances/whats-coming/bulk` | `POST` | Authenticated + route permission | `whats_coming_bulk` | Mutation; preserve confirmation/audit contract | 9 |
| Finance | `/admin/finances/whats-coming/combine-preview` | `POST` | Authenticated + route permission | `whats_coming_combine_preview` | Mutation; preserve confirmation/audit contract | 9 |
| Finance | `/admin/finances/whats-coming/decide` | `POST` | Authenticated + route permission | `whats_coming_decide` | Mutation; preserve confirmation/audit contract | 9 |
| Finance | `/admin/finances/whats-coming/track-preview` | `POST` | Authenticated + route permission | `whats_coming_track_preview` | Mutation; preserve confirmation/audit contract | 9 |
| Finance | `/admin/finances/whats-coming/undo` | `POST` | Authenticated + route permission | `whats_coming_undo` | Mutation; preserve confirmation/audit contract | 9 |
| Finance | `/admin/finances/whats-coming/vendor-alias/revoke` | `POST` | Authenticated + route permission | `whats_coming_alias_revoke` | Mutation; preserve confirmation/audit contract | 9 |
| Fulfillment | `/admin/fulfillment` | `GET` | Authenticated + route permission | `admin_fulfillment_root` | Read, navigate, or download | 4–5 |
| Fulfillment | `/admin/fulfillment-cs{rest:path}` | `GET` | Authenticated + route permission | `admin_fulfillment_cs_legacy_redirect` | Read, navigate, or download | 4–5 |
| Fulfillment | `/admin/fulfillment/cs` | `GET` | Authenticated + route permission | `admin_fulfillment_cs_root` | Read, navigate, or download | 4–5 |
| Fulfillment | `/admin/fulfillment/cs/` | `GET` | Authenticated + route permission | `admin_fulfillment_cs_dashboard` | Read, navigate, or download | 4–5 |
| Fulfillment | `/admin/fulfillment/cs/reports` | `GET` | Authenticated + route permission | `admin_fulfillment_cs_reports_root` | Read, navigate, or download | 4–5 |
| Fulfillment | `/admin/fulfillment/cs/reports/` | `GET` | Authenticated + route permission | `admin_fulfillment_cs_reports` | Read, navigate, or download | 4–5 |
| Fulfillment | `/admin/fulfillment/cs/reports/latest` | `GET` | Authenticated + route permission | `admin_fulfillment_cs_latest_report` | Read, navigate, or download | 4–5 |
| Fulfillment | `/admin/fulfillment/cs/reports/{report_slug}` | `GET` | Authenticated + route permission | `admin_fulfillment_cs_report_detail` | Read, navigate, or download | 4–5 |
| Fulfillment | `/admin/fulfillment/cs/reports/{report_slug}.html` | `GET` | Authenticated + route permission | `admin_fulfillment_cs_report_html` | Read, navigate, or download | 4–5 |
| Fulfillment | `/admin/fulfillment/cs/reports/{report_slug}.json` | `GET` | Authenticated + route permission | `admin_fulfillment_cs_report_json` | Read, navigate, or download | 4–5 |
| Fulfillment | `/admin/fulfillment/cs/reports/{report_slug}.md` | `GET` | Authenticated + route permission | `admin_fulfillment_cs_report_markdown` | Read, navigate, or download | 4–5 |
| Fulfillment | `/admin/fulfillment/sales` | `GET` | Authenticated + route permission | `landing` | Read, navigate, or download | 4–5 |
| Fulfillment | `/admin/fulfillment/sales/export.csv` | `GET` | Authenticated + route permission | `export_pipeline_csv` | Read, navigate, or download | 4–5 |
| Fulfillment | `/admin/fulfillment/sales/generate` | `POST` | Authenticated + route permission | `generate` | Mutation; preserve confirmation/audit contract | 4–5 |
| Fulfillment | `/admin/fulfillment/sales/runs/{run_id}/costs` | `PATCH` | Authenticated + route permission | `patch_costs` | Mutation; preserve confirmation/audit contract | 4–5 |
| Fulfillment | `/admin/fulfillment/sales/runs/{run_id}/delete` | `POST` | Authenticated + route permission | `delete_run` | Mutation; preserve confirmation/audit contract | 4–5 |
| Fulfillment | `/admin/fulfillment/sales/runs/{run_id}/notes` | `PATCH` | Authenticated + route permission | `patch_notes` | Mutation; preserve confirmation/audit contract | 4–5 |
| Fulfillment | `/admin/fulfillment/sales/runs/{run_id}/preview` | `GET` | Authenticated + route permission | `preview_run` | Read, navigate, or download | 4–5 |
| Fulfillment | `/admin/fulfillment/sales/runs/{run_id}/publish` | `POST` | Authenticated + route permission | `publish_run` | Mutation; preserve confirmation/audit contract | 4–5 |
| Fulfillment | `/admin/fulfillment/sales/runs/{run_id}/quote` | `POST` | Authenticated + route permission | `create_quote` | Mutation; preserve confirmation/audit contract | 4–5 |
| Fulfillment | `/admin/fulfillment/sales/runs/{run_id}/review` | `GET` | Authenticated + route permission | `review_run` | Read, navigate, or download | 4–5 |
| Fulfillment | `/admin/fulfillment/sales/runs/{run_id}/send-brief` | `POST` | Authenticated + route permission | `send_brief_email` | Mutation; preserve confirmation/audit contract | 4–5 |
| Fulfillment | `/admin/fulfillment/sales/runs/{run_id}/stage` | `PATCH` | Authenticated + route permission | `patch_stage` | Mutation; preserve confirmation/audit contract | 4–5 |
| Fulfillment | `/admin/fulfillment/sales/runs/{run_id}/update` | `POST` | Authenticated + route permission | `update_run` | Mutation; preserve confirmation/audit contract | 4–5 |
| HR | `/admin/hr` | `GET` | Authenticated + route permission | `hr_dashboard` | Read, navigate, or download | 8 |
| HR | `/admin/hr/compliance` | `GET` | Authenticated + route permission | `hr_compliance` | Read, navigate, or download | 8 |
| HR | `/admin/hr/compliance/{task_id}` | `POST` | Authenticated + route permission | `hr_compliance_update` | Mutation; preserve confirmation/audit contract | 8 |
| HR | `/admin/hr/contractors` | `GET` | Authenticated + route permission | `hr_contractors` | Read, navigate, or download | 8 |
| HR | `/admin/hr/contractors/payments` | `POST` | Authenticated + route permission | `hr_contractor_payment_create` | Mutation; preserve confirmation/audit contract | 8 |
| HR | `/admin/hr/contractors/payments/{payment_id}` | `POST` | Authenticated + route permission | `hr_contractor_payment_action` | Mutation; preserve confirmation/audit contract | 8 |
| HR | `/admin/hr/contractors/profile` | `POST` | Authenticated + route permission | `hr_contractor_profile_save` | Mutation; preserve confirmation/audit contract | 8 |
| HR | `/admin/hr/employees` | `GET` | Authenticated + route permission | `employees_list` | Read, navigate, or download | 8 |
| HR | `/admin/hr/employees/new` | `GET` | Authenticated + route permission | `employee_new` | Read, navigate, or download | 8 |
| HR | `/admin/hr/employees/new` | `POST` | Authenticated + route permission | `employee_create` | Mutation; preserve confirmation/audit contract | 8 |
| HR | `/admin/hr/employees/{emp_id}` | `GET` | Authenticated + route permission | `employee_edit` | Read, navigate, or download | 8 |
| HR | `/admin/hr/employees/{emp_id}` | `POST` | Authenticated + route permission | `employee_update` | Mutation; preserve confirmation/audit contract | 8 |
| HR | `/admin/hr/employees/{emp_id}/invite` | `POST` | Authenticated + route permission | `employee_invite` | Mutation; preserve confirmation/audit contract | 8 |
| HR | `/admin/hr/employees/{emp_id}/onboarding-correction` | `POST` | Authenticated + route permission | `onboarding_correction_request` | Mutation; preserve confirmation/audit contract | 8 |
| HR | `/admin/hr/employees/{emp_id}/onboarding-review` | `POST` | Authenticated + route permission | `onboarding_employer_review` | Mutation; preserve confirmation/audit contract | 8 |
| HR | `/admin/hr/employees/{emp_id}/status` | `POST` | Authenticated + route permission | `employee_status_update` | Mutation; preserve confirmation/audit contract | 8 |
| HR | `/admin/hr/offboarding` | `GET` | Authenticated + route permission | `hr_offboarding` | Read, navigate, or download | 8 |
| HR | `/admin/hr/offboarding` | `POST` | Authenticated + route permission | `hr_offboarding_create` | Mutation; preserve confirmation/audit contract | 8 |
| HR | `/admin/hr/offboarding/{checklist_id}` | `POST` | Authenticated + route permission | `hr_offboarding_update` | Mutation; preserve confirmation/audit contract | 8 |
| HR | `/admin/hr/onboarding` | `GET` | Authenticated + route permission | `employee_onboarding` | Read, navigate, or download | 8 |
| HR | `/admin/hr/onboarding/attestations` | `POST` | Authenticated + route permission | `onboarding_attestations` | Mutation; preserve confirmation/audit contract | 8 |
| HR | `/admin/hr/onboarding/profile` | `POST` | Authenticated + route permission | `onboarding_profile` | Mutation; preserve confirmation/audit contract | 8 |
| HR | `/admin/hr/onboarding/w4` | `POST` | Authenticated + route permission | `onboarding_w4` | Mutation; preserve confirmation/audit contract | 8 |
| HR | `/admin/hr/pay-statements` | `GET` | Authenticated + route permission | `hr_pay_statements` | Read, navigate, or download | 8 |
| HR | `/admin/hr/pay-statements/{run_id}` | `GET` | Authenticated + route permission | `hr_pay_statement_detail` | Read, navigate, or download | 8 |
| HR | `/admin/hr/payroll` | `GET` | Authenticated + route permission | `hr_payroll` | Read, navigate, or download | 8 |
| HR | `/admin/hr/payroll/inputs` | `POST` | Authenticated + route permission | `hr_payroll_input` | Mutation; preserve confirmation/audit contract | 8 |
| HR | `/admin/hr/payroll/inputs/{input_id}/decision` | `POST` | Authenticated + route permission | `hr_payroll_input_decision` | Mutation; preserve confirmation/audit contract | 8 |
| HR | `/admin/hr/payroll/liabilities/{liability_id}` | `POST` | Authenticated + route permission | `hr_payroll_liability_action` | Mutation; preserve confirmation/audit contract | 8 |
| HR | `/admin/hr/payroll/prepare` | `POST` | Authenticated + route permission | `hr_payroll_prepare` | Mutation; preserve confirmation/audit contract | 8 |
| HR | `/admin/hr/payroll/runs/{run_id}` | `GET` | Authenticated + route permission | `hr_payroll_run_review` | Read, navigate, or download | 8 |
| HR | `/admin/hr/payroll/runs/{run_id}/approve` | `GET` | Authenticated + route permission | `hr_payroll_approval_review` | Read, navigate, or download | 8 |
| HR | `/admin/hr/payroll/runs/{run_id}/checks` | `POST` | Authenticated + route permission | `hr_payroll_issue_check` | Mutation; preserve confirmation/audit contract | 8 |
| HR | `/admin/hr/payroll/runs/{run_id}/checks/confirm` | `POST` | Authenticated + route permission | `hr_payroll_confirm_check` | Mutation; preserve confirmation/audit contract | 8 |
| HR | `/admin/hr/payroll/runs/{run_id}/checks/reissue` | `POST` | Authenticated + route permission | `hr_payroll_reissue_check` | Mutation; preserve confirmation/audit contract | 8 |
| HR | `/admin/hr/payroll/runs/{run_id}/close` | `POST` | Authenticated + route permission | `hr_payroll_close_run` | Mutation; preserve confirmation/audit contract | 8 |
| HR | `/admin/hr/payroll/runs/{run_id}/provider` | `POST` | Authenticated + route permission | `hr_payroll_provider_action` | Mutation; preserve confirmation/audit contract | 8 |
| HR | `/admin/hr/payroll/runs/{run_id}/provider.csv` | `GET` | Authenticated + route permission | `hr_payroll_provider_export` | Read, navigate, or download | 8 |
| HR | `/admin/hr/payroll/{run_id}/approve` | `POST` | Authenticated + route permission | `hr_payroll_approve` | Mutation; preserve confirmation/audit contract | 8 |
| HR | `/admin/hr/payroll/{run_id}/reject` | `POST` | Authenticated + route permission | `hr_payroll_reject` | Mutation; preserve confirmation/audit contract | 8 |
| HR | `/admin/hr/policies` | `GET` | Authenticated + route permission | `hr_policies` | Read, navigate, or download | 8 |
| HR | `/admin/hr/policies/acknowledge` | `POST` | Authenticated + route permission | `hr_policy_acknowledge` | Mutation; preserve confirmation/audit contract | 8 |
| HR | `/admin/hr/reports` | `GET` | Authenticated + route permission | `hr_reports` | Read, navigate, or download | 8 |
| HR | `/admin/hr/reports/backup.zip` | `GET` | Authenticated + route permission | `hr_backup_zip` | Read, navigate, or download | 8 |
| HR | `/admin/hr/reports/{kind}.csv` | `GET` | Authenticated + route permission | `hr_report_csv` | Read, navigate, or download | 8 |
| HR | `/admin/hr/settings` | `GET` | Authenticated + route permission | `hr_settings` | Read, navigate, or download | 8 |
| HR | `/admin/hr/settings` | `POST` | Authenticated + route permission | `hr_settings_save` | Mutation; preserve confirmation/audit contract | 8 |
| HR | `/admin/hr/settings/company` | `POST` | Authenticated + route permission | `hr_company_profile_save` | Mutation; preserve confirmation/audit contract | 8 |
| HR | `/admin/hr/settings/handbook` | `POST` | Authenticated + route permission | `hr_handbook_publish` | Mutation; preserve confirmation/audit contract | 8 |
| HR | `/admin/hr/settings/legacy-import` | `GET` | Authenticated + route permission | `hr_legacy_import_page` | Read, navigate, or download | 8 |
| HR | `/admin/hr/settings/legacy-import/commit` | `POST` | Authenticated + route permission | `hr_legacy_import_commit` | Mutation; preserve confirmation/audit contract | 8 |
| HR | `/admin/hr/settings/legacy-import/preview` | `POST` | Authenticated + route permission | `hr_legacy_import_preview` | Mutation; preserve confirmation/audit contract | 8 |
| HR | `/admin/hr/settings/opening-balance` | `POST` | Authenticated + route permission | `hr_opening_balance_save` | Mutation; preserve confirmation/audit contract | 8 |
| HR | `/admin/hr/settings/opening-balance/{balance_id}/decision` | `POST` | Authenticated + route permission | `hr_opening_balance_decision` | Mutation; preserve confirmation/audit contract | 8 |
| HR | `/admin/hr/settings/provider-contract.json` | `GET` | Authenticated + route permission | `hr_provider_contract` | Read, navigate, or download | 8 |
| HR | `/admin/hr/settings/qualified-review` | `POST` | Authenticated + route permission | `hr_qualified_review_save` | Mutation; preserve confirmation/audit contract | 8 |
| HR | `/admin/hr/setup` | `GET` | Authenticated + route permission | `hr_setup` | Read, navigate, or download | 8 |
| HR | `/admin/hr/teams` | `GET` | Authenticated + route permission | `teams_list` | Read, navigate, or download | 8 |
| HR | `/admin/hr/teams` | `POST` | Authenticated + route permission | `team_create` | Mutation; preserve confirmation/audit contract | 8 |
| HR | `/admin/hr/teams/{team_id}` | `GET` | Authenticated + route permission | `team_detail` | Read, navigate, or download | 8 |
| HR | `/admin/hr/teams/{team_id}` | `POST` | Authenticated + route permission | `team_update` | Mutation; preserve confirmation/audit contract | 8 |
| HR | `/admin/hr/teams/{team_id}/members` | `POST` | Authenticated + route permission | `team_member_update` | Mutation; preserve confirmation/audit contract | 8 |
| HR | `/admin/hr/time` | `GET` | Authenticated + route permission | `hr_time` | Read, navigate, or download | 8 |
| HR | `/admin/hr/time/clock` | `POST` | Authenticated + route permission | `hr_time_clock` | Mutation; preserve confirmation/audit contract | 8 |
| HR | `/admin/hr/time/corrections/{correction_id}/decision` | `POST` | Authenticated + route permission | `hr_time_correction_decision` | Mutation; preserve confirmation/audit contract | 8 |
| HR | `/admin/hr/time/missed-punch` | `POST` | Authenticated + route permission | `hr_time_missed_punch` | Mutation; preserve confirmation/audit contract | 8 |
| HR | `/admin/hr/time/pto` | `POST` | Authenticated + route permission | `hr_pto_request` | Mutation; preserve confirmation/audit contract | 8 |
| HR | `/admin/hr/time/pto/{request_id}/decision` | `POST` | Authenticated + route permission | `hr_pto_decision` | Mutation; preserve confirmation/audit contract | 8 |
| HR | `/admin/hr/time/pto/{request_id}/withdraw` | `POST` | Authenticated + route permission | `hr_pto_withdraw` | Mutation; preserve confirmation/audit contract | 8 |
| HR | `/admin/hr/time/timesheets/submit` | `POST` | Authenticated + route permission | `hr_timesheet_submit` | Mutation; preserve confirmation/audit contract | 8 |
| HR | `/admin/hr/time/timesheets/{approval_id}/decision` | `POST` | Authenticated + route permission | `hr_timesheet_decision` | Mutation; preserve confirmation/audit contract | 8 |
| HR | `/admin/hr/time/{time_entry_id}/correction` | `POST` | Authenticated + route permission | `hr_time_correction` | Mutation; preserve confirmation/audit contract | 8 |
| Public deliverable | `/amazon-profit-calculator/runtime` | `GET` | Public or token-gated | `profit_calculator_app` | Read, navigate, or download | 10 |
| Public deliverable | `/api/public/amazon-bulk-profitability/catalog/{asin}` | `GET` | Public or token-gated | `bulk_profitability_catalog_proxy` | Read, navigate, or download | 10 |
| Public deliverable | `/api/public/amazon-bulk-profitability/profitability/estimate` | `POST` | Public or token-gated | `bulk_profitability_estimate_proxy` | Mutation; preserve confirmation/audit contract | 10 |
| Public deliverable | `/api/public/amazon-profit-calculator/catalog/{asin}` | `GET` | Public or token-gated | `profit_calculator_catalog_proxy` | Read, navigate, or download | 10 |
| Public deliverable | `/api/public/amazon-profit-calculator/profitability/estimate` | `POST` | Public or token-gated | `profit_calculator_estimate_proxy` | Mutation; preserve confirmation/audit contract | 10 |
| Public deliverable | `/api/public/building/availability` | `GET` | Public or token-gated | `list_public_availability` | Read, navigate, or download | 10 |
| Public deliverable | `/api/public/building/bookings/status` | `GET` | Public or token-gated | `public_customer_status` | Read, navigate, or download | 10 |
| Public deliverable | `/api/public/building/content` | `GET` | Public or token-gated | `get_public_building_content` | Read, navigate, or download | 10 |
| Public deliverable | `/api/public/building/event-estimates` | `POST` | Public or token-gated | `calculate_public_event_estimate` | Mutation; preserve confirmation/audit contract | 10 |
| Public deliverable | `/api/public/building/inquiries` | `POST` | Public or token-gated | `create_inquiry` | Mutation; preserve confirmation/audit contract | 10 |
| Public deliverable | `/api/public/building/offerings` | `GET` | Public or token-gated | `list_public_offerings` | Read, navigate, or download | 10 |
| Public deliverable | `/api/public/building/offerings/{slug}` | `GET` | Public or token-gated | `get_public_offering` | Read, navigate, or download | 10 |
| Public deliverable | `/api/public/building/unsubscribe` | `GET` | Public or token-gated | `unsubscribe` | Read, navigate, or download | 10 |
| Public deliverable | `/api/public/fulfillment/rate-sheet/result/{correlation_id}` | `GET` | Public or token-gated | `rate_sheet_result` | Read, navigate, or download | 10 |
| Public deliverable | `/api/public/fulfillment/rate-sheet/status/{correlation_id}` | `GET` | Public or token-gated | `rate_sheet_status` | Read, navigate, or download | 10 |
| Public deliverable | `/api/public/fulfillment/rate-sheet/taste` | `POST` | Public or token-gated | `rate_sheet_taste` | Mutation; preserve confirmation/audit contract | 10 |
| Public deliverable | `/api/public/fulfillment/rate-sheet/unlock` | `POST` | Public or token-gated | `rate_sheet_unlock` | Mutation; preserve confirmation/audit contract | 10 |
| Public deliverable | `/api/public/marketing/advertising-audit` | `POST` | Public or token-gated | `advertising_audit_intake` | Mutation; preserve confirmation/audit contract | 10 |
| Public deliverable | `/api/public/marketing/advertising-audit/{run_id}` | `GET` | Public or token-gated | `advertising_audit_status` | Read, navigate, or download | 10 |
| Public deliverable | `/api/public/marketing/analysis` | `POST` | Public or token-gated | `marketing_analysis_intake` | Mutation; preserve confirmation/audit contract | 10 |
| Public deliverable | `/api/public/marketing/analysis/status` | `GET` | Public or token-gated | `marketing_analysis_status` | Read, navigate, or download | 10 |
| Public deliverable | `/api/public/marketing/intake` | `POST` | Public or token-gated | `marketing_site_intake_create` | Mutation; preserve confirmation/audit contract | 10 |
| Public deliverable | `/api/public/marketing/intake/{intake_id}` | `GET` | Public or token-gated | `marketing_site_intake_status` | Read, navigate, or download | 10 |
| Public deliverable | `/api/public/marketing/intake/{intake_id}/booked` | `POST` | Public or token-gated | `marketing_site_intake_booked` | Mutation; preserve confirmation/audit contract | 10 |
| Public deliverable | `/api/public/marketing/intake/{intake_id}/needs` | `POST` | Public or token-gated | `marketing_site_intake_needs` | Mutation; preserve confirmation/audit contract | 10 |
| Public deliverable | `/api/public/marketing/intake/{intake_id}/unlock` | `POST` | Public or token-gated | `marketing_site_intake_unlock` | Mutation; preserve confirmation/audit contract | 10 |
| Public deliverable | `/decks/{deck_slug}/{run_id}/{token}` | `GET` | Public or token-gated | `deck_export_slug_view` | Read, navigate, or download | 10 |
| Public deliverable | `/decks/{deck_slug}/{run_id}/{token}/heartbeat` | `POST` | Public or token-gated | `deck_heartbeat` | Mutation; preserve confirmation/audit contract | 10 |
| Public deliverable | `/decks/{deck_slug}/{run_id}/{token}/preview.png` | `GET` | Public or token-gated | `deck_export_preview_image` | Read, navigate, or download | 10 |
| Public deliverable | `/decks/{deck_slug}/{run_id}/{token}/story` | `GET` | Public or token-gated | `deck_story_view` | Read, navigate, or download | 10 |
| Public deliverable | `/decks/{deck_slug}/{run_id}/{token}/story.md` | `GET` | Public or token-gated | `deck_story_download` | Read, navigate, or download | 10 |
| Sales | `/admin/sales` | `GET` | Authenticated + route permission | `sales_operator` | Read, navigate, or download | 4–5 |
| Sales | `/admin/sales-decks` | `GET` | Authenticated + route permission | `admin_sales_decks` | Read, navigate, or download | 4–5 |
| Sales | `/admin/sales/` | `GET` | Authenticated + route permission | `sales_operator` | Read, navigate, or download | 4–5 |
| Sales | `/admin/sales/deals` | `GET` | Authenticated + route permission | `deal_board` | Read, navigate, or download | 4–5 |
| Sales | `/admin/sales/deals/alerts/send` | `POST` | Authenticated + route permission | `send_slack_alerts` | Mutation; preserve confirmation/audit contract | 4–5 |
| Sales | `/admin/sales/deals/cleanup` | `GET` | Authenticated + route permission | `batch_cleanup` | Read, navigate, or download | 4–5 |
| Sales | `/admin/sales/deals/cleanup` | `POST` | Authenticated + route permission | `batch_cleanup_apply` | Mutation; preserve confirmation/audit contract | 4–5 |
| Sales | `/admin/sales/deals/create` | `GET` | Authenticated + route permission | `create_deal_form` | Read, navigate, or download | 4–5 |
| Sales | `/admin/sales/deals/create` | `POST` | Authenticated + route permission | `create_deal` | Mutation; preserve confirmation/audit contract | 4–5 |
| Sales | `/admin/sales/deals/sync` | `POST` | Authenticated + route permission | `trigger_sync` | Mutation; preserve confirmation/audit contract | 4–5 |
| Sales | `/admin/sales/deals/sync/status` | `GET` | Authenticated + route permission | `sync_status` | Read, navigate, or download | 4–5 |
| Sales | `/admin/sales/deals/{deal_id}` | `GET` | Authenticated + route permission | `deal_detail` | Read, navigate, or download | 4–5 |
| Sales | `/admin/sales/deals/{deal_id}/actions/approve` | `POST` | Authenticated + route permission | `approve_action` | Mutation; preserve confirmation/audit contract | 4–5 |
| Sales | `/admin/sales/deals/{deal_id}/draft-followup` | `GET` | Authenticated + route permission | `draft_followup` | Read, navigate, or download | 4–5 |
| Sales | `/admin/sales/deals/{deal_id}/send-followup` | `POST` | Authenticated + route permission | `send_followup` | Mutation; preserve confirmation/audit contract | 4–5 |
| Sales | `/admin/sales/decks` | `GET` | Authenticated + route permission | `admin_sales_decks_canonical_redirect` | Read, navigate, or download | 4–5 |
| Sales | `/admin/sales/decks/` | `GET` | Authenticated + route permission | `admin_sales_decks` | Read, navigate, or download | 4–5 |
| Sales | `/admin/sales/diag` | `GET` | Authenticated + route permission | `sales_diag` | Read, navigate, or download | 4–5 |
| Sales | `/admin/sales/fix-queue` | `GET` | Authenticated + route permission | `admin_dashboard` | Read, navigate, or download | 4–5 |
| Sales | `/admin/sales/reps` | `GET` | Authenticated + route permission | `rep_accountability` | Read, navigate, or download | 4–5 |
| Sales | `/admin/sales/snapshot` | `GET` | Authenticated + route permission | `sales_operator_snapshot` | Read, navigate, or download | 4–5 |
| Sales | `/admin/sales/writeback` | `POST` | Authenticated + route permission | `sales_operator_writeback` | Mutation; preserve confirmation/audit contract | 4–5 |
| Service / API | `/` | `GET` | Service contract | `root` | Read, navigate, or download | Exempt |
| Service / API | `/amazon-bulk-profitability/runtime` | `GET` | Service contract | `bulk_profitability_app` | Read, navigate, or download | Exempt |
| Service / API | `/api/admin/dashboard-data` | `GET` | Service contract | `admin_dashboard_data` | Read, navigate, or download | Exempt |
| Service / API | `/api/admin/deck-runs/{run_id}/delete` | `POST` | Service contract | `internal_admin_delete_deck_run` | Mutation; preserve confirmation/audit contract | Exempt |
| Service / API | `/api/admin/digital-shelf/generate-deck` | `POST` | Service contract | `internal_digital_shelf_generate_deck` | Mutation; preserve confirmation/audit contract | Exempt |
| Service / API | `/api/admin/executive-data` | `GET` | Service contract | `admin_executive_data` | Read, navigate, or download | Exempt |
| Service / API | `/api/admin/generate-deck` | `POST` | Service contract | `internal_admin_generate_deck` | Mutation; preserve confirmation/audit contract | Exempt |
| Service / API | `/api/admin/gmail-drafts` | `POST` | Service contract | `admin_create_gmail_drafts` | Mutation; preserve confirmation/audit contract | Exempt |
| Service / API | `/api/clickup/sync` | `POST` | Service contract | `sync_clickup_tasks` | Mutation; preserve confirmation/audit contract | Exempt |
| Service / API | `/api/communications/events` | `POST` | Service contract | `ingest_communication_event` | Mutation; preserve confirmation/audit contract | Exempt |
| Service / API | `/api/discovery/clickup-schema` | `POST` | Service contract | `discover_clickup_schema` | Mutation; preserve confirmation/audit contract | Exempt |
| Service / API | `/api/integrations/instantly/webhook` | `POST` | Service contract | `ingest_instantly_webhook` | Mutation; preserve confirmation/audit contract | Exempt |
| Service / API | `/api/integrations/plaid/link-initialize.js` | `GET` | Service contract | `plaid_link_sdk` | Read, navigate, or download | Exempt |
| Service / API | `/api/integrations/plaid/webhook` | `POST` | Service contract | `plaid_webhook` | Mutation; preserve confirmation/audit contract | Exempt |
| Service / API | `/api/integrations/resend/webhook` | `POST` | Service contract | `ingest_resend_webhook` | Mutation; preserve confirmation/audit contract | Exempt |
| Service / API | `/api/integrations/stripe/webhook` | `POST` | Service contract | `stripe_webhook` | Mutation; preserve confirmation/audit contract | Exempt |
| Service / API | `/api/internal/building/agreement-readiness/packages` | `POST` | Service contract | `prepare_agreement_package` | Mutation; preserve confirmation/audit contract | Exempt |
| Service / API | `/api/internal/building/agreement-readiness/packages/{agreement_id}/transition` | `POST` | Service contract | `transition_agreement_package` | Mutation; preserve confirmation/audit contract | Exempt |
| Service / API | `/api/internal/building/agreement-readiness/payments/{payment_id}/transition` | `POST` | Service contract | `transition_payment_readiness` | Mutation; preserve confirmation/audit contract | Exempt |
| Service / API | `/api/internal/building/agreement-readiness/reservations/{reservation_id}` | `GET` | Service contract | `get_reservation_readiness` | Read, navigate, or download | Exempt |
| Service / API | `/api/internal/building/agreement-readiness/templates/{template_id}` | `PUT` | Service contract | `upsert_agreement_template` | Mutation; preserve confirmation/audit contract | Exempt |
| Service / API | `/api/internal/building/agreement-readiness/templates/{template_id}/transition` | `POST` | Service contract | `transition_agreement_template` | Mutation; preserve confirmation/audit contract | Exempt |
| Service / API | `/api/internal/building/analytics` | `GET` | Service contract | `get_building_analytics` | Read, navigate, or download | Exempt |
| Service / API | `/api/internal/building/availability` | `POST` | Service contract | `create_availability_block` | Mutation; preserve confirmation/audit contract | Exempt |
| Service / API | `/api/internal/building/billing/accounts/{account_id}` | `PUT` | Service contract | `upsert_billing_account` | Mutation; preserve confirmation/audit contract | Exempt |
| Service / API | `/api/internal/building/billing/adjustments` | `GET` | Service contract | `list_adjustments` | Read, navigate, or download | Exempt |
| Service / API | `/api/internal/building/billing/adjustments` | `POST` | Service contract | `request_adjustment` | Mutation; preserve confirmation/audit contract | Exempt |
| Service / API | `/api/internal/building/billing/adjustments/{adjustment_id}/approve` | `POST` | Service contract | `approve_adjustment` | Mutation; preserve confirmation/audit contract | Exempt |
| Service / API | `/api/internal/building/billing/adjustments/{adjustment_id}/evidence` | `POST` | Service contract | `record_adjustment_evidence` | Mutation; preserve confirmation/audit contract | Exempt |
| Service / API | `/api/internal/building/billing/collections` | `GET` | Service contract | `list_collection_cases` | Read, navigate, or download | Exempt |
| Service / API | `/api/internal/building/billing/collections/refresh` | `POST` | Service contract | `refresh_collection_cases` | Mutation; preserve confirmation/audit contract | Exempt |
| Service / API | `/api/internal/building/billing/collections/{case_id}` | `PUT` | Service contract | `transition_collection_case` | Mutation; preserve confirmation/audit contract | Exempt |
| Service / API | `/api/internal/building/billing/collections/{case_id}/remind` | `POST` | Service contract | `send_collection_reminder` | Mutation; preserve confirmation/audit contract | Exempt |
| Service / API | `/api/internal/building/billing/invoices` | `GET` | Service contract | `list_invoices` | Read, navigate, or download | Exempt |
| Service / API | `/api/internal/building/billing/invoices` | `POST` | Service contract | `create_invoice_from_schedule` | Mutation; preserve confirmation/audit contract | Exempt |
| Service / API | `/api/internal/building/billing/invoices/{invoice_id}/accounting-link` | `PUT` | Service contract | `record_accounting_link` | Mutation; preserve confirmation/audit contract | Exempt |
| Service / API | `/api/internal/building/billing/qbo-export` | `GET` | Service contract | `qbo_export_queue` | Read, navigate, or download | Exempt |
| Service / API | `/api/internal/building/billing/schedules/{schedule_id}` | `PUT` | Service contract | `upsert_billing_schedule` | Mutation; preserve confirmation/audit contract | Exempt |
| Service / API | `/api/internal/building/billing/schedules/{schedule_id}/approve` | `POST` | Service contract | `approve_billing_schedule` | Mutation; preserve confirmation/audit contract | Exempt |
| Service / API | `/api/internal/building/bookings` | `GET` | Service contract | `list_reservations` | Read, navigate, or download | Exempt |
| Service / API | `/api/internal/building/bookings` | `POST` | Service contract | `create_reservation` | Mutation; preserve confirmation/audit contract | Exempt |
| Service / API | `/api/internal/building/bookings/event-reviews` | `POST` | Service contract | `create_event_review` | Mutation; preserve confirmation/audit contract | Exempt |
| Service / API | `/api/internal/building/bookings/tour-inquiry-handoffs` | `POST` | Service contract | `create_tour_inquiry_handoff` | Mutation; preserve confirmation/audit contract | Exempt |
| Service / API | `/api/internal/building/bookings/tours/{tour_id}` | `PUT` | Service contract | `update_tour` | Mutation; preserve confirmation/audit contract | Exempt |
| Service / API | `/api/internal/building/bookings/{reservation_id}/agreements` | `POST` | Service contract | `record_agreement` | Mutation; preserve confirmation/audit contract | Exempt |
| Service / API | `/api/internal/building/bookings/{reservation_id}/customer-status-access` | `POST` | Service contract | `prepare_customer_status_access` | Mutation; preserve confirmation/audit contract | Exempt |
| Service / API | `/api/internal/building/bookings/{reservation_id}/deposit-evidence` | `POST` | Service contract | `record_deposit` | Mutation; preserve confirmation/audit contract | Exempt |
| Service / API | `/api/internal/building/bookings/{reservation_id}/lifecycle` | `GET` | Service contract | `get_event_lifecycle` | Read, navigate, or download | Exempt |
| Service / API | `/api/internal/building/bookings/{reservation_id}/proposals` | `GET` | Service contract | `list_proposals` | Read, navigate, or download | Exempt |
| Service / API | `/api/internal/building/bookings/{reservation_id}/proposals` | `POST` | Service contract | `record_proposal` | Mutation; preserve confirmation/audit contract | Exempt |
| Service / API | `/api/internal/building/bookings/{reservation_id}/tours` | `GET` | Service contract | `list_tours` | Read, navigate, or download | Exempt |
| Service / API | `/api/internal/building/bookings/{reservation_id}/tours` | `POST` | Service contract | `create_tour` | Mutation; preserve confirmation/audit contract | Exempt |
| Service / API | `/api/internal/building/bookings/{reservation_id}/transition` | `POST` | Service contract | `transition_reservation` | Mutation; preserve confirmation/audit contract | Exempt |
| Service / API | `/api/internal/building/calendar/projections` | `GET` | Service contract | `list_calendar_projections` | Read, navigate, or download | Exempt |
| Service / API | `/api/internal/building/calendar/readiness` | `GET` | Service contract | `calendar_readiness` | Read, navigate, or download | Exempt |
| Service / API | `/api/internal/building/calendar/sync` | `POST` | Service contract | `sync_calendar_projections` | Mutation; preserve confirmation/audit contract | Exempt |
| Service / API | `/api/internal/building/checklists` | `GET` | Service contract | `list_checklists` | Read, navigate, or download | Exempt |
| Service / API | `/api/internal/building/checklists/items/{item_id}/status` | `POST` | Service contract | `update_checklist_item_status` | Mutation; preserve confirmation/audit contract | Exempt |
| Service / API | `/api/internal/building/checklists/{checklist_id}/items` | `POST` | Service contract | `add_checklist_item` | Mutation; preserve confirmation/audit contract | Exempt |
| Service / API | `/api/internal/building/content` | `GET` | Service contract | `list_content` | Read, navigate, or download | Exempt |
| Service / API | `/api/internal/building/content/{kind}/{record_id}` | `PUT` | Service contract | `upsert_content` | Mutation; preserve confirmation/audit contract | Exempt |
| Service / API | `/api/internal/building/content/{kind}/{record_id}/review` | `POST` | Service contract | `review_content` | Mutation; preserve confirmation/audit contract | Exempt |
| Service / API | `/api/internal/building/crm/campaigns/{campaign_id}` | `PUT` | Service contract | `upsert_campaign` | Mutation; preserve confirmation/audit contract | Exempt |
| Service / API | `/api/internal/building/crm/campaigns/{campaign_id}/approve` | `POST` | Service contract | `approve_campaign` | Mutation; preserve confirmation/audit contract | Exempt |
| Service / API | `/api/internal/building/crm/campaigns/{campaign_id}/preview` | `POST` | Service contract | `preview_campaign` | Mutation; preserve confirmation/audit contract | Exempt |
| Service / API | `/api/internal/building/crm/campaigns/{campaign_id}/retry` | `POST` | Service contract | `retry_campaign_failures` | Mutation; preserve confirmation/audit contract | Exempt |
| Service / API | `/api/internal/building/crm/campaigns/{campaign_id}/review` | `POST` | Service contract | `review_campaign` | Mutation; preserve confirmation/audit contract | Exempt |
| Service / API | `/api/internal/building/crm/campaigns/{campaign_id}/schedule` | `POST` | Service contract | `schedule_campaign` | Mutation; preserve confirmation/audit contract | Exempt |
| Service / API | `/api/internal/building/crm/campaigns/{campaign_id}/send` | `POST` | Service contract | `send_campaign` | Mutation; preserve confirmation/audit contract | Exempt |
| Service / API | `/api/internal/building/crm/campaigns/{campaign_id}/test-send` | `POST` | Service contract | `test_send_campaign` | Mutation; preserve confirmation/audit contract | Exempt |
| Service / API | `/api/internal/building/crm/campaigns/{campaign_id}/unschedule` | `POST` | Service contract | `unschedule_campaign` | Mutation; preserve confirmation/audit contract | Exempt |
| Service / API | `/api/internal/building/crm/contacts/merge` | `POST` | Service contract | `merge_contacts` | Mutation; preserve confirmation/audit contract | Exempt |
| Service / API | `/api/internal/building/crm/contacts/merge/preview` | `POST` | Service contract | `preview_contact_merge` | Mutation; preserve confirmation/audit contract | Exempt |
| Service / API | `/api/internal/building/crm/contacts/{contact_id}` | `GET` | Service contract | `get_contact` | Read, navigate, or download | Exempt |
| Service / API | `/api/internal/building/crm/contacts/{contact_id}` | `PUT` | Service contract | `upsert_contact` | Mutation; preserve confirmation/audit contract | Exempt |
| Service / API | `/api/internal/building/crm/contacts/{contact_id}/operational-preference` | `PUT` | Service contract | `set_operational_preference` | Mutation; preserve confirmation/audit contract | Exempt |
| Service / API | `/api/internal/building/crm/contacts/{contact_id}/preference` | `PUT` | Service contract | `set_preference` | Mutation; preserve confirmation/audit contract | Exempt |
| Service / API | `/api/internal/building/crm/contacts/{contact_id}/relationships` | `POST` | Service contract | `add_relationship` | Mutation; preserve confirmation/audit contract | Exempt |
| Service / API | `/api/internal/building/crm/contacts/{contact_id}/relationships/{relationship_id}/review` | `PUT` | Service contract | `review_relationship` | Mutation; preserve confirmation/audit contract | Exempt |
| Service / API | `/api/internal/building/crm/scheduled-campaigns/run` | `POST` | Service contract | `run_scheduled_campaigns` | Mutation; preserve confirmation/audit contract | Exempt |
| Service / API | `/api/internal/building/crm/segments/bootstrap` | `POST` | Service contract | `bootstrap_standard_segments` | Mutation; preserve confirmation/audit contract | Exempt |
| Service / API | `/api/internal/building/crm/segments/{segment_id}` | `PUT` | Service contract | `upsert_segment` | Mutation; preserve confirmation/audit contract | Exempt |
| Service / API | `/api/internal/building/crm/segments/{segment_id}/preview` | `GET` | Service contract | `preview_segment` | Read, navigate, or download | Exempt |
| Service / API | `/api/internal/building/inquiries/{inquiry_id}/lifecycle` | `POST` | Service contract | `update_inquiry_lifecycle` | Mutation; preserve confirmation/audit contract | Exempt |
| Service / API | `/api/internal/building/inquiries/{inquiry_id}/retry-hubspot` | `POST` | Service contract | `retry_inquiry_hubspot` | Mutation; preserve confirmation/audit contract | Exempt |
| Service / API | `/api/internal/building/offerings/{offering_id}` | `PUT` | Service contract | `upsert_offering` | Mutation; preserve confirmation/audit contract | Exempt |
| Service / API | `/api/internal/building/offerings/{offering_id}/publication-readiness` | `GET` | Service contract | `get_offering_publication_readiness` | Read, navigate, or download | Exempt |
| Service / API | `/api/internal/building/offerings/{offering_id}/rate-plans` | `GET` | Service contract | `list_rate_plans` | Read, navigate, or download | Exempt |
| Service / API | `/api/internal/building/offerings/{offering_id}/rate-plans/{rate_plan_id}` | `PUT` | Service contract | `upsert_rate_plan` | Mutation; preserve confirmation/audit contract | Exempt |
| Service / API | `/api/internal/building/privacy/contacts/{contact_id}/correct` | `POST` | Service contract | `correct_contact_internal` | Mutation; preserve confirmation/audit contract | Exempt |
| Service / API | `/api/internal/building/privacy/contacts/{contact_id}/export` | `GET` | Service contract | `export_contact_internal` | Read, navigate, or download | Exempt |
| Service / API | `/api/internal/building/privacy/contacts/{contact_id}/suppress` | `POST` | Service contract | `suppress_contact_internal` | Mutation; preserve confirmation/audit contract | Exempt |
| Service / API | `/api/internal/building/privacy/requests` | `GET` | Service contract | `list_requests` | Read, navigate, or download | Exempt |
| Service / API | `/api/internal/building/privacy/requests` | `POST` | Service contract | `create_request` | Mutation; preserve confirmation/audit contract | Exempt |
| Service / API | `/api/internal/building/privacy/requests/{request_id}/transition` | `POST` | Service contract | `transition_request` | Mutation; preserve confirmation/audit contract | Exempt |
| Service / API | `/api/internal/building/service-requests` | `GET` | Service contract | `list_service_requests` | Read, navigate, or download | Exempt |
| Service / API | `/api/internal/building/service-requests` | `POST` | Service contract | `create_service_request` | Mutation; preserve confirmation/audit contract | Exempt |
| Service / API | `/api/internal/building/service-requests/{request_id}/transition` | `POST` | Service contract | `transition_service_request` | Mutation; preserve confirmation/audit contract | Exempt |
| Service / API | `/api/internal/building/spaces/{space_id}` | `PUT` | Service contract | `upsert_space` | Mutation; preserve confirmation/audit contract | Exempt |
| Service / API | `/api/internal/building/spaces/{space_id}/media/{media_id}` | `DELETE` | Service contract | `delete_space_media` | Mutation; preserve confirmation/audit contract | Exempt |
| Service / API | `/api/internal/building/spaces/{space_id}/media/{media_id}` | `PUT` | Service contract | `upsert_space_media` | Mutation; preserve confirmation/audit contract | Exempt |
| Service / API | `/api/jobs/building-holds/run` | `POST` | Service contract | `building_hold_expiration_job` | Mutation; preserve confirmation/audit contract | Exempt |
| Service / API | `/api/jobs/content/run` | `POST` | Service contract | `content_run` | Mutation; preserve confirmation/audit contract | Exempt |
| Service / API | `/api/jobs/content/source-assets` | `POST` | Service contract | `content_source_assets` | Mutation; preserve confirmation/audit contract | Exempt |
| Service / API | `/api/jobs/daily-digest/run` | `POST` | Service contract | `run_daily_digest_job` | Mutation; preserve confirmation/audit contract | Exempt |
| Service / API | `/api/jobs/gmail-sync/run` | `POST` | Service contract | `run_gmail_sync_job` | Mutation; preserve confirmation/audit contract | Exempt |
| Service / API | `/api/jobs/hr-reminders/run` | `POST` | Service contract | `hr_reminders_run` | Mutation; preserve confirmation/audit contract | Exempt |
| Service / API | `/api/jobs/outbound-morning/run` | `POST` | Service contract | `run_scheduled_outbound` | Mutation; preserve confirmation/audit contract | Exempt |
| Service / API | `/api/jobs/sales-operator/run` | `POST` | Service contract | `sales_operator_run_job` | Mutation; preserve confirmation/audit contract | Exempt |
| Service / API | `/api/jobs/stale-leads/run` | `POST` | Service contract | `run_stale_lead_job` | Mutation; preserve confirmation/audit contract | Exempt |
| Service / API | `/api/jobs/website-ops/health` | `GET` | Service contract | `website_ops_runtime_health` | Read, navigate, or download | Exempt |
| Service / API | `/api/jobs/website-ops/run` | `POST` | Service contract | `run_scheduled_website_ops` | Mutation; preserve confirmation/audit contract | Exempt |
| Service / API | `/brand-intake` | `GET` | Service contract | `public_intake_guide` | Read, navigate, or download | Exempt |
| Service / API | `/brand/{slug}/{report_id}/{token}` | `GET` | Service contract | `public_brand_page` | Read, navigate, or download | Exempt |
| Service / API | `/callback` | `GET` | Service contract | `qb_callback` | Read, navigate, or download | Exempt |
| Service / API | `/connect` | `GET` | Service contract | `qb_connect` | Read, navigate, or download | Exempt |
| Service / API | `/deck-exports/{run_id}/{token}` | `GET` | Service contract | `deck_export_view` | Read, navigate, or download | Exempt |
| Service / API | `/disconnect` | `GET` | Service contract | `qb_disconnect_get` | Read, navigate, or download | Exempt |
| Service / API | `/disconnect` | `POST` | Service contract | `qb_disconnect_post` | Mutation; preserve confirmation/audit contract | Exempt |
| Service / API | `/fulfillment-costs/{run_id}/{token}` | `GET` | Service contract | `fulfillment_cost_form` | Read, navigate, or download | Exempt |
| Service / API | `/fulfillment-costs/{run_id}/{token}` | `POST` | Service contract | `save_fulfillment_cost_form` | Mutation; preserve confirmation/audit contract | Exempt |
| Service / API | `/health` | `GET` | Service contract | `health` | Read, navigate, or download | Exempt |
| Service / API | `/health/live` | `GET` | Service contract | `health_live` | Read, navigate, or download | Exempt |
| Service / API | `/health/ready` | `GET` | Service contract | `health_ready` | Read, navigate, or download | Exempt |
| Service / API | `/health/storage` | `GET` | Service contract | `health_storage` | Read, navigate, or download | Exempt |
| Service / API | `/rate-sheets/{slug}/{run_id}/{token}` | `GET` | Service contract | `rate_sheet_view` | Read, navigate, or download | Exempt |
| Service / API | `/rate-sheets/{slug}/{run_id}/{token}/heartbeat` | `POST` | Service contract | `rate_sheet_heartbeat` | Mutation; preserve confirmation/audit contract | Exempt |
| Service / API | `/rate-sheets/{slug}/{run_id}/{token}/requote` | `POST` | Service contract | `rate_sheet_requote` | Mutation; preserve confirmation/audit contract | Exempt |
| Website Ops | `/admin/website-ops` | `GET` | Authenticated + route permission | `admin_website_ops` | Read, navigate, or download | 7 |
| Website Ops | `/admin/website-ops/candidates` | `GET` | Authenticated + route permission | `admin_website_ops_candidates` | Read, navigate, or download | 7 |
| Website Ops | `/admin/website-ops/feedback/{feedback_id}` | `GET` | Authenticated + route permission | `admin_website_ops_feedback_detail` | Read, navigate, or download | 7 |
| Website Ops | `/admin/website-ops/indexing` | `GET` | Authenticated + route permission | `admin_website_ops_indexing` | Read, navigate, or download | 7 |
| Website Ops | `/admin/website-ops/indexing` | `POST` | Authenticated + route permission | `admin_website_ops_indexing_import` | Mutation; preserve confirmation/audit contract | 7 |
| Website Ops | `/admin/website-ops/queries` | `GET` | Authenticated + route permission | `admin_website_ops_queries` | Read, navigate, or download | 7 |
| Website Ops | `/admin/website-ops/queue` | `GET` | Authenticated + route permission | `admin_website_ops_queue` | Read, navigate, or download | 7 |
| Website Ops | `/admin/website-ops/reports` | `GET` | Authenticated + route permission | `admin_website_ops_reports` | Read, navigate, or download | 7 |
| Website Ops | `/admin/website-ops/reports/latest` | `GET` | Authenticated + route permission | `admin_website_ops_reports_latest` | Read, navigate, or download | 7 |
| Website Ops | `/admin/website-ops/reports/{mode}/{slug}` | `GET` | Authenticated + route permission | `admin_website_ops_report_detail` | Read, navigate, or download | 7 |

## State coverage contract

Every user-visible HTML family must verify the states that apply: default, loading, empty, filtered-empty, partial, stale, error, permission denied, success, long-running, and destructive confirmation.

Binary, JSON, CSV, webhook, and internal service routes are intentionally exempt from visual migration. Their contracts, security, and error behavior remain in scope for regression testing.
