<?php

if (! defined('ABSPATH')) {
    exit;
}

final class Anata_Website_Ops
{
    private const OPTION_KEY = 'anata_ops_settings';
    private const LOG_OPTION_KEY = 'anata_ops_debug_log';
    private const META_BEFORE = '_anata_ops_before_snapshot';
    private const META_AFTER = '_anata_ops_after_snapshot';
    private const META_TITLE = '_anata_ops_meta_title';
    private const META_DESCRIPTION = '_anata_ops_meta_description';
    private const META_CANONICAL = '_anata_ops_canonical_url';
    private const MAX_LOG_ENTRIES = 100;
    private const MAX_TIMESTAMP_SKEW = 300;
    private const SUPPORTED_ACTIONS = ['inject_faq_block', 'meta_update', 'meta_title_update', 'meta_description_update', 'canonical_update'];
    private const MVP_ALLOWED_TARGET_PATHS = ['/services/fulfillment/', '/contact/'];

    private static ?Anata_Website_Ops $instance = null;

    public static function instance(): Anata_Website_Ops
    {
        if (self::$instance === null) {
            self::$instance = new self();
        }

        return self::$instance;
    }

    private function __construct()
    {
        add_action('rest_api_init', [$this, 'register_routes']);
        add_action('admin_menu', [$this, 'register_admin_page']);
        add_action('admin_init', [$this, 'register_settings']);
        add_action('admin_post_anata_ops_restore_snapshot', [$this, 'handle_restore_snapshot']);
        add_filter('pre_get_document_title', [$this, 'filter_document_title'], 20);
        add_action('wp_head', [$this, 'render_seo_meta_tags'], 20);
    }

    public function register_routes(): void
    {
        register_rest_route(
            'anata-ops/v1',
            '/faq-insert',
            [
                'methods' => WP_REST_Server::CREATABLE,
                'callback' => [$this, 'handle_faq_insert'],
                'permission_callback' => [$this, 'permission_check'],
                'args' => [],
            ]
        );
        register_rest_route(
            'anata-ops/v1',
            '/meta-update',
            [
                'methods' => WP_REST_Server::CREATABLE,
                'callback' => [$this, 'handle_meta_update'],
                'permission_callback' => [$this, 'permission_check'],
                'args' => [],
            ]
        );
    }

    public function permission_check(WP_REST_Request $request)
    {
        $params = $request->get_json_params();
        if (! is_array($params)) {
            return new WP_Error('anata_ops_invalid_json', 'Invalid JSON payload.', ['status' => 400]);
        }

        try {
            $this->validate_request($params);

            return true;
        } catch (Throwable $error) {
            $this->log_execution('error', $params, 'rejected', $error->getMessage());

            return new WP_Error('anata_ops_forbidden', $error->getMessage(), ['status' => 403]);
        }
    }

    public function register_settings(): void
    {
        register_setting(
            'anata_ops_settings_group',
            self::OPTION_KEY,
            [
                'type' => 'array',
                'sanitize_callback' => [$this, 'sanitize_settings'],
                'default' => $this->default_settings(),
            ]
        );
    }

    public function sanitize_settings($input): array
    {
        $defaults = $this->default_settings();
        $input = is_array($input) ? $input : [];
        $allowed_post_types = isset($input['allowed_post_types']) && is_array($input['allowed_post_types'])
            ? array_values(array_filter(array_map('sanitize_key', $input['allowed_post_types'])))
            : $defaults['allowed_post_types'];

        return [
            'shared_secret' => sanitize_text_field((string) ($input['shared_secret'] ?? '')),
            'allowed_post_types' => $allowed_post_types ?: $defaults['allowed_post_types'],
            'allowed_page_ids' => sanitize_text_field((string) ($input['allowed_page_ids'] ?? '')),
        ];
    }

    public function register_admin_page(): void
    {
        add_options_page(
            'Anata Website Ops',
            'Anata Website Ops',
            'manage_options',
            'anata-website-ops',
            [$this, 'render_admin_page']
        );
    }

    public function render_admin_page(): void
    {
        if (! current_user_can('manage_options')) {
            return;
        }

        $settings = $this->settings();
        $public_post_types = get_post_types(['public' => true], 'objects');
        $log_entries = get_option(self::LOG_OPTION_KEY, []);
        ?>
        <div class="wrap">
            <h1>Anata Website Ops</h1>
            <?php if (isset($_GET['restored']) && $_GET['restored'] === '1') : ?>
                <div class="notice notice-success"><p>Website Ops snapshot restored.</p></div>
            <?php endif; ?>
            <?php if (! empty($_GET['restore_error'])) : ?>
                <div class="notice notice-error"><p><?php echo esc_html(wp_unslash((string) $_GET['restore_error'])); ?></p></div>
            <?php endif; ?>
            <form method="post" action="options.php">
                <?php settings_fields('anata_ops_settings_group'); ?>
                <table class="form-table" role="presentation">
                    <tr>
                        <th scope="row"><label for="anata-ops-shared-secret">Shared secret</label></th>
                        <td>
                            <input id="anata-ops-shared-secret" name="<?php echo esc_attr(self::OPTION_KEY); ?>[shared_secret]" type="text" class="regular-text" value="<?php echo esc_attr($settings['shared_secret']); ?>">
                            <p class="description">Use the same secret in the agent service as <code>ANATA_OPS_SHARED_SECRET</code>.</p>
                        </td>
                    </tr>
                    <tr>
                        <th scope="row">Allowed post types</th>
                        <td>
                            <?php foreach ($public_post_types as $post_type) : ?>
                                <label style="display:block;margin-bottom:6px;">
                                    <input
                                        type="checkbox"
                                        name="<?php echo esc_attr(self::OPTION_KEY); ?>[allowed_post_types][]"
                                        value="<?php echo esc_attr($post_type->name); ?>"
                                        <?php checked(in_array($post_type->name, $settings['allowed_post_types'], true)); ?>
                                    >
                                    <?php echo esc_html($post_type->labels->singular_name); ?> (<code><?php echo esc_html($post_type->name); ?></code>)
                                </label>
                            <?php endforeach; ?>
                        </td>
                    </tr>
                    <tr>
                        <th scope="row"><label for="anata-ops-allowed-page-ids">Allowed page IDs</label></th>
                        <td>
                            <textarea id="anata-ops-allowed-page-ids" name="<?php echo esc_attr(self::OPTION_KEY); ?>[allowed_page_ids]" rows="4" class="large-text"><?php echo esc_textarea($settings['allowed_page_ids']); ?></textarea>
                            <p class="description">Optional comma-separated allowlist. Leave blank to allow all IDs within the allowed post types.</p>
                        </td>
                    </tr>
                </table>
                <?php submit_button('Save Website Ops Settings'); ?>
            </form>

            <hr>
            <h2>Restore snapshot</h2>
            <form method="post" action="<?php echo esc_url(admin_url('admin-post.php')); ?>">
                <input type="hidden" name="action" value="anata_ops_restore_snapshot">
                <?php wp_nonce_field('anata_ops_restore_snapshot'); ?>
                <table class="form-table" role="presentation">
                    <tr>
                        <th scope="row"><label for="anata-ops-restore-post-id">Post ID</label></th>
                        <td>
                            <input id="anata-ops-restore-post-id" name="post_id" type="number" min="1" class="small-text">
                            <p class="description">Restores the most recent stored <code><?php echo esc_html(self::META_BEFORE); ?></code> snapshot for this post.</p>
                        </td>
                    </tr>
                </table>
                <?php submit_button('Restore previous snapshot', 'secondary'); ?>
            </form>

            <hr>
            <h2>Debug log</h2>
            <?php if (empty($log_entries)) : ?>
                <p>No plugin execution entries yet.</p>
            <?php else : ?>
                <table class="widefat striped">
                    <thead>
                        <tr>
                            <th>Time</th>
                            <th>Level</th>
                            <th>Message</th>
                            <th>Context</th>
                        </tr>
                    </thead>
                    <tbody>
                    <?php foreach ($log_entries as $entry) : ?>
                        <tr>
                            <td><?php echo esc_html((string) ($entry['time'] ?? '')); ?></td>
                            <td><?php echo esc_html((string) ($entry['level'] ?? 'info')); ?></td>
                            <td><?php echo esc_html((string) ($entry['message'] ?? '')); ?></td>
                            <td><pre style="white-space:pre-wrap;"><?php echo esc_html(wp_json_encode($entry['context'] ?? [], JSON_PRETTY_PRINT)); ?></pre></td>
                        </tr>
                    <?php endforeach; ?>
                    </tbody>
                </table>
            <?php endif; ?>
        </div>
        <?php
    }

    public function handle_faq_insert(WP_REST_Request $request): WP_REST_Response
    {
        try {
            $params = $request->get_json_params();
            if (! is_array($params)) {
                throw new RuntimeException('Invalid JSON payload.');
            }

            $validated = $this->validate_request($params);
            if (($validated['action_type'] ?? '') !== 'inject_faq_block') {
                throw new RuntimeException('Unsupported action_type.');
            }

            $post = $this->resolve_target_post($validated);
            $this->assert_post_allowed($post);

            $before_snapshot = $this->load_page_snapshot($post->ID);
            if (($before_snapshot['mode'] ?? '') !== 'elementor') {
                throw new RuntimeException('Target page does not expose Elementor data. MVP execution refuses post_content mutations.');
            }
            $duplicate_exists = $this->faq_exists($before_snapshot['html']);
            $insertion_strategy = $this->resolve_insertion_strategy($before_snapshot['html']);

            if (! empty($validated['dry_run'])) {
                $response = [
                    'ok' => true,
                    'dry_run' => true,
                    'validation_passed' => ! $duplicate_exists,
                    'action_type' => 'inject_faq_block',
                    'target_post_id' => $post->ID,
                    'target_url' => get_permalink($post),
                    'insertion_strategy' => $insertion_strategy,
                    'updated_via' => $before_snapshot['mode'],
                    'duplicate_faq_detected' => $duplicate_exists,
                    'before_faq_count' => $this->faq_marker_count($before_snapshot['html']),
                    'after_faq_count' => $this->faq_marker_count($before_snapshot['html']),
                    'backup_reference' => sprintf('post-meta:%s,%s', self::META_BEFORE, self::META_AFTER),
                ];
                $this->log_execution('info', $validated, 'dry_run', '');

                return new WP_REST_Response($response, 200);
            }

            if ($duplicate_exists) {
                throw new RuntimeException('FAQ block already exists on the target page.');
            }

            $faq_html = $this->build_faq_html($validated);
            $result = $this->insert_faq($post->ID, $before_snapshot, $faq_html);
            $after_snapshot = $this->load_page_snapshot($post->ID);

            update_post_meta(
                $post->ID,
                self::META_BEFORE,
                [
                    'captured_at' => current_time('mysql', true),
                    'snapshot' => $before_snapshot,
                ]
            );
            update_post_meta(
                $post->ID,
                self::META_AFTER,
                [
                    'captured_at' => current_time('mysql', true),
                    'snapshot' => $after_snapshot,
                ]
            );

            $response = [
                'ok' => true,
                'action_type' => 'inject_faq_block',
                'target_post_id' => $post->ID,
                'target_url' => get_permalink($post),
                'insertion_strategy' => $result['insertion_strategy'],
                'updated_via' => $result['updated_via'],
                'before_faq_count' => $this->faq_marker_count($before_snapshot['html']),
                'after_faq_count' => $this->faq_marker_count($after_snapshot['html']),
                'backup_reference' => sprintf('post-meta:%s,%s', self::META_BEFORE, self::META_AFTER),
            ];
            $this->log_execution('info', $validated, 'success', '', $response);

            return new WP_REST_Response($response, 200);
        } catch (Throwable $error) {
            $params = $request->get_json_params();
            $this->log_execution('error', is_array($params) ? $params : [], 'error', $error->getMessage());

            return new WP_REST_Response(
                [
                    'ok' => false,
                    'error' => $error->getMessage(),
                ],
                400
            );
        }
    }

    public function handle_meta_update(WP_REST_Request $request): WP_REST_Response
    {
        try {
            $params = $request->get_json_params();
            if (! is_array($params)) {
                throw new RuntimeException('Invalid JSON payload.');
            }

            $validated = $this->validate_request($params);
            if (! $this->is_meta_action((string) ($validated['action_type'] ?? ''))) {
                throw new RuntimeException('Unsupported action_type.');
            }

            $post = $this->resolve_target_post($validated);
            $this->assert_post_allowed($post);

            $before_snapshot = $this->load_page_snapshot($post->ID);
            $before_meta = $this->load_seo_meta($post->ID);

            $planned_meta = $this->planned_meta_values($before_meta, $validated);
            $response = [
                'ok' => true,
                'dry_run' => ! empty($validated['dry_run']),
                'action_type' => (string) $validated['action_type'],
                'target_post_id' => $post->ID,
                'target_url' => get_permalink($post),
                'before_meta' => $before_meta,
                'after_meta' => $planned_meta,
                'updated_fields' => $this->updated_meta_fields($before_meta, $planned_meta),
                'backup_reference' => sprintf('post-meta:%s,%s', self::META_BEFORE, self::META_AFTER),
            ];

            if (! empty($validated['dry_run'])) {
                $response['validation_passed'] = true;
                $this->log_execution('info', $validated, 'dry_run', '', $response);

                return new WP_REST_Response($response, 200);
            }

            $this->store_snapshots($post->ID, $before_snapshot, $before_meta, null);
            $this->apply_seo_meta($post->ID, $planned_meta);
            $after_snapshot = $this->load_page_snapshot($post->ID);
            $after_meta = $this->load_seo_meta($post->ID);
            $this->store_snapshots($post->ID, $before_snapshot, $before_meta, ['snapshot' => $after_snapshot, 'seo_meta' => $after_meta]);

            $response['after_meta'] = $after_meta;
            $response['updated_fields'] = $this->updated_meta_fields($before_meta, $after_meta);
            $this->log_execution('info', $validated, 'success', '', $response);

            return new WP_REST_Response($response, 200);
        } catch (Throwable $error) {
            $params = $request->get_json_params();
            $this->log_execution('error', is_array($params) ? $params : [], 'error', $error->getMessage());

            return new WP_REST_Response(
                [
                    'ok' => false,
                    'error' => $error->getMessage(),
                ],
                400
            );
        }
    }

    private function default_settings(): array
    {
        return [
            'shared_secret' => '',
            'allowed_post_types' => ['page'],
            'allowed_page_ids' => '',
        ];
    }

    public function handle_restore_snapshot(): void
    {
        if (! current_user_can('manage_options')) {
            wp_die('Unauthorized', 403);
        }

        check_admin_referer('anata_ops_restore_snapshot');

        $post_id = isset($_POST['post_id']) ? (int) $_POST['post_id'] : 0;
        $redirect = add_query_arg(
            ['page' => 'anata-website-ops'],
            admin_url('options-general.php')
        );
        if ($post_id <= 0) {
            wp_safe_redirect(add_query_arg('restore_error', rawurlencode('A valid post ID is required.'), $redirect));

            exit;
        }

        try {
            $this->restore_snapshot($post_id);
            $this->log('info', 'Snapshot restored from admin.', ['post_id' => $post_id]);
            wp_safe_redirect(add_query_arg('restored', '1', $redirect));
        } catch (Throwable $error) {
            $this->log('error', 'Snapshot restore failed.', ['post_id' => $post_id, 'error' => $error->getMessage()]);
            wp_safe_redirect(add_query_arg('restore_error', rawurlencode($error->getMessage()), $redirect));
        }

        exit;
    }

    private function settings(): array
    {
        $settings = get_option(self::OPTION_KEY, []);
        $settings = is_array($settings) ? $settings : [];

        return wp_parse_args($settings, $this->default_settings());
    }

    private function allowed_page_ids(): array
    {
        $raw = (string) ($this->settings()['allowed_page_ids'] ?? '');
        if ($raw === '') {
            return [];
        }

        return array_values(
            array_filter(
                array_map(
                    'intval',
                    preg_split('/[\s,]+/', $raw) ?: []
                )
            )
        );
    }

    private function validate_request(array $params): array
    {
        $settings = $this->settings();
        $secret = (string) ($settings['shared_secret'] ?? '');
        if ($secret === '') {
            throw new RuntimeException('The Website Ops shared secret is not configured.');
        }

        $required = ['action_type', 'request_timestamp', 'signature'];
        foreach ($required as $field) {
            if (! array_key_exists($field, $params)) {
                throw new RuntimeException(sprintf('Missing required field: %s', $field));
            }
        }

        $timestamp = strtotime((string) $params['request_timestamp']);
        if (! $timestamp) {
            throw new RuntimeException('Invalid request timestamp.');
        }
        if (abs(time() - $timestamp) > self::MAX_TIMESTAMP_SKEW) {
            throw new RuntimeException('Request timestamp is outside the allowed execution window.');
        }

        $action_type = sanitize_key((string) $params['action_type']);
        if (! in_array($action_type, self::SUPPORTED_ACTIONS, true)) {
            throw new RuntimeException('Unsupported action_type.');
        }

        $normalized = [
            'target_post_id' => isset($params['target_post_id']) ? (int) $params['target_post_id'] : null,
            'target_url' => isset($params['target_url']) ? esc_url_raw((string) $params['target_url']) : '',
            'action_type' => $action_type,
            'request_timestamp' => gmdate('c', $timestamp),
            'dry_run' => ! empty($params['dry_run']),
        ];

        if (empty($normalized['target_post_id']) && empty($normalized['target_url'])) {
            throw new RuntimeException('A target_post_id or target_url is required.');
        }

        if ($action_type === 'inject_faq_block') {
            if (! array_key_exists('heading', $params) || ! array_key_exists('questions', $params)) {
                throw new RuntimeException('Missing FAQ fields.');
            }
            $questions = is_array($params['questions']) ? $params['questions'] : [];
            $definitions = isset($params['definitions']) && is_array($params['definitions']) ? $params['definitions'] : [];
            $normalized['heading'] = sanitize_text_field((string) $params['heading']);
            $normalized['questions'] = array_values(array_filter(array_map([$this, 'normalize_question_item'], $questions)));
            $normalized['definitions'] = array_values(array_filter(array_map('sanitize_text_field', $definitions)));
            if (empty($normalized['questions'])) {
                throw new RuntimeException('At least one valid FAQ question is required.');
            }
        } elseif ($this->is_meta_action($action_type)) {
            $normalized['meta_title'] = sanitize_text_field((string) ($params['meta_title'] ?? ''));
            $normalized['meta_description'] = sanitize_text_field((string) ($params['meta_description'] ?? ''));
            $normalized['canonical_url'] = esc_url_raw((string) ($params['canonical_url'] ?? ''));
            if ($action_type === 'meta_update' && $normalized['meta_title'] === '' && $normalized['meta_description'] === '' && $normalized['canonical_url'] === '') {
                throw new RuntimeException('meta_update requires at least one meta field.');
            }
            if ($action_type === 'meta_title_update' && $normalized['meta_title'] === '') {
                throw new RuntimeException('meta_title_update requires meta_title.');
            }
            if ($action_type === 'meta_description_update' && $normalized['meta_description'] === '') {
                throw new RuntimeException('meta_description_update requires meta_description.');
            }
            if ($action_type === 'canonical_update' && $normalized['canonical_url'] === '') {
                throw new RuntimeException('canonical_update requires canonical_url.');
            }
        }

        $expected = hash_hmac('sha256', $this->canonical_json($normalized), $secret);
        if (! hash_equals($expected, (string) $params['signature'])) {
            throw new RuntimeException('Invalid request signature.');
        }

        return $normalized;
    }

    private function normalize_question_item($item): ?array
    {
        if (! is_array($item)) {
            return null;
        }

        $question = sanitize_text_field((string) ($item['question'] ?? ''));
        $answer = wp_kses_post((string) ($item['answer'] ?? ''));
        if ($question === '' || $answer === '') {
            return null;
        }

        return [
            'question' => $question,
            'answer' => $answer,
        ];
    }

    public function filter_document_title(string $title): string
    {
        if (! is_singular()) {
            return $title;
        }
        $post_id = get_queried_object_id();
        if (! $post_id) {
            return $title;
        }
        $meta_title = (string) get_post_meta($post_id, self::META_TITLE, true);

        return $meta_title !== '' ? $meta_title : $title;
    }

    public function render_seo_meta_tags(): void
    {
        if (! is_singular()) {
            return;
        }
        $post_id = get_queried_object_id();
        if (! $post_id) {
            return;
        }
        $meta = $this->load_seo_meta($post_id);
        if ($meta['meta_description'] !== '') {
            echo sprintf("<meta name=\"description\" content=\"%s\" />\n", esc_attr($meta['meta_description']));
        }
        if ($meta['canonical_url'] !== '') {
            echo sprintf("<link rel=\"canonical\" href=\"%s\" />\n", esc_url($meta['canonical_url']));
        }
    }

    private function canonical_json($value): string
    {
        $normalized = $this->sort_recursive($value);

        return wp_json_encode($normalized, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE);
    }

    private function sort_recursive($value)
    {
        if (is_array($value)) {
            if ($this->is_assoc($value)) {
                ksort($value);
            }
            foreach ($value as $key => $item) {
                $value[$key] = $this->sort_recursive($item);
            }
        }

        return $value;
    }

    private function is_assoc(array $value): bool
    {
        return array_keys($value) !== range(0, count($value) - 1);
    }

    private function resolve_target_post(array $payload): WP_Post
    {
        if (! empty($payload['target_post_id'])) {
            $post = get_post((int) $payload['target_post_id']);
            if ($post instanceof WP_Post) {
                return $post;
            }
        }

        if (! empty($payload['target_url'])) {
            $post_id = url_to_postid($payload['target_url']);
            if ($post_id) {
                $post = get_post($post_id);
                if ($post instanceof WP_Post) {
                    return $post;
                }
            }
        }

        throw new RuntimeException('The target page could not be resolved.');
    }

    private function is_meta_action(string $action_type): bool
    {
        return in_array($action_type, ['meta_update', 'meta_title_update', 'meta_description_update', 'canonical_update'], true);
    }

    private function assert_post_allowed(WP_Post $post): void
    {
        $settings = $this->settings();
        $allowed_post_types = (array) ($settings['allowed_post_types'] ?? ['page']);
        if (! in_array($post->post_type, $allowed_post_types, true)) {
            throw new RuntimeException('The target post type is not allowed.');
        }

        $allowed_ids = $this->allowed_page_ids();
        if ($allowed_ids && ! in_array((int) $post->ID, $allowed_ids, true)) {
            throw new RuntimeException('The target page is not in the Website Ops allowlist.');
        }

        $path = trailingslashit((string) wp_parse_url(get_permalink($post), PHP_URL_PATH));
        if (! in_array($path, self::MVP_ALLOWED_TARGET_PATHS, true)) {
            throw new RuntimeException('The target page is outside the MVP Website Ops allowlist.');
        }
    }

    private function load_page_snapshot(int $post_id): array
    {
        $elementor_raw = get_post_meta($post_id, '_elementor_data', true);
        $elementor_data = [];
        if (is_string($elementor_raw) && $elementor_raw !== '') {
            $decoded = json_decode(wp_unslash($elementor_raw), true);
            if (is_array($decoded)) {
                $elementor_data = $decoded;
            }
        }

        if ($elementor_data) {
            return [
                'mode' => 'elementor',
                'elementor_data' => $elementor_data,
                'html' => $this->elementor_snapshot_html($elementor_data),
            ];
        }

        $post = get_post($post_id);
        $html = $post instanceof WP_Post ? (string) $post->post_content : '';

        return [
            'mode' => 'post_content',
            'elementor_data' => [],
            'html' => $html,
        ];
    }

    private function load_seo_meta(int $post_id): array
    {
        $canonical = (string) get_post_meta($post_id, self::META_CANONICAL, true);
        if ($canonical === '') {
            $canonical = (string) get_permalink($post_id);
        }

        return [
            'meta_title' => (string) get_post_meta($post_id, self::META_TITLE, true),
            'meta_description' => (string) get_post_meta($post_id, self::META_DESCRIPTION, true),
            'canonical_url' => $canonical,
        ];
    }

    private function planned_meta_values(array $before_meta, array $validated): array
    {
        $after = $before_meta;
        if (array_key_exists('meta_title', $validated) && $validated['meta_title'] !== '') {
            $after['meta_title'] = (string) $validated['meta_title'];
        }
        if (array_key_exists('meta_description', $validated) && $validated['meta_description'] !== '') {
            $after['meta_description'] = (string) $validated['meta_description'];
        }
        if (array_key_exists('canonical_url', $validated) && $validated['canonical_url'] !== '') {
            $after['canonical_url'] = (string) $validated['canonical_url'];
        }

        return $after;
    }

    private function updated_meta_fields(array $before_meta, array $after_meta): array
    {
        $updated = [];
        foreach (['meta_title', 'meta_description', 'canonical_url'] as $field) {
            if ((string) ($before_meta[$field] ?? '') !== (string) ($after_meta[$field] ?? '')) {
                $updated[] = $field;
            }
        }

        return $updated;
    }

    private function apply_seo_meta(int $post_id, array $meta): void
    {
        if (array_key_exists('meta_title', $meta)) {
            update_post_meta($post_id, self::META_TITLE, (string) $meta['meta_title']);
        }
        if (array_key_exists('meta_description', $meta)) {
            update_post_meta($post_id, self::META_DESCRIPTION, (string) $meta['meta_description']);
        }
        if (array_key_exists('canonical_url', $meta)) {
            update_post_meta($post_id, self::META_CANONICAL, (string) $meta['canonical_url']);
        }
        clean_post_cache($post_id);
    }

    private function store_snapshots(int $post_id, ?array $content_snapshot, ?array $before_meta, ?array $after_bundle): void
    {
        if ($content_snapshot !== null && $before_meta !== null) {
            update_post_meta(
                $post_id,
                self::META_BEFORE,
                [
                    'captured_at' => current_time('mysql', true),
                    'snapshot' => $content_snapshot,
                    'seo_meta' => $before_meta,
                ]
            );
        }
        if ($after_bundle !== null) {
            update_post_meta(
                $post_id,
                self::META_AFTER,
                [
                    'captured_at' => current_time('mysql', true),
                    'snapshot' => $after_bundle['snapshot'] ?? [],
                    'seo_meta' => $after_bundle['seo_meta'] ?? [],
                ]
            );
        }
    }

    private function faq_exists(string $html): bool
    {
        $normalized = strtolower($html);
        if (strpos($normalized, 'class="anata-faq"') !== false || strpos($normalized, "class='anata-faq'") !== false) {
            return true;
        }
        if (strpos($normalized, 'frequently asked') !== false || preg_match('/\bfaq\b/i', $html)) {
            return true;
        }

        return strpos(str_replace(' ', '', $normalized), '"@type":"faqpage"') !== false;
    }

    private function faq_marker_count(string $html): int
    {
        $normalized = strtolower($html);
        $count = 0;
        $count += substr_count($normalized, 'class="anata-faq"');
        $count += substr_count($normalized, "class='anata-faq'");
        $count += preg_match_all('/\bfaq\b/i', $html);
        $count += substr_count($normalized, 'frequently asked');
        $count += substr_count(str_replace(' ', '', $normalized), '"@type":"faqpage"');

        return $count;
    }

    private function build_faq_html(array $payload): string
    {
        $items = '';
        foreach ($payload['questions'] as $item) {
            $items .= sprintf(
                '<div class="faq-item"><h3>%s</h3><p>%s</p></div>',
                esc_html($item['question']),
                wp_kses_post($item['answer'])
            );
        }

        $definition_html = '';
        if (! empty($payload['definitions'])) {
            $definition_html .= '<div class="faq-definitions">';
            foreach (array_slice($payload['definitions'], 0, 3) as $definition) {
                $definition_html .= sprintf('<p>%s</p>', esc_html($definition));
            }
            $definition_html .= '</div>';
        }

        return sprintf(
            '<section class="anata-faq"><h2>%s</h2>%s%s</section>',
            esc_html($payload['heading']),
            $definition_html,
            $items
        );
    }

    private function insert_faq(int $post_id, array $snapshot, string $faq_html): array
    {
        $strategy = $this->resolve_insertion_strategy($snapshot['html']);

        if ($snapshot['mode'] === 'elementor') {
            $elements = $snapshot['elementor_data'];
            $insert_index = $this->resolve_top_level_insertion_index($elements, $strategy);
            array_splice($elements, $insert_index, 0, [$this->make_text_widget($faq_html)]);
            update_post_meta($post_id, '_elementor_data', wp_json_encode($elements));
        } else {
            $post = get_post($post_id);
            $content = $post instanceof WP_Post ? (string) $post->post_content : '';
            $updated_content = $this->insert_html_fragment($content, $faq_html, $strategy);
            wp_update_post(
                [
                    'ID' => $post_id,
                    'post_content' => $updated_content,
                ]
            );
        }

        clean_post_cache($post_id);
        if (class_exists('\Elementor\Plugin')) {
            \Elementor\Plugin::$instance->files_manager->clear_cache();
        }

        return [
            'insertion_strategy' => $strategy,
            'updated_via' => $snapshot['mode'],
        ];
    }

    private function resolve_insertion_strategy(string $html): string
    {
        if ($html === '') {
            return 'end_of_content';
        }

        if (preg_match('/<\/(?:p|section|div|h2)>/i', $html, $match, PREG_OFFSET_CAPTURE)) {
            $major_end = $match[0][1] + strlen($match[0][0]);
            if (preg_match('/(book|contact|schedule|analysis|call|get started)/i', $html, $cta_match, PREG_OFFSET_CAPTURE)) {
                if ($major_end <= $cta_match[0][1]) {
                    return 'after_first_major_section';
                }

                return 'before_cta';
            }

            return 'after_first_major_section';
        }

        if (preg_match('/(book|contact|schedule|analysis|call|get started)/i', $html)) {
            return 'before_cta';
        }

        return 'end_of_content';
    }

    private function resolve_top_level_insertion_index(array $elements, string $strategy): int
    {
        if ($strategy === 'before_cta') {
            foreach ($elements as $index => $element) {
                if (($element['widgetType'] ?? '') === 'button') {
                    return $index;
                }
            }
        }

        if ($strategy === 'after_first_major_section') {
            $relevant = 0;
            foreach ($elements as $index => $element) {
                $widget_type = (string) ($element['widgetType'] ?? '');
                if (in_array($widget_type, ['heading', 'text-editor', 'html'], true)) {
                    $relevant++;
                }
                if ($relevant >= 2) {
                    return $index + 1;
                }
            }
        }

        return count($elements);
    }

    private function insert_html_fragment(string $content, string $faq_html, string $strategy): string
    {
        if ($strategy === 'before_cta' && preg_match('/(book|contact|schedule|analysis|call|get started)/i', $content, $match, PREG_OFFSET_CAPTURE)) {
            $position = (int) $match[0][1];

            return substr($content, 0, $position) . $faq_html . substr($content, $position);
        }

        if ($strategy === 'after_first_major_section' && preg_match('/<\/(?:p|section|div|h2)>/i', $content, $match, PREG_OFFSET_CAPTURE)) {
            $position = (int) $match[0][1] + strlen($match[0][0]);

            return substr($content, 0, $position) . $faq_html . substr($content, $position);
        }

        return $content . $faq_html;
    }

    private function make_text_widget(string $html): array
    {
        return [
            'id' => substr(wp_hash('anata-faq-' . wp_generate_uuid4()), 0, 7),
            'elType' => 'widget',
            'widgetType' => 'text-editor',
            'settings' => [
                'editor' => $html,
            ],
            'elements' => [],
        ];
    }

    private function elementor_snapshot_html(array $elements): string
    {
        $chunks = [];
        foreach ($this->flatten_widget_refs($elements) as $element) {
            $widget_type = (string) ($element['widgetType'] ?? '');
            $text = $this->widget_text($element);
            if ($text === '') {
                continue;
            }
            if ($widget_type === 'heading') {
                $level = strtolower((string) ($element['settings']['header_size'] ?? 'h2'));
                $chunks[] = sprintf('<%1$s>%2$s</%1$s>', esc_attr($level), $text);
            } elseif ($widget_type === 'button') {
                $chunks[] = sprintf('<div class="cta-section"><button>%s</button></div>', esc_html(wp_strip_all_tags($text)));
            } else {
                $chunks[] = $text;
            }
        }

        return implode("\n", $chunks);
    }

    private function flatten_widget_refs(array $elements): array
    {
        $refs = [];
        foreach ($elements as $element) {
            if (! is_array($element)) {
                continue;
            }
            if (! empty($element['widgetType'])) {
                $refs[] = $element;
            }
            if (! empty($element['elements']) && is_array($element['elements'])) {
                $refs = array_merge($refs, $this->flatten_widget_refs($element['elements']));
            }
        }

        return $refs;
    }

    private function widget_text(array $element): string
    {
        $widget_type = (string) ($element['widgetType'] ?? '');
        $settings = isset($element['settings']) && is_array($element['settings']) ? $element['settings'] : [];
        if ($widget_type === 'heading') {
            return (string) ($settings['title'] ?? '');
        }
        if ($widget_type === 'button') {
            return (string) ($settings['text'] ?? '');
        }
        if ($widget_type === 'html') {
            return (string) ($settings['html'] ?? '');
        }

        return (string) ($settings['editor'] ?? '');
    }

    private function log(string $level, string $message, array $context = []): void
    {
        $entries = get_option(self::LOG_OPTION_KEY, []);
        $entries = is_array($entries) ? $entries : [];
        array_unshift(
            $entries,
            [
                'time' => current_time('mysql', true),
                'level' => $level,
                'message' => $message,
                'context' => $context,
            ]
        );
        $entries = array_slice($entries, 0, self::MAX_LOG_ENTRIES);
        update_option(self::LOG_OPTION_KEY, $entries, false);
    }

    private function log_execution(string $level, array $payload, string $result, string $error_message = '', array $context = []): void
    {
        $this->log(
            $level,
            'Website Ops execution event.',
            array_merge(
                [
                    'request_time' => (string) ($payload['request_timestamp'] ?? ''),
                    'target_url' => (string) ($payload['target_url'] ?? ''),
                    'target_post_id' => isset($payload['target_post_id']) ? (int) $payload['target_post_id'] : 0,
                    'action_type' => (string) ($payload['action_type'] ?? ''),
                    'result' => $result,
                    'error_message' => $error_message,
                ],
                $context
            )
        );
    }

    private function restore_snapshot(int $post_id): void
    {
        $before_meta = get_post_meta($post_id, self::META_BEFORE, true);
        if (! is_array($before_meta)) {
            throw new RuntimeException('No stored before snapshot was found for this post.');
        }

        $snapshot = $before_meta['snapshot'] ?? null;
        if (is_array($snapshot) && ! empty($snapshot)) {
            $mode = (string) ($snapshot['mode'] ?? '');
            if ($mode === 'elementor') {
                $elementor_data = $snapshot['elementor_data'] ?? null;
                if (! is_array($elementor_data)) {
                    throw new RuntimeException('Stored Elementor snapshot is invalid.');
                }
                update_post_meta($post_id, '_elementor_data', wp_json_encode($elementor_data));
            } elseif ($mode === 'post_content') {
                $html = (string) ($snapshot['html'] ?? '');
                wp_update_post(
                    [
                        'ID' => $post_id,
                        'post_content' => $html,
                    ]
                );
            } else {
                throw new RuntimeException('Stored snapshot mode is not supported.');
            }
        }

        $seo_meta = $before_meta['seo_meta'] ?? null;
        if (is_array($seo_meta) && ! empty($seo_meta)) {
            $this->apply_seo_meta($post_id, $seo_meta);
        }

        clean_post_cache($post_id);
        if (class_exists('\Elementor\Plugin')) {
            \Elementor\Plugin::$instance->files_manager->clear_cache();
        }
    }
}
