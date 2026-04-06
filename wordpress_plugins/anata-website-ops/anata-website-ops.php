<?php
/**
 * Plugin Name: Anata Website Ops
 * Plugin URI: https://anatainc.com
 * Description: Narrow Website Ops execution bridge for signed FAQ insertion requests from agent.anatainc.com.
 * Version: 0.1.0
 * Author: Anata
 * Requires at least: 6.2
 * Requires PHP: 8.0
 */

if (! defined('ABSPATH')) {
    exit;
}

require_once __DIR__ . '/includes/class-anata-website-ops.php';

Anata_Website_Ops::instance();
