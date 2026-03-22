"""Tests for the web-based settings editor API (Plan 12, Phases 1-3)."""

import json
import os
import tempfile
import pytest
from unittest.mock import patch, MagicMock

from utils.settings_api import (
    get_env_schema,
    read_env_values,
    write_env_values,
    validate_env_values,
    _sanitize_value,
    _needs_quoting,
    _format_env_line,
    ENV_SCHEMA,
    _ALL_KEYS,
    get_plex_debrid_schema,
    read_plex_debrid_values,
    write_plex_debrid_values,
    validate_plex_debrid_values,
    _sync_plex_debrid_to_env,
    _sync_env_to_plex_debrid,
    _SETTINGS_JSON_TO_ENV,
    _ENV_TO_SETTINGS_JSON,
    PLEX_DEBRID_SCHEMA,
    _PD_ALL_KEYS,
    OAUTH_SERVICES,
    _OAUTH_FIELD_MAP,
    oauth_start,
    oauth_poll,
    export_env,
    export_plex_debrid,
    get_plex_debrid_defaults,
    get_env_defaults,
    VERSION_PRESETS,
    get_version_presets,
    get_version_editor_metadata,
)
from utils.settings_page import get_settings_html


# ---------------------------------------------------------------------------
# Env var schema tests
# ---------------------------------------------------------------------------

class TestEnvSchema:

    def test_schema_has_categories(self):
        schema = get_env_schema()
        assert 'categories' in schema
        assert len(schema['categories']) > 0

    def test_all_categories_have_required_keys(self):
        schema = get_env_schema()
        for cat in schema['categories']:
            assert 'name' in cat
            assert 'description' in cat
            assert 'fields' in cat
            assert len(cat['fields']) > 0

    def test_all_fields_have_required_keys(self):
        schema = get_env_schema()
        for cat in schema['categories']:
            for field in cat['fields']:
                assert 'key' in field
                assert 'label' in field
                assert 'type' in field
                assert 'required' in field
                assert 'help' in field
                assert 'sensitive' in field

    def test_field_types_are_valid(self):
        valid_prefixes = ('boolean', 'string', 'secret', 'url', 'number:', 'select:')
        schema = get_env_schema()
        for cat in schema['categories']:
            for field in cat['fields']:
                assert any(field['type'].startswith(p) or field['type'] == p.rstrip(':')
                           for p in valid_prefixes), \
                    f"Invalid type '{field['type']}' for {field['key']}"

    def test_sensitive_fields_are_marked(self):
        schema = get_env_schema()
        sensitive_keys = set()
        for cat in schema['categories']:
            for field in cat['fields']:
                if field['sensitive']:
                    sensitive_keys.add(field['key'])
        assert 'RD_API_KEY' in sensitive_keys
        assert 'PLEX_TOKEN' in sensitive_keys
        assert 'ZURG_PASS' in sensitive_keys

    def test_non_sensitive_fields(self):
        schema = get_env_schema()
        for cat in schema['categories']:
            for field in cat['fields']:
                if field['key'] == 'ZURG_ENABLED':
                    assert field['sensitive'] is False
                if field['key'] == 'RCLONE_MOUNT_NAME':
                    assert field['sensitive'] is False

    def test_all_keys_set_matches_schema(self):
        schema_keys = set()
        for cat in ENV_SCHEMA:
            for field in cat['fields']:
                schema_keys.add(field[0])
        assert _ALL_KEYS == schema_keys

    def test_schema_is_json_serializable(self):
        schema = get_env_schema()
        serialized = json.dumps(schema)
        assert len(serialized) > 100
        roundtrip = json.loads(serialized)
        assert roundtrip == schema


# ---------------------------------------------------------------------------
# Sanitization tests
# ---------------------------------------------------------------------------

class TestSanitizeValue:

    def test_strips_whitespace(self):
        assert _sanitize_value('  hello  ') == 'hello'

    def test_rejects_newlines(self):
        with pytest.raises(ValueError, match='newlines'):
            _sanitize_value('line1\nline2')

    def test_removes_null_bytes(self):
        assert _sanitize_value('abc\x00def') == 'abcdef'

    def test_removes_carriage_returns(self):
        assert _sanitize_value('abc\rdef') == 'abcdef'

    def test_none_becomes_empty(self):
        assert _sanitize_value(None) == ''

    def test_number_becomes_string(self):
        assert _sanitize_value(42) == '42'

    def test_normal_string_passthrough(self):
        assert _sanitize_value('my-api-key-123') == 'my-api-key-123'


class TestNeedsQuoting:

    def test_empty_string(self):
        assert _needs_quoting('') is False

    def test_simple_value(self):
        assert _needs_quoting('hello') is False

    def test_value_with_space(self):
        assert _needs_quoting('hello world') is True

    def test_value_with_hash(self):
        assert _needs_quoting('value#comment') is True

    def test_value_with_dollar(self):
        assert _needs_quoting('pass$word') is True

    def test_value_with_single_quote(self):
        assert _needs_quoting("it's") is True

    def test_value_with_double_quote(self):
        assert _needs_quoting('say "hi"') is True

    def test_value_with_backslash(self):
        assert _needs_quoting('path\\to') is True


class TestFormatEnvLine:

    def test_empty_value(self):
        assert _format_env_line('KEY', '') == 'KEY='

    def test_simple_value(self):
        assert _format_env_line('KEY', 'value') == 'KEY=value'

    def test_value_needing_quotes(self):
        assert _format_env_line('KEY', 'hello world') == 'KEY="hello world"'

    def test_value_with_dollar(self):
        assert _format_env_line('KEY', 'abc$def') == 'KEY="abc$def"'

    def test_value_with_double_quote(self):
        assert _format_env_line('KEY', 'say "hi"') == 'KEY="say \\"hi\\""'


# ---------------------------------------------------------------------------
# Env read / write tests
# ---------------------------------------------------------------------------

class TestReadEnvValues:

    def test_returns_all_schema_keys(self):
        with patch('utils.settings_api.ENV_FILE', '/nonexistent/.env'):
            values = read_env_values()
        for key in _ALL_KEYS:
            assert key in values

    def test_missing_file_returns_empty_values(self):
        with patch('utils.settings_api.ENV_FILE', '/nonexistent/.env'):
            values = read_env_values()
        assert all(v == '' for v in values.values())

    def test_reads_existing_file(self, tmp_path):
        env_file = tmp_path / '.env'
        env_file.write_text('ZURG_ENABLED=true\nRD_API_KEY=test123\n')
        with patch('utils.settings_api.ENV_FILE', str(env_file)):
            values = read_env_values()
        assert values['ZURG_ENABLED'] == 'true'
        assert values['RD_API_KEY'] == 'test123'
        assert values['AD_API_KEY'] == ''


class TestWriteEnvValues:

    def test_writes_valid_values(self, tmp_path):
        env_file = tmp_path / '.env'
        with patch('utils.settings_api.ENV_FILE', str(env_file)), \
             patch('os.kill') as mock_kill, \
             patch('utils.settings_api.read_env_values', return_value={k: '' for k in _ALL_KEYS}):
            result = write_env_values({
                'ZURG_ENABLED': 'true',
                'RD_API_KEY': 'test-key-123',
                'RCLONE_MOUNT_NAME': 'media',
            })
        assert result['status'] == 'saved'
        assert result['errors'] == []
        assert env_file.exists()
        content = env_file.read_text()
        assert 'ZURG_ENABLED=true' in content
        assert 'RD_API_KEY=test-key-123' in content

    def test_rejects_newlines_in_values(self, tmp_path):
        env_file = tmp_path / '.env'
        with patch('utils.settings_api.ENV_FILE', str(env_file)):
            result = write_env_values({
                'ZURG_ENABLED': 'true\nRD_API_KEY=hacked',
            })
        assert result['status'] == 'error'
        assert any('newlines' in e for e in result['errors'])

    def test_ignores_unknown_keys(self, tmp_path):
        env_file = tmp_path / '.env'
        with patch('utils.settings_api.ENV_FILE', str(env_file)), \
             patch('os.kill'), \
             patch('utils.settings_api.read_env_values', return_value={k: '' for k in _ALL_KEYS}):
            result = write_env_values({
                'ZURG_ENABLED': 'true',
                'RD_API_KEY': 'key123',
                'TOTALLY_UNKNOWN': 'injected',
            })
        assert result['status'] == 'saved'
        content = env_file.read_text()
        assert 'TOTALLY_UNKNOWN' not in content

    def test_validation_blocks_save(self, tmp_path):
        env_file = tmp_path / '.env'
        with patch('utils.settings_api.ENV_FILE', str(env_file)), \
             patch('utils.settings_api.read_env_values', return_value={k: '' for k in _ALL_KEYS}):
            result = write_env_values({
                'ZURG_ENABLED': 'true',
            })
        assert result['status'] == 'error'
        assert any('API key' in e for e in result['errors'])
        assert not env_file.exists()


# ---------------------------------------------------------------------------
# Env validation tests
# ---------------------------------------------------------------------------

class TestValidateEnvValues:

    def test_valid_config(self):
        result = validate_env_values({
            'ZURG_ENABLED': 'true',
            'RD_API_KEY': 'my-api-key',
            'RCLONE_MOUNT_NAME': 'media',
        })
        assert result['errors'] == []

    def test_zurg_enabled_no_key(self):
        result = validate_env_values({'ZURG_ENABLED': 'true'})
        assert any('API key' in e for e in result['errors'])

    def test_zurg_disabled_no_key_ok(self):
        result = validate_env_values({'ZURG_ENABLED': 'false'})
        assert result['errors'] == []

    def test_invalid_url(self):
        result = validate_env_values({'PLEX_ADDRESS': 'not-a-url'})
        assert any('PLEX_ADDRESS' in e for e in result['errors'])

    def test_valid_url(self):
        result = validate_env_values({'PLEX_ADDRESS': 'http://192.168.1.100:32400'})
        assert result['errors'] == []

    def test_invalid_blackhole_debrid(self):
        result = validate_env_values({'BLACKHOLE_DEBRID': 'invalid_service'})
        assert any('BLACKHOLE_DEBRID' in e for e in result['errors'])

    def test_valid_blackhole_debrid(self):
        result = validate_env_values({'BLACKHOLE_DEBRID': 'realdebrid'})
        assert result['errors'] == []

    def test_invalid_port(self):
        result = validate_env_values({'STATUS_UI_PORT': 'not_a_number'})
        assert any('STATUS_UI_PORT' in e for e in result['errors'])

    def test_port_out_of_range(self):
        result = validate_env_values({'STATUS_UI_PORT': '99999'})
        assert any('STATUS_UI_PORT' in w for w in result['warnings'])

    def test_invalid_notification_level(self):
        result = validate_env_values({'NOTIFICATION_LEVEL': 'critical'})
        assert any('NOTIFICATION_LEVEL' in e for e in result['errors'])

    def test_invalid_auth_format(self):
        result = validate_env_values({'STATUS_UI_AUTH': 'no-colon'})
        assert any('STATUS_UI_AUTH' in e for e in result['errors'])

    def test_valid_auth_format(self):
        result = validate_env_values({'STATUS_UI_AUTH': 'user:pass'})
        assert result['errors'] == []

    def test_duplicate_cleanup_without_plex_token(self):
        result = validate_env_values({'DUPLICATE_CLEANUP': 'true'})
        assert any('PLEX_TOKEN' in e for e in result['errors'])

    def test_pd_enabled_without_zurg_warns(self):
        result = validate_env_values({'PD_ENABLED': 'true', 'ZURG_ENABLED': 'false'})
        assert any('PD_ENABLED' in w for w in result['warnings'])

    def test_rclone_log_level_notice_ok(self):
        result = validate_env_values({'RCLONE_LOG_LEVEL': 'NOTICE'})
        assert result['warnings'] == []

    def test_zurg_log_level_notice_warns(self):
        result = validate_env_values({'ZURG_LOG_LEVEL': 'NOTICE'})
        assert any('ZURG_LOG_LEVEL' in w for w in result['warnings'])

    def test_mount_name_special_chars(self):
        result = validate_env_values({'RCLONE_MOUNT_NAME': 'my mount!'})
        assert any('RCLONE_MOUNT_NAME' in w for w in result['warnings'])

    def test_blackhole_enabled_no_debrid_key(self):
        result = validate_env_values({'BLACKHOLE_ENABLED': 'true'})
        assert any('debrid API key' in e for e in result['errors'])


# ===========================================================================
# plex_debrid schema tests
# ===========================================================================

class TestPlexDebridSchema:

    def test_schema_has_categories(self):
        schema = get_plex_debrid_schema()
        assert 'categories' in schema
        assert len(schema['categories']) == 5

    def test_category_names(self):
        schema = get_plex_debrid_schema()
        names = [c['name'] for c in schema['categories']]
        assert 'Content Services' in names
        assert 'Library Services' in names
        assert 'Scraper Settings' in names
        assert 'Debrid Services' in names
        assert 'UI Settings' in names

    def test_multiselect_fields_have_options(self):
        schema = get_plex_debrid_schema()
        for cat in schema['categories']:
            for field in cat['fields']:
                if field['type'] == 'multiselect':
                    assert 'options' in field, f"{field['key']} missing options"
                    assert len(field['options']) > 0

    def test_radio_fields_have_options(self):
        schema = get_plex_debrid_schema()
        for cat in schema['categories']:
            for field in cat['fields']:
                if field['type'] == 'radio':
                    assert 'options' in field, f"{field['key']} missing options"

    def test_all_fields_have_required_attributes(self):
        schema = get_plex_debrid_schema()
        for cat in schema['categories']:
            for field in cat['fields']:
                assert 'key' in field
                assert 'label' in field
                assert 'type' in field
                assert 'hidden' in field
                assert 'help' in field

    def test_schema_is_json_serializable(self):
        schema = get_plex_debrid_schema()
        serialized = json.dumps(schema)
        roundtrip = json.loads(serialized)
        assert roundtrip == schema

    def test_pd_all_keys_matches_schema(self):
        schema_keys = {field[0] for cat in PLEX_DEBRID_SCHEMA for field in cat['fields']}
        assert _PD_ALL_KEYS == schema_keys

    def test_content_services_options(self):
        schema = get_plex_debrid_schema()
        content_cat = schema['categories'][0]
        services_field = content_cat['fields'][0]
        assert services_field['key'] == 'Content Services'
        assert 'Plex' in services_field['options']
        assert 'Trakt' in services_field['options']

    def test_debrid_services_options(self):
        schema = get_plex_debrid_schema()
        debrid_cat = next(c for c in schema['categories'] if c['name'] == 'Debrid Services')
        services_field = debrid_cat['fields'][0]
        assert 'Real Debrid' in services_field['options']
        assert 'All Debrid' in services_field['options']
        assert 'Torbox' in services_field['options']

    def test_hidden_fields_exist(self):
        schema = get_plex_debrid_schema()
        hidden = []
        for cat in schema['categories']:
            for field in cat['fields']:
                if field.get('hidden'):
                    hidden.append(field['key'])
        assert 'Plex users' in hidden
        assert 'Overseerr API Key' in hidden
        assert 'Real Debrid API Key' in hidden


# ---------------------------------------------------------------------------
# plex_debrid read/write tests
# ---------------------------------------------------------------------------

class TestReadPlexDebridValues:

    def test_reads_settings_file(self, tmp_path):
        settings_file = tmp_path / 'settings.json'
        settings_file.write_text(json.dumps({
            'Content Services': ['Plex'],
            'Debug printing': 'false',
        }))
        with patch('utils.settings_api.SETTINGS_JSON_FILE', str(settings_file)):
            values = read_plex_debrid_values()
        assert values['Content Services'] == ['Plex']
        assert values['Debug printing'] == 'false'

    def test_falls_back_to_defaults(self, tmp_path):
        defaults_file = tmp_path / 'defaults.json'
        defaults_file.write_text(json.dumps({'Show Menu on Startup': 'true'}))
        with patch('utils.settings_api.SETTINGS_JSON_FILE', '/nonexistent/settings.json'), \
             patch('utils.settings_api.SETTINGS_DEFAULT_FILE', str(defaults_file)):
            values = read_plex_debrid_values()
        assert values['Show Menu on Startup'] == 'true'

    def test_returns_empty_on_no_files(self):
        with patch('utils.settings_api.SETTINGS_JSON_FILE', '/nonexistent/a'), \
             patch('utils.settings_api.SETTINGS_DEFAULT_FILE', '/nonexistent/b'):
            values = read_plex_debrid_values()
        assert values == {}


class TestWritePlexDebridValues:

    def test_writes_valid_settings(self, tmp_path):
        settings_file = tmp_path / 'settings.json'
        values = {
            'Content Services': ['Plex', 'Trakt'],
            'Debrid Services': ['Real Debrid'],
            'Show Menu on Startup': 'false',
        }
        with patch('utils.settings_api.SETTINGS_JSON_FILE', str(settings_file)), \
             patch('utils.processes.restart_service', create=True), \
             patch('utils.settings_api.write_plex_debrid_values') as mock:
            # Call actual implementation with mocked file path
            pass
        # Direct test without restart
        with patch('utils.settings_api.SETTINGS_JSON_FILE', str(settings_file)), \
             patch('threading.Thread') as mock_thread:
            result = write_plex_debrid_values(values)
        assert result['status'] == 'saved'
        assert settings_file.exists()
        written = json.loads(settings_file.read_text())
        assert written['Content Services'] == ['Plex', 'Trakt']

    def test_rejects_non_dict(self):
        result = write_plex_debrid_values(['not', 'a', 'dict'])
        assert result['status'] == 'error'

    def test_validation_errors_block_save(self, tmp_path):
        settings_file = tmp_path / 'settings.json'
        values = {
            'Content Services': 'not a list',  # Should be a list
        }
        with patch('utils.settings_api.SETTINGS_JSON_FILE', str(settings_file)):
            result = write_plex_debrid_values(values)
        assert result['status'] == 'error'
        assert not settings_file.exists()


# ---------------------------------------------------------------------------
# plex_debrid validation tests
# ---------------------------------------------------------------------------

class TestValidatePlexDebridValues:

    def test_valid_settings(self):
        result = validate_plex_debrid_values({
            'Content Services': ['Plex'],
            'Debrid Services': ['Real Debrid'],
            'Show Menu on Startup': 'true',
        })
        assert result['errors'] == []

    def test_multiselect_must_be_list(self):
        result = validate_plex_debrid_values({
            'Content Services': 'Plex',
        })
        assert any('Content Services' in e for e in result['errors'])

    def test_unknown_multiselect_option_warns(self):
        result = validate_plex_debrid_values({
            'Content Services': ['Plex', 'UnknownService'],
        })
        assert any('UnknownService' in w for w in result['warnings'])

    def test_radio_at_most_one(self):
        result = validate_plex_debrid_values({
            'Library collection service': ['Plex Library', 'Trakt Collection'],
        })
        assert any('at most one' in w for w in result['warnings'])

    def test_list_pairs_structure(self):
        result = validate_plex_debrid_values({
            'Plex users': [['user1', 'token1']],
        })
        assert result['errors'] == []

    def test_list_pairs_bad_structure(self):
        result = validate_plex_debrid_values({
            'Plex users': ['not a pair'],
        })
        assert any('Plex users' in e for e in result['errors'])

    def test_boolean_str_validates(self):
        result = validate_plex_debrid_values({
            'Show Menu on Startup': 'maybe',
        })
        assert any('Show Menu on Startup' in w for w in result['warnings'])

    def test_versions_must_be_list(self):
        result = validate_plex_debrid_values({
            'Versions': 'not a list',
        })
        assert any('Versions' in e for e in result['errors'])

    def test_non_dict_input(self):
        result = validate_plex_debrid_values([1, 2, 3])
        assert result['errors']

    def test_empty_dict_ok(self):
        result = validate_plex_debrid_values({})
        assert result['errors'] == []


# ---------------------------------------------------------------------------
# Settings page HTML tests
# ---------------------------------------------------------------------------

class TestSettingsPage:

    def _get_html(self):
        return get_settings_html(get_env_schema(), get_plex_debrid_schema())

    def test_returns_html(self):
        html = self._get_html()
        assert '<!DOCTYPE html>' in html
        assert 'pd_zurg Settings' in html

    def test_has_both_tabs(self):
        html = self._get_html()
        assert 'tab-env' in html
        assert 'tab-pd' in html
        assert 'pd_zurg' in html
        assert 'plex_debrid' in html

    def test_env_schema_embedded(self):
        html = self._get_html()
        assert 'ZURG_ENABLED' in html
        assert 'RD_API_KEY' in html

    def test_pd_schema_embedded(self):
        html = self._get_html()
        assert 'Content Services' in html
        assert 'Debrid Services' in html

    def test_html_contains_form_elements(self):
        html = self._get_html()
        assert 'Save' in html
        assert 'Validate' in html

    def test_dashboard_link(self):
        html = self._get_html()
        assert '/status' in html

    def test_no_placeholder_tokens(self):
        html = self._get_html()
        assert '__ENV_SCHEMA_JSON__' not in html
        assert '__PD_SCHEMA_JSON__' not in html

    def test_multiselect_renderable(self):
        """Ensure the HTML contains multiselect JS renderer."""
        html = self._get_html()
        assert 'multiselect' in html

    def test_list_pairs_renderable(self):
        html = self._get_html()
        assert 'list_pairs' in html

    def test_oauth_buttons_in_html(self):
        html = self._get_html()
        assert 'oauthConnect' in html

    def test_import_export_buttons(self):
        html = self._get_html()
        assert 'export/env' in html
        assert 'export/plex-debrid' in html
        assert 'pdImport' in html

    def test_reset_buttons(self):
        html = self._get_html()
        assert 'envResetDefaults' in html
        assert 'pdResetDefaults' in html


# ===========================================================================
# Phase 3: OAuth tests
# ===========================================================================

class TestOAuthConfig:

    def test_all_services_defined(self):
        assert 'trakt' in OAUTH_SERVICES
        assert 'debridlink' in OAUTH_SERVICES
        assert 'putio' in OAUTH_SERVICES
        assert 'orionoid' in OAUTH_SERVICES

    def test_services_have_required_fields(self):
        for key, svc in OAUTH_SERVICES.items():
            assert 'name' in svc
            assert 'verification_url' in svc
            assert 'interval' in svc
            assert 'settings_key' in svc

    def test_oauth_field_map_keys_exist_in_schema(self):
        pd_keys = {field[0] for cat in PLEX_DEBRID_SCHEMA for field in cat['fields']}
        for key in _OAUTH_FIELD_MAP:
            assert key in pd_keys, f"OAuth field '{key}' not in plex_debrid schema"

    def test_oauth_field_map_services_exist(self):
        for key, service in _OAUTH_FIELD_MAP.items():
            assert service in OAUTH_SERVICES, f"Service '{service}' not in OAUTH_SERVICES"

    def test_schema_has_oauth_attributes(self):
        schema = get_plex_debrid_schema()
        oauth_fields = []
        for cat in schema['categories']:
            for field in cat['fields']:
                if 'oauth' in field:
                    oauth_fields.append((field['key'], field['oauth']))
        assert len(oauth_fields) == 4
        oauth_keys = {k for k, v in oauth_fields}
        assert 'Trakt users' in oauth_keys
        assert 'Debrid Link API Key' in oauth_keys
        assert 'Put.io API Key' in oauth_keys
        assert 'Orionoid API Key' in oauth_keys


class TestOAuthStart:

    def test_unknown_service(self):
        result = oauth_start('nonexistent')
        assert 'error' in result
        assert 'Unknown' in result['error']

    def test_trakt_missing_client_id(self):
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop('TRAKT_CLIENT_ID', None)
            result = oauth_start('trakt')
        assert 'error' in result
        assert 'TRAKT_CLIENT_ID' in result['error']

    def test_trakt_api_failure(self):
        import requests as _req
        with patch.dict(os.environ, {'TRAKT_CLIENT_ID': 'test-id'}):
            with patch.object(_req, 'post', side_effect=_req.RequestException('Connection refused')):
                result = oauth_start('trakt')
        assert 'error' in result

    def test_debridlink_success(self):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json.return_value = {
            'value': {'device_code': 'dev123', 'user_code': 'USR456'}
        }
        with patch('requests.post', return_value=mock_resp):
            result = oauth_start('debridlink')
        assert 'error' not in result
        assert result['user_code'] == 'USR456'
        assert result['device_code'] == 'dev123'
        assert result['verification_url'] == 'https://debrid-link.fr/device'

    def test_putio_success(self):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json.return_value = {'code': 'PUT789'}
        with patch('requests.get', return_value=mock_resp):
            result = oauth_start('putio')
        assert result['user_code'] == 'PUT789'
        assert result['device_code'] == 'PUT789'

    def test_orionoid_success(self):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json.return_value = {'data': {'code': 'ORI999'}}
        with patch('requests.get', return_value=mock_resp):
            result = oauth_start('orionoid')
        assert result['user_code'] == 'ORI999'


class TestOAuthPoll:

    def test_unknown_service(self):
        result = oauth_poll('nonexistent', 'code')
        assert 'error' in result

    def test_trakt_pending(self):
        mock_resp = MagicMock()
        mock_resp.status_code = 400
        with patch.dict(os.environ, {'TRAKT_CLIENT_ID': 'id', 'TRAKT_CLIENT_SECRET': 'sec'}):
            with patch('requests.post', return_value=mock_resp):
                result = oauth_poll('trakt', 'dev123')
        assert result['status'] == 'pending'

    def test_trakt_complete(self):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json.return_value = {'access_token': 'tok_abc'}
        with patch.dict(os.environ, {'TRAKT_CLIENT_ID': 'id', 'TRAKT_CLIENT_SECRET': 'sec'}):
            with patch('requests.post', return_value=mock_resp):
                result = oauth_poll('trakt', 'dev123')
        assert result['status'] == 'complete'
        assert result['token'] == 'tok_abc'

    def test_debridlink_pending(self):
        mock_resp = MagicMock()
        mock_resp.status_code = 400
        mock_resp.headers = {'content-type': 'application/json'}
        mock_resp.json.return_value = {'error': 'authorization_pending'}
        with patch('requests.post', return_value=mock_resp):
            result = oauth_poll('debridlink', 'dev123')
        assert result['status'] == 'pending'

    def test_debridlink_complete(self):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json.return_value = {'value': {'access_token': 'dl_tok'}}
        with patch('requests.post', return_value=mock_resp):
            result = oauth_poll('debridlink', 'dev123')
        assert result['status'] == 'complete'
        assert result['token'] == 'dl_tok'

    def test_putio_pending(self):
        mock_resp = MagicMock()
        mock_resp.status_code = 400
        with patch('requests.get', return_value=mock_resp):
            result = oauth_poll('putio', 'code123')
        assert result['status'] == 'pending'

    def test_putio_complete(self):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json.return_value = {'oauth_token': 'putio_tok'}
        with patch('requests.get', return_value=mock_resp):
            result = oauth_poll('putio', 'code123')
        assert result['status'] == 'complete'
        assert result['token'] == 'putio_tok'

    def test_orionoid_pending(self):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json.return_value = {'data': {}}
        with patch('requests.get', return_value=mock_resp):
            result = oauth_poll('orionoid', 'code123')
        assert result['status'] == 'pending'

    def test_orionoid_complete(self):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json.return_value = {'data': {'token': 'ori_tok'}}
        with patch('requests.get', return_value=mock_resp):
            result = oauth_poll('orionoid', 'code123')
        assert result['status'] == 'complete'
        assert result['token'] == 'ori_tok'


# ===========================================================================
# Phase 3: Import / Export / Reset tests
# ===========================================================================

class TestExport:

    def test_export_env(self, tmp_path):
        env_file = tmp_path / '.env'
        env_file.write_text('ZURG_ENABLED=true\nRD_API_KEY=key\n')
        with patch('utils.settings_api.ENV_FILE', str(env_file)):
            content = export_env()
        assert 'ZURG_ENABLED=true' in content

    def test_export_env_missing_file(self):
        with patch('utils.settings_api.ENV_FILE', '/nonexistent/.env'):
            content = export_env()
        assert content == ''

    def test_export_plex_debrid(self, tmp_path):
        settings_file = tmp_path / 'settings.json'
        settings_file.write_text('{"key":"value"}')
        with patch('utils.settings_api.SETTINGS_JSON_FILE', str(settings_file)):
            content = export_plex_debrid()
        assert '"key"' in content

    def test_export_plex_debrid_missing_file(self):
        with patch('utils.settings_api.SETTINGS_JSON_FILE', '/nonexistent/x'):
            content = export_plex_debrid()
        assert content == '{}'


class TestReset:

    def test_env_defaults_returns_all_keys(self):
        defaults = get_env_defaults()
        assert len(defaults) == len(_ALL_KEYS)
        assert all(v == '' for v in defaults.values())

    def test_plex_debrid_defaults_from_file(self, tmp_path):
        defaults_file = tmp_path / 'defaults.json'
        defaults_file.write_text(json.dumps({'Show Menu on Startup': 'true'}))
        with patch('utils.settings_api.SETTINGS_DEFAULT_FILE', str(defaults_file)):
            defaults = get_plex_debrid_defaults()
        assert defaults['Show Menu on Startup'] == 'true'

    def test_plex_debrid_defaults_missing_file(self):
        with patch('utils.settings_api.SETTINGS_DEFAULT_FILE', '/nonexistent/x'):
            defaults = get_plex_debrid_defaults()
        assert defaults == {}


# ===========================================================================
# Quality profile presets and editor metadata
# ===========================================================================

class TestVersionPresets:

    def test_presets_exist(self):
        assert len(VERSION_PRESETS) >= 5

    def test_preset_keys(self):
        assert '1080p_sdr' in VERSION_PRESETS
        assert '4k_hdr' in VERSION_PRESETS
        assert '720p' in VERSION_PRESETS
        assert 'any_quality' in VERSION_PRESETS
        assert 'anime' in VERSION_PRESETS

    def test_preset_structure(self):
        for key, preset in VERSION_PRESETS.items():
            assert 'name' in preset, f'{key} missing name'
            assert 'description' in preset, f'{key} missing description'
            assert 'profile' in preset, f'{key} missing profile'
            profile = preset['profile']
            assert isinstance(profile, list), f'{key} profile not a list'
            assert len(profile) == 4, f'{key} profile should have 4 elements'
            assert isinstance(profile[0], str), f'{key} profile[0] should be name string'
            assert isinstance(profile[1], list), f'{key} profile[1] should be conditions list'
            assert isinstance(profile[2], str), f'{key} profile[2] should be a string (language code)'
            assert isinstance(profile[3], list), f'{key} profile[3] should be rules list'

    def test_preset_rules_structure(self):
        for key, preset in VERSION_PRESETS.items():
            for i, rule in enumerate(preset['profile'][3]):
                assert isinstance(rule, list), f'{key} rule {i} not a list'
                assert len(rule) == 4, f'{key} rule {i} should have 4 elements'

    def test_get_version_presets_serializable(self):
        presets = get_version_presets()
        serialized = json.dumps(presets)
        roundtrip = json.loads(serialized)
        assert len(roundtrip) == len(VERSION_PRESETS)

    def test_all_presets_have_cache_rule(self):
        """Every preset should require cached releases."""
        for key, preset in VERSION_PRESETS.items():
            rules = preset['profile'][3]
            has_cache = any(r[0] == 'cache status' and r[2] == 'cached' for r in rules)
            assert has_cache, f'{key} missing cache requirement'


class TestVersionEditorMetadata:

    def test_metadata_structure(self):
        meta = get_version_editor_metadata()
        assert 'rule_fields' in meta
        assert 'rule_weights' in meta
        assert 'condition_fields' in meta

    def test_rule_fields(self):
        meta = get_version_editor_metadata()
        assert 'resolution' in meta['rule_fields']
        assert 'cache status' in meta['rule_fields']
        assert 'title' in meta['rule_fields']
        assert 'size' in meta['rule_fields']

    def test_rule_weights(self):
        meta = get_version_editor_metadata()
        assert 'requirement' in meta['rule_weights']
        assert 'preference' in meta['rule_weights']

    def test_schema_includes_presets(self):
        schema = get_plex_debrid_schema()
        assert 'version_presets' in schema
        assert '1080p_sdr' in schema['version_presets']

    def test_schema_includes_editor_metadata(self):
        schema = get_plex_debrid_schema()
        assert 'version_editor' in schema
        assert 'rule_fields' in schema['version_editor']


class TestVersionsInSettingsPage:

    def test_html_contains_preset_cards(self):
        html = get_settings_html(get_env_schema(), get_plex_debrid_schema())
        assert 'preset-card' in html
        assert 'preset-grid' in html

    def test_html_contains_profile_editor(self):
        html = get_settings_html(get_env_schema(), get_plex_debrid_schema())
        assert 'profile-list' in html
        assert 'renderVersionsEditor' in html

    def test_html_contains_json_fallback(self):
        html = get_settings_html(get_env_schema(), get_plex_debrid_schema())
        assert 'Edit as JSON' in html
        assert 'versions-json-textarea' in html


# ---------------------------------------------------------------------------
# Bidirectional sync: settings.json → .env
# ---------------------------------------------------------------------------

class TestSyncPlexDebridToEnv:
    """Tests for _sync_plex_debrid_to_env — ensures plex_debrid settings
    are written back to .env so pd_setup() doesn't overwrite them on restart."""

    def _make_env(self, tmp_path, content=''):
        env_file = tmp_path / '.env'
        env_file.write_text(content)
        return str(env_file)

    def test_syncs_overseerr_to_env(self, tmp_path):
        env_file = self._make_env(tmp_path, 'SEERR_ADDRESS=http://old:5055\nSEERR_API_KEY=oldkey\n')
        values = {
            'Overseerr Base URL': 'http://new:5055',
            'Overseerr API Key': 'newkey',
        }
        with patch('utils.settings_api.ENV_FILE', env_file), \
             patch.dict(os.environ, {'SEERR_ADDRESS': 'http://old:5055', 'SEERR_API_KEY': 'oldkey'}):
            _sync_plex_debrid_to_env(values)

        from dotenv import dotenv_values
        written = dotenv_values(env_file)
        assert written['SEERR_ADDRESS'] == 'http://new:5055'
        assert written['SEERR_API_KEY'] == 'newkey'

    def test_syncs_all_simple_mappings(self, tmp_path):
        env_file = self._make_env(tmp_path, '')
        values = {
            'Overseerr Base URL': 'http://seerr:5055',
            'Overseerr API Key': 'seerrkey',
            'Plex server address': 'http://plex:32400',
            'Jellyfin API Key': 'jfkey',
            'Jellyfin server address': 'http://jf:8096',
            'Real Debrid API Key': 'rdkey',
            'All Debrid API Key': 'adkey',
            'Show Menu on Startup': 'false',
            'Log to file': 'true',
        }
        with patch('utils.settings_api.ENV_FILE', env_file), \
             patch.dict(os.environ, {}, clear=False):
            _sync_plex_debrid_to_env(values)

        from dotenv import dotenv_values
        written = dotenv_values(env_file)
        assert written.get('SEERR_ADDRESS') == 'http://seerr:5055'
        assert written.get('SEERR_API_KEY') == 'seerrkey'
        assert written.get('PLEX_ADDRESS') == 'http://plex:32400'
        assert written.get('JF_API_KEY') == 'jfkey'
        assert written.get('JF_ADDRESS') == 'http://jf:8096'
        assert written.get('RD_API_KEY') == 'rdkey'
        assert written.get('AD_API_KEY') == 'adkey'
        assert written.get('SHOW_MENU') == 'false'
        assert written.get('PD_LOGFILE') == 'true'

    def test_syncs_plex_users_first_pair(self, tmp_path):
        env_file = self._make_env(tmp_path, '')
        values = {
            'Plex users': [['myuser', 'mytoken'], ['other', 'othertoken']],
        }
        with patch('utils.settings_api.ENV_FILE', env_file), \
             patch.dict(os.environ, {}, clear=False):
            _sync_plex_debrid_to_env(values)

        from dotenv import dotenv_values
        written = dotenv_values(env_file)
        assert written.get('PLEX_USER') == 'myuser'
        assert written.get('PLEX_TOKEN') == 'mytoken'

    def test_syncs_debug_printing_true_to_debug(self, tmp_path):
        env_file = self._make_env(tmp_path, 'PD_LOG_LEVEL=INFO\n')
        values = {'Debug printing': 'true'}
        with patch('utils.settings_api.ENV_FILE', env_file), \
             patch.dict(os.environ, {'PD_LOG_LEVEL': 'INFO'}):
            _sync_plex_debrid_to_env(values)

        from dotenv import dotenv_values
        written = dotenv_values(env_file)
        assert written.get('PD_LOG_LEVEL') == 'DEBUG'

    def test_syncs_debug_printing_false_downgrades_from_debug(self, tmp_path):
        env_file = self._make_env(tmp_path, 'PD_LOG_LEVEL=DEBUG\n')
        values = {'Debug printing': 'false'}
        with patch('utils.settings_api.ENV_FILE', env_file), \
             patch.dict(os.environ, {'PD_LOG_LEVEL': 'DEBUG'}):
            _sync_plex_debrid_to_env(values)

        from dotenv import dotenv_values
        written = dotenv_values(env_file)
        assert written.get('PD_LOG_LEVEL') == 'INFO'

    def test_debug_printing_false_preserves_non_debug_level(self, tmp_path):
        """Turning off debug shouldn't overwrite WARNING/ERROR levels."""
        env_file = self._make_env(tmp_path, 'PD_LOG_LEVEL=WARNING\n')
        values = {'Debug printing': 'false'}
        with patch('utils.settings_api.ENV_FILE', env_file), \
             patch.dict(os.environ, {'PD_LOG_LEVEL': 'WARNING'}):
            _sync_plex_debrid_to_env(values)

        from dotenv import dotenv_values
        written = dotenv_values(env_file)
        assert written.get('PD_LOG_LEVEL') == 'WARNING'

    def test_no_write_when_values_unchanged(self, tmp_path):
        env_file = self._make_env(tmp_path, 'SEERR_ADDRESS=http://same:5055\n')
        values = {'Overseerr Base URL': 'http://same:5055'}
        with patch('utils.settings_api.ENV_FILE', env_file), \
             patch.dict(os.environ, {'SEERR_ADDRESS': 'http://same:5055'}), \
             patch('utils.settings_api.atomic_write') as mock_write:
            _sync_plex_debrid_to_env(values)
        mock_write.assert_not_called()

    def test_updates_os_environ(self, tmp_path):
        env_file = self._make_env(tmp_path, 'SEERR_ADDRESS=http://old:5055\n')
        values = {'Overseerr Base URL': 'http://new:5055'}
        with patch('utils.settings_api.ENV_FILE', env_file), \
             patch.dict(os.environ, {'SEERR_ADDRESS': 'http://old:5055'}):
            _sync_plex_debrid_to_env(values)
            assert os.environ['SEERR_ADDRESS'] == 'http://new:5055'

    def test_boolean_values_lowercased(self, tmp_path):
        """Python bool True/False should become 'true'/'false' in .env."""
        env_file = self._make_env(tmp_path, '')
        values = {'Show Menu on Startup': True}
        with patch('utils.settings_api.ENV_FILE', env_file), \
             patch.dict(os.environ, {}, clear=False):
            _sync_plex_debrid_to_env(values)

        from dotenv import dotenv_values
        written = dotenv_values(env_file)
        assert written.get('SHOW_MENU') == 'true'

    def test_write_plex_debrid_triggers_sync(self, tmp_path):
        """write_plex_debrid_values() should call _sync_plex_debrid_to_env."""
        settings_file = tmp_path / 'settings.json'
        values = {'Overseerr Base URL': 'http://test:5055'}
        with patch('utils.settings_api.SETTINGS_JSON_FILE', str(settings_file)), \
             patch('utils.settings_api._sync_plex_debrid_to_env') as mock_sync, \
             patch('threading.Thread'):
            write_plex_debrid_values(values)
        mock_sync.assert_called_once_with(values)


# ---------------------------------------------------------------------------
# Reverse sync: .env → settings.json
# ---------------------------------------------------------------------------

class TestSyncEnvToPlexDebrid:
    """Tests for _sync_env_to_plex_debrid — ensures .env changes are written
    into settings.json so plex_debrid picks them up on SIGHUP restart."""

    def _make_settings(self, tmp_path, data):
        settings_file = tmp_path / 'settings.json'
        settings_file.write_text(json.dumps(data, indent=4))
        return str(settings_file)

    def test_syncs_overseerr_to_settings_json(self, tmp_path):
        sf = self._make_settings(tmp_path, {
            'Overseerr Base URL': 'http://old:5055',
            'Overseerr API Key': 'oldkey',
        })
        env_values = {'SEERR_ADDRESS': 'http://new:5055', 'SEERR_API_KEY': 'newkey'}
        with patch('utils.settings_api.SETTINGS_JSON_FILE', sf):
            _sync_env_to_plex_debrid(env_values)
        written = json.loads(open(sf).read())
        assert written['Overseerr Base URL'] == 'http://new:5055'
        assert written['Overseerr API Key'] == 'newkey'

    def test_syncs_all_simple_mappings_plex_mode(self, tmp_path):
        sf = self._make_settings(tmp_path, {})
        env_values = {
            'SEERR_ADDRESS': 'http://seerr:5055',
            'SEERR_API_KEY': 'seerrkey',
            'PLEX_ADDRESS': 'http://plex:32400',
            'PLEX_USER': 'testuser',
            'PLEX_TOKEN': 'testtoken',
            'RD_API_KEY': 'rdkey',
            'AD_API_KEY': 'adkey',
            'SHOW_MENU': 'false',
            'PD_LOGFILE': 'true',
        }
        with patch('utils.settings_api.SETTINGS_JSON_FILE', sf):
            _sync_env_to_plex_debrid(env_values)
        written = json.loads(open(sf).read())
        assert written['Overseerr Base URL'] == 'http://seerr:5055'
        assert written['Overseerr API Key'] == 'seerrkey'
        assert written['Plex server address'] == 'http://plex:32400'
        assert written['Real Debrid API Key'] == 'rdkey'
        assert written['All Debrid API Key'] == 'adkey'
        assert written['Show Menu on Startup'] == 'false'
        assert written['Log to file'] == 'true'

    def test_syncs_all_simple_mappings_jellyfin_mode(self, tmp_path):
        sf = self._make_settings(tmp_path, {})
        env_values = {
            'SEERR_ADDRESS': 'http://seerr:5055',
            'SEERR_API_KEY': 'seerrkey',
            'JF_API_KEY': 'jfkey',
            'JF_ADDRESS': 'http://jf:8096',
            'RD_API_KEY': 'rdkey',
            'SHOW_MENU': 'false',
        }
        with patch('utils.settings_api.SETTINGS_JSON_FILE', sf):
            _sync_env_to_plex_debrid(env_values)
        written = json.loads(open(sf).read())
        assert written['Jellyfin API Key'] == 'jfkey'
        assert written['Jellyfin server address'] == 'http://jf:8096'

    def test_syncs_plex_user_token(self, tmp_path):
        sf = self._make_settings(tmp_path, {'Plex users': []})
        env_values = {'PLEX_USER': 'myuser', 'PLEX_TOKEN': 'mytoken'}
        with patch('utils.settings_api.SETTINGS_JSON_FILE', sf):
            _sync_env_to_plex_debrid(env_values)
        written = json.loads(open(sf).read())
        assert ['myuser', 'mytoken'] in written['Plex users']

    def test_updates_existing_first_plex_user(self, tmp_path):
        sf = self._make_settings(tmp_path, {
            'Plex users': [['olduser', 'oldtoken'], ['other', 'othertoken']],
        })
        env_values = {'PLEX_USER': 'newuser', 'PLEX_TOKEN': 'newtoken'}
        with patch('utils.settings_api.SETTINGS_JSON_FILE', sf):
            _sync_env_to_plex_debrid(env_values)
        written = json.loads(open(sf).read())
        assert written['Plex users'][0] == ['newuser', 'newtoken']
        assert written['Plex users'][1] == ['other', 'othertoken']

    def test_syncs_debug_level(self, tmp_path):
        sf = self._make_settings(tmp_path, {'Debug printing': 'false'})
        env_values = {'PD_LOG_LEVEL': 'DEBUG'}
        with patch('utils.settings_api.SETTINGS_JSON_FILE', sf):
            _sync_env_to_plex_debrid(env_values)
        written = json.loads(open(sf).read())
        assert written['Debug printing'] == 'true'

    def test_no_write_when_unchanged(self, tmp_path):
        sf = self._make_settings(tmp_path, {'Overseerr Base URL': 'http://same:5055'})
        env_values = {'SEERR_ADDRESS': 'http://same:5055'}
        with patch('utils.settings_api.SETTINGS_JSON_FILE', sf), \
             patch('utils.settings_api.atomic_write') as mock_write:
            _sync_env_to_plex_debrid(env_values)
        mock_write.assert_not_called()

    def test_no_crash_when_settings_file_missing(self, tmp_path):
        sf = str(tmp_path / 'nonexistent.json')
        env_values = {'SEERR_ADDRESS': 'http://new:5055'}
        with patch('utils.settings_api.SETTINGS_JSON_FILE', sf):
            _sync_env_to_plex_debrid(env_values)  # Should not raise

    def test_preserves_unrelated_keys(self, tmp_path):
        sf = self._make_settings(tmp_path, {
            'Overseerr Base URL': 'http://old:5055',
            'Content Services': ['Plex'],
            'Trakt lists': ['my-list'],
        })
        env_values = {'SEERR_ADDRESS': 'http://new:5055'}
        with patch('utils.settings_api.SETTINGS_JSON_FILE', sf):
            _sync_env_to_plex_debrid(env_values)
        written = json.loads(open(sf).read())
        assert written['Overseerr Base URL'] == 'http://new:5055'
        assert written['Content Services'] == ['Plex']
        assert written['Trakt lists'] == ['my-list']

    def test_write_env_values_triggers_sync(self, tmp_path):
        """write_env_values() should call _sync_env_to_plex_debrid."""
        env_file = tmp_path / '.env'
        env_file.write_text('')
        values = {'SEERR_ADDRESS': 'http://test:5055'}
        with patch('utils.settings_api.ENV_FILE', str(env_file)), \
             patch('utils.settings_api._sync_env_to_plex_debrid') as mock_sync, \
             patch('os.kill'):
            write_env_values(values)
        mock_sync.assert_called_once()

    def test_rebuilds_debrid_services_on_key_add(self, tmp_path):
        sf = self._make_settings(tmp_path, {'Debrid Services': []})
        env_values = {'RD_API_KEY': 'rdkey', 'AD_API_KEY': 'adkey'}
        with patch('utils.settings_api.SETTINGS_JSON_FILE', sf):
            _sync_env_to_plex_debrid(env_values)
        written = json.loads(open(sf).read())
        assert 'Real Debrid' in written['Debrid Services']
        assert 'All Debrid' in written['Debrid Services']

    def test_rebuilds_debrid_services_on_key_remove(self, tmp_path):
        sf = self._make_settings(tmp_path, {
            'Debrid Services': ['Real Debrid', 'All Debrid'],
            'Real Debrid API Key': 'rdkey',
        })
        env_values = {'RD_API_KEY': 'rdkey', 'AD_API_KEY': ''}
        with patch('utils.settings_api.SETTINGS_JSON_FILE', sf):
            _sync_env_to_plex_debrid(env_values)
        written = json.loads(open(sf).read())
        assert 'Real Debrid' in written['Debrid Services']
        assert 'All Debrid' not in written['Debrid Services']

    def test_jellyfin_mode_clears_plex(self, tmp_path):
        sf = self._make_settings(tmp_path, {
            'Plex users': [['user', 'token']],
            'Plex server address': 'http://plex:32400',
        })
        env_values = {'JF_API_KEY': 'jfkey', 'JF_ADDRESS': 'http://jf:8096'}
        with patch('utils.settings_api.SETTINGS_JSON_FILE', sf):
            _sync_env_to_plex_debrid(env_values)
        written = json.loads(open(sf).read())
        assert written['Plex users'] == []
        assert written['Plex server address'] == 'http://localhost:32400'
        assert written['Jellyfin API Key'] == 'jfkey'

    def test_plex_mode_clears_jellyfin(self, tmp_path):
        sf = self._make_settings(tmp_path, {
            'Jellyfin API Key': 'jfkey',
            'Jellyfin server address': 'http://jf:8096',
        })
        env_values = {'PLEX_USER': 'user', 'PLEX_TOKEN': 'token', 'PLEX_ADDRESS': 'http://plex:32400'}
        with patch('utils.settings_api.SETTINGS_JSON_FILE', sf):
            _sync_env_to_plex_debrid(env_values)
        written = json.loads(open(sf).read())
        assert written['Jellyfin API Key'] == ''
        assert written['Plex server address'] == 'http://plex:32400'
