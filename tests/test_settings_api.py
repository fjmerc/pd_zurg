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
