"""Offline checks that diagnostic output does not disclose sensitive fields."""

import base64
import contextlib
import hashlib
import io
import json
import os
from pathlib import Path
import runpy
import unittest
from unittest.mock import patch


SCRIPTS = Path(__file__).parent
TOKEN = 'PRIVATE_STATE_CANARY'
PREVIEW = 'PRIVATE_PREVIEW_CANARY'
KEY = 'PRIVATE_MANAGEMENT_CANARY'
PROXY = 'socks5://PRIVATE_USER:PRIVATE_PASSWORD@private-host.invalid:1080'
ERROR = 'PRIVATE_UPSTREAM_BODY_CANARY'
PAYLOAD = {
    'version': '1.5.30', 'total': 1, 'mismatches': 0,
    'records': [{
        'time': '2026-01-01T00:00:00Z', 'request_id': 'test-request',
        'model': 'gpt-6-astra', 'upstream_model': 'gpt-6-astra',
        'turn_state_value': TOKEN, 'turn_state_preview': PREVIEW,
        'turn_state_unknown_field': TOKEN, 'turn_state_length': len(TOKEN),
        'turn_state_source': 'response_header', 'turn_state_truncated': False,
    }],
    'turn_state_override': {
        'enabled': True, 'value': TOKEN, 'value_preview': PREVIEW,
        'probe': {
            'enabled': True, 'running': False, 'last_error': ERROR,
            'values': [{'model': 'gpt-6-astra', 'value': TOKEN, 'proxy': PROXY}],
            'history': [{'model': 'gpt-6-astra', 'proxy': PROXY, 'error': ERROR}],
        },
    },
}


class DiagnosticTests(unittest.TestCase):
    def run_diagnostic(self, name):
        output = io.StringIO()
        response = io.BytesIO(json.dumps(PAYLOAD).encode('utf-8'))
        with patch.dict(os.environ, {'CPA_MANAGEMENT_KEY': KEY}, clear=True):
            with patch('urllib.request.urlopen', return_value=response) as request:
                with contextlib.redirect_stdout(output):
                    runpy.run_path(str(SCRIPTS / name), run_name='__main__')
        request.assert_called_once()
        self.assertEqual(request.call_args.args[0].full_url,
                         'http://127.0.0.1:8317/v0/management/oai-adversarial-plugin/requests')
        self.assertEqual(request.call_args.args[0].get_header('Authorization'), 'Bearer ' + KEY)
        text = output.getvalue()
        for secret in (TOKEN, PREVIEW, KEY, PROXY, ERROR, 'PRIVATE_PASSWORD'):
            self.assertNotIn(secret, text)
        self.assertIn('1.5.30', text)
        return text

    def test_dump_exposes_metadata_only(self):
        text = self.run_diagnostic('dump_turn_state.py')
        self.assertIn('turn_state_present: True', text)
        self.assertIn('turn_state_length:', text)

    def test_fetch_uses_fingerprint(self):
        text = self.run_diagnostic('fetch_record.py')
        self.assertIn(hashlib.sha256(TOKEN.encode('utf-8')).hexdigest(), text)
        self.assertNotIn('turn_state_unknown_field', text)

    def test_probe_hides_proxy_and_error_body(self):
        text = self.run_diagnostic('probe_status.py')
        self.assertIn('has_last_error: True', text)
        self.assertIn('has_error=True', text)

    def test_missing_key_makes_no_request(self):
        for name in ('dump_turn_state.py', 'fetch_record.py', 'probe_status.py'):
            with self.subTest(script=name), patch.dict(os.environ, {}, clear=True):
                with patch('urllib.request.urlopen') as request:
                    with self.assertRaisesRegex(SystemExit, 'Set CPA_MANAGEMENT_KEY'):
                        runpy.run_path(str(SCRIPTS / name), run_name='__main__')
                request.assert_not_called()

    def test_analysis_samples_are_generated(self):
        with contextlib.redirect_stdout(io.StringIO()):
            result = runpy.run_path(str(SCRIPTS / 'analyze_turn_state.py'))
        for seed, value in enumerate(result['VALUES'].values()):
            raw = base64.urlsafe_b64decode(value)
            self.assertEqual(len(value), 292)
            self.assertEqual(raw[0], 0x80)
            self.assertEqual(raw[9:], bytes((index + seed) % 256 for index in range(208)))


if __name__ == '__main__':
    unittest.main()
