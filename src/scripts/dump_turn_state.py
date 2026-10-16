"""Show turn-state metadata without exporting token values or previews."""

import json
import os
import urllib.request

key = os.environ.get('CPA_MANAGEMENT_KEY')
if not key:
    raise SystemExit('Set CPA_MANAGEMENT_KEY in the environment before running this script.')

request = urllib.request.Request('http://127.0.0.1:8317/v0/management/oai-adversarial-plugin/requests',
                                 headers={'Authorization': 'Bearer ' + key})
with urllib.request.urlopen(request, timeout=8) as response:
    payload = json.loads(response.read())

print('version:', payload.get('version'), '| total:', payload.get('total'),
      '| mismatches:', payload.get('mismatches'))
for record in payload.get('records', []):
    print('----')
    print('time:', record.get('time'))
    print('request_id:', record.get('request_id'))
    print('model:', record.get('model'),
          '| upstream:', record.get('upstream_model') or '(none)',
          '| mismatch:', record.get('model_mismatch'))
    print('turn_state_length:', record.get('turn_state_length'))
    print('turn_state_source:', record.get('turn_state_source') or '(none)')
    print('turn_state_present:', bool(record.get('turn_state_value')))
    if record.get('turn_state_truncated'):
        print('turn_state_truncated: True')
