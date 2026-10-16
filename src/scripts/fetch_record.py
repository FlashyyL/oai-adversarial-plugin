"""Show audit metadata and token fingerprints without exporting token values."""

import json
import hashlib
import os
import urllib.request

key = os.environ.get('CPA_MANAGEMENT_KEY')
if not key:
    raise SystemExit('Set CPA_MANAGEMENT_KEY in the environment before running this script.')

api = urllib.request.Request('http://127.0.0.1:8317/v0/management/oai-adversarial-plugin/requests',
                             headers={'Authorization': 'Bearer ' + key})
with urllib.request.urlopen(api, timeout=8) as response:
    payload = json.loads(response.read())

records = payload.get('records', [])
print('plugin version:', payload.get('version'), '| total:', payload.get('total'),
      '| records kept:', len(records))
print()
for index, record in enumerate(records):
    print('== record', index, '| time:', record.get('time'),
          '| model:', record.get('model'), '| upstream:', record.get('upstream_model'),
          '| mismatch:', record.get('model_mismatch'),
          '| length:', record.get('turn_state_length'),
          '| source:', record.get('turn_state_source'))
    value = record.get('turn_state_value') or ''
    if value:
        print('   turn_state_sha256:', hashlib.sha256(value.encode('utf-8')).hexdigest())
    print('   turn_state_truncated:', bool(record.get('turn_state_truncated')))
    print()
