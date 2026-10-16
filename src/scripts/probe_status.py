"""Show probe metadata without tokens, proxy addresses or upstream error bodies."""

import json
import os
import urllib.request

key = os.environ.get('CPA_MANAGEMENT_KEY')
if not key:
    raise SystemExit('Set CPA_MANAGEMENT_KEY in the environment before running this script.')

request = urllib.request.Request('http://127.0.0.1:8317/v0/management/oai-adversarial-plugin/requests',
                                 headers={'Authorization': 'Bearer ' + key})
with urllib.request.urlopen(request, timeout=8) as response:
    data = json.loads(response.read())

print('plugin version:', data.get('version'))
probe = (data.get('turn_state_override') or {}).get('probe') or {}
print('probe enabled:', probe.get('enabled'), '| running:', probe.get('running'),
      '| total:', probe.get('probes_total'), '| ok:', probe.get('probes_ok'))
print('has_last_error:', bool(probe.get('last_error')))
print('last_activity:', probe.get('last_activity') or '')
print()
print('== per-model values ==')
for value in probe.get('values', []):
    print('  {model:<14} len={length:<5} source={source:<8} expires={expires}'.format(
        model=value.get('model', '?'), length=value.get('value_length', 0),
        source=value.get('source') or '-', expires=value.get('expires_at') or '-'))
print()
print('== recent probe history ==')
for record in probe.get('history', [])[:12]:
    status = 'OK ' if record.get('success') else 'FAIL'
    print('  {time} {status} {model:<14} {ms:>6}ms has_error={has_error}'.format(
        time=(record.get('time') or '')[:19], status=status, model=record.get('model', '?'),
        ms=record.get('duration_ms', 0), has_error=bool(record.get('error'))))
print()
print('== rewrite override ==')
override = data.get('turn_state_override') or {}
print('  enabled:', override.get('enabled'), '| force:', override.get('force'),
      '| models:', override.get('models'), '| value_length:', override.get('value_length'))
