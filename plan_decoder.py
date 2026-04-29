import base64
import json
import sys

if len(sys.argv) != 2:
    raise SystemExit("Usage: plan_decoder.py <output_json_path>")

output_json_path = sys.argv[1]

with open('run.log', 'r') as fb:
    base64_payload = fb.read().split('payloadBase64=')[1].split('\n')[0]
    payload = base64.b64decode(base64_payload).decode('utf-8')
    payload = json.loads(payload)
    print(payload)

with open(output_json_path, 'w', encoding='utf-8') as f:
    json.dump(payload, f)