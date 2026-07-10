import httpx, json
from pathlib import Path

token = Path('tokens/current_token.txt').read_text().strip()
venue_id = 'ec7d2c4e-dc4a-434f-97ee-95cfd0f3c3a5'
sport_code = 'SP83'
date = '2026-07-10'

url = f'https://api.playo.io/booking-lab-public/availability/v1/{venue_id}/{sport_code}/{date}'
headers = {
    'authorization': token,
    'accept': 'application/json',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'referer': 'https://playo.co/booking',
}

print(f'URL: {url}')
print(f'Token (first 40): {token[:40]}...')
print()

r = httpx.get(url, headers=headers, timeout=15, follow_redirects=True)
print(f'Status: {r.status_code}')
print()

try:
    data = r.json()
    if isinstance(data, dict):
        print('Top-level keys:', list(data.keys()))
    else:
        print('Response is a list, length:', len(data))
    print()
    print(json.dumps(data, indent=2)[:3000])
except Exception as e:
    print('JSON parse error:', e)
    print('Raw (first 1000 chars):')
    print(r.text[:1000])
