# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

"""
NICE CXone API - Connection Test (Auth OK, Diagnose DNS/Egress)
- Auth on regional host (e.g., na1.nice-incontact.com)
- Use discovery to get api_endpoint (e.g., https://api-na1.niceincontact.com)
- Build base: {api_endpoint}/incontactapi
- DNS probe + helpful diagnostics for NameResolutionError scenarios
- UPDATED: Uses Azure Key Vault for credentials
"""

import requests, json, socket, sys
from datetime import datetime, timedelta, timezone
from notebookutils import mssparkutils  # Fabric's utility for Key Vault access

# ====== CONFIG ======
# Azure Key Vault Configuration
KEY_VAULT_URL = "https://itt-dataanalytics.vault.azure.net/"
ACCESS_KEY_ID_SECRET_NAME = "NiceInContactKeyIDMBrents"
ACCESS_KEY_SECRET_SECRET_NAME = "NiceInContactSecretKeyMBrents"

# Retrieve credentials from Key Vault
print("Retrieving NICE CXone credentials from Azure Key Vault...")
try:
    ACCESS_KEY_ID = mssparkutils.credentials.getSecret(KEY_VAULT_URL, ACCESS_KEY_ID_SECRET_NAME)
    ACCESS_KEY_SECRET = mssparkutils.credentials.getSecret(KEY_VAULT_URL, ACCESS_KEY_SECRET_SECRET_NAME)
    print("✓ Successfully retrieved both credentials from Key Vault")
except Exception as e:
    print(f"✗ Error retrieving credentials from Key Vault: {str(e)}")
    print("  Make sure:")
    print("  1. The Key Vault is linked to your Fabric workspace")
    print("  2. You have 'Get' permissions on the secrets")
    print(f"  3. Secret names are correct: {ACCESS_KEY_ID_SECRET_NAME}, {ACCESS_KEY_SECRET_SECRET_NAME}")
    raise

FORCE_REGION = "na1"   # your UI shows na1
TIMEOUT = 30
MAX_PRINT = 2000

def dprint(title, value):
    print(f"\n--- {title} ---")
    try:
        text = json.dumps(value, indent=2) if isinstance(value, (dict, list)) else str(value)
    except Exception:
        text = str(value)
    print(text[:MAX_PRINT])
    if len(text) > MAX_PRINT:
        print("... (truncated)")

def regional_auth_url(region: str) -> str:
    return f"https://{region}.nice-incontact.com/authentication/v1/token/access-key"

def regional_discovery_url(region: str) -> str:
    return f"https://{region}.nice-incontact.com/.well-known/cxone-configuration"

def dns_probe(host: str):
    try:
        ip = socket.gethostbyname(host)
        dprint(f"DNS OK for {host}", ip)
        return True
    except Exception as e:
        dprint(f"DNS FAILED for {host}", repr(e))
        return False

print("=" * 60)
print("NICE CXone API - Simple Connection Test")
print("=" * 60)

# 1) AUTHENTICATE (Access Key -> Bearer)
print("\n1️⃣ Testing Authentication...")
auth_url = regional_auth_url(FORCE_REGION)
dprint("Auth URL", auth_url)

try:
    resp = requests.post(
        auth_url,
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        json={"accessKeyId": ACCESS_KEY_ID, "accessKeySecret": ACCESS_KEY_SECRET},
        timeout=TIMEOUT,
    )
    dprint("Auth HTTP status", resp.status_code)
    try:
        auth_data = resp.json()
        print("\nFULL ACCESS TOKEN:\n")
        print(auth_data["access_token"])

    except Exception:
        auth_data = {"_non_json_body": resp.text}
    dprint("Auth response (raw)", auth_data)

    token = auth_data.get("access_token")
    expires_in = auth_data.get("expires_in")
    if not token:
        raise RuntimeError("No access_token in auth response.")

    exp_at = None
    if isinstance(expires_in, (int, float)):
        exp_at = (datetime.now(timezone.utc) + timedelta(seconds=int(expires_in))).isoformat()

    print("   ✅ Authentication SUCCESSFUL!")
    print(f"   Region: {FORCE_REGION}")
    print(f"   Token expires in: {expires_in} seconds" + (f" (≈ {exp_at})" if exp_at else ""))

except Exception as e:
    print(f"   ❌ Authentication FAILED: {e}")
    raise

# 2) REGIONAL DISCOVERY -> api_endpoint
print("\n2️⃣ Discovering API endpoint (regional)...")
disc_url = regional_discovery_url(FORCE_REGION)
dprint("Discovery URL", disc_url)

try:
    disc = requests.get(
        disc_url,
        headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
        timeout=TIMEOUT,
    )
    dprint("Discovery HTTP status", disc.status_code)
    try:
        disc_json = disc.json()
    except Exception:
        disc_json = {"_non_json_body": disc.text}
    dprint("Discovery payload", disc_json)

    api_endpoint = (disc_json.get("api_endpoint") or "").rstrip("/")
    if not api_endpoint:
        # very rare on regional; if so, synthesize
        api_endpoint = f"https://api-{FORCE_REGION}.niceincontact.com"
        print("   ⚠️ No api_endpoint in discovery; using synthesized:", api_endpoint)

except requests.exceptions.RequestException as e:
    print(f"   ❌ Discovery request error: {e}")
    api_endpoint = f"https://api-{FORCE_REGION}.niceincontact.com"
    print("   ➜ Using synthesized api_endpoint:", api_endpoint)

# 2a) DNS PROBE for api_endpoint host
from urllib.parse import urlparse
api_host = urlparse(api_endpoint).hostname or ""
if api_host:
    dns_ok = dns_probe(api_host)
else:
    dns_ok = False
    dprint("Error", "Could not parse host from api_endpoint.")

# Build base = {api_endpoint}/incontactapi
api_base = f"{api_endpoint}/incontactapi"
dprint("API Base", api_base)

# Early exit-style message (no sys.exit): if DNS fails, the API call will also fail.
if not dns_ok:
    print("\n⚠️  DNS to the API host failed. In a managed environment (Fabric/corporate), ask to allowlist:")
    print("   - api-na1.niceincontact.com  (API)")
    print("   - cxone.niceincontact.com    (discovery)")
    print("   - na1.nice-incontact.com     (regional auth/UI)")
    print("   And generally: *.niceincontact.com and *.nice-incontact.com")
    print("   After allowlisting, re-run this cell.")
    # continue to attempt the call anyway so you see the same error Fabric shows

# 3) TEST CALL: GET /services/v27.0/skills
print("\n3️⃣ Testing API Call: GET /services/v27.0/skills")
test_url = f"{api_base}/services/v27.0/skills"
dprint("Test URL", test_url)

try:
    r = requests.get(
        test_url,
        headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
        timeout=TIMEOUT,
    )
    dprint("Skills HTTP status", r.status_code)
    try:
        payload = r.json()
    except Exception:
        payload = {"_non_json_body": r.text}
    dprint("Skills payload (sample)", payload)

    if r.ok:
        print("   ✅ API Call SUCCESSFUL!")
    else:
        print("   ⚠️ API call returned non-2xx. See payload above (permissions/route).")

except requests.exceptions.RequestException as e:
    print(f"   ❌ API Call FAILED (network/HTTP): {e}")
    print("\n👉 Likely causes here:")
    print("   • DNS/egress block to niceincontact.com (auth host uses nice-incontact.com, which worked)")
    print("   • Corporate proxy/SSL interception not trusting the cert chain")
    print("\n✅ Fix by allowlisting these FQDNs/IPs per CXone connectivity requirements and retry:")
    print("   • api-na1.niceincontact.com")
    print("   • cxone.niceincontact.com")
    print("   • na1.nice-incontact.com")
    print("   • Wildcards: *.niceincontact.com, *.nice-incontact.com")
except Exception as e:
    print(f"   ❌ API Call FAILED (logic): {e}")

print("\n" + "=" * 60)
print("✅ Connection Test Complete!")
print("=" * 60)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
