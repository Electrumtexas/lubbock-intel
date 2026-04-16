"""
push_secrets.py
Reads FILL_IN_SECRETS.txt and pushes each value to GitHub as a
repository secret using the GitHub REST API.

Requirements: Python only — no GitHub CLI needed.
  pip install requests PyNaCl

Run from this folder:
  python push_secrets.py
"""

import sys
import base64
import requests
from pathlib import Path

REPO         = "Electrumtexas/lubbock-intel"
SECRETS_FILE = Path(__file__).parent / "FILL_IN_SECRETS.txt"


def install_if_missing():
    """Auto-install required packages if not present."""
    import subprocess
    for pkg in ("requests", "PyNaCl"):
        try:
            __import__(pkg.lower().replace("pynacl", "nacl"))
        except ImportError:
            print(f"Installing {pkg}...")
            subprocess.check_call([sys.executable, "-m", "pip", "install", pkg, "--quiet"])


def load_file(path: Path) -> dict:
    values = {}
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, val = line.partition("=")
        key = key.strip()
        val = val.strip()
        if key and val:
            values[key] = val
    return values


def get_repo_public_key(token: str) -> tuple[str, str]:
    """Fetch the repo's public key needed to encrypt secrets."""
    url = f"https://api.github.com/repos/{REPO}/actions/secrets/public-key"
    r = requests.get(url, headers={
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
    })
    if r.status_code != 200:
        print(f"\nERROR fetching repo public key: {r.status_code}")
        print(r.text)
        sys.exit(1)
    data = r.json()
    return data["key_id"], data["key"]


def encrypt_secret(public_key_b64: str, secret_value: str) -> str:
    """Encrypt a secret value using the repo's public key (libsodium)."""
    from nacl import encoding, public
    public_key = public.PublicKey(public_key_b64.encode(), encoding.Base64Encoder)
    sealed_box = public.SealedBox(public_key)
    encrypted  = sealed_box.encrypt(secret_value.encode())
    return base64.b64encode(encrypted).decode()


def push_secret(token: str, key_id: str, pub_key: str, name: str, value: str) -> bool:
    encrypted = encrypt_secret(pub_key, value)
    url = f"https://api.github.com/repos/{REPO}/actions/secrets/{name}"
    r = requests.put(url, json={
        "encrypted_value": encrypted,
        "key_id":          key_id,
    }, headers={
        "Authorization": f"Bearer {token}",
        "Accept":        "application/vnd.github+json",
    })
    if r.status_code in (201, 204):
        print(f"  ✓  {name}")
        return True
    else:
        print(f"  ✗  {name}  —  {r.status_code}: {r.text[:120]}")
        return False


def main():
    install_if_missing()

    if not SECRETS_FILE.exists():
        print(f"ERROR: {SECRETS_FILE.name} not found.")
        sys.exit(1)

    all_values = load_file(SECRETS_FILE)

    token = all_values.pop("GITHUB_TOKEN", "")
    if not token:
        print("\nERROR: GITHUB_TOKEN is missing from FILL_IN_SECRETS.txt")
        print("Create one at: https://github.com/settings/tokens")
        print("Check the 'repo' scope, then paste it next to GITHUB_TOKEN=")
        sys.exit(1)

    secrets = {k: v for k, v in all_values.items() if v}
    if not secrets:
        print("No secrets found — make sure you filled in values in FILL_IN_SECRETS.txt")
        sys.exit(1)

    print(f"\nConnecting to GitHub repo: {REPO}")
    key_id, pub_key = get_repo_public_key(token)
    print(f"Repo public key fetched. Pushing {len(secrets)} secrets...\n")

    success = 0
    for name, value in secrets.items():
        if push_secret(token, key_id, pub_key, name, value):
            success += 1

    print(f"\nDone: {success}/{len(secrets)} secrets pushed to GitHub.")
    if success == len(secrets):
        print("\nAll done! You can now delete FILL_IN_SECRETS.txt.")
    else:
        print("\nSome secrets failed — check errors above.")


if __name__ == "__main__":
    main()
