#!/usr/bin/env python3
"""
nuvio_release_watcher.py

Polls GitHub for new Nuvio/Orivio/Debrify .ipa releases and pushes each to
atvloadly's /mcp install_app tool on its correct target device.
After triggering an install, it waits for atvloadly's own docker logs
to confirm success or failure before recording the release as installed --
an install that's merely "queued" is not trusted until confirmed.

Requires:
    pip install mcp requests

Run modes:
    python3 nuvio_release_watcher.py --discover
        -> lists atvloadly's paired devices and Apple accounts with their
           IDs so you can fill in ACCOUNT_ID and each repo's device_id below.

    python3 nuvio_release_watcher.py
        -> normal run: checks configured repos for new releases, downloads
           any new .ipa, triggers install on atvloadly, and waits to confirm
           the result before updating state.

Intended to run on the Pi itself via cron, e.g. every 30 min:
    */30 * * * * /usr/bin/python3 /home/pi/nuvio-watcher/nuvio_release_watcher.py >> /home/pi/nuvio-watcher/watcher.log 2>&1
"""

import asyncio
import json
import os
import plistlib
import re
import subprocess
import sys
import time
import zipfile
from datetime import datetime, timedelta

import requests

from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

ATVLOADLY_MCP_URL = "http://127.0.0.1:5533/mcp"

# Single Apple account used for all installs.
ACCOUNT_ID = "5a9d3a6efffe846aa3fc57e15e29584d"

# Known device IDs (from --discover).
DEVICE_APPLE_TV = "1ae6fb9343ba80df6a5ce530bffe874d"   # Saad Sabrina Apple TV
DEVICE_IPHONE = "dcf544bf98dcb00f4f0e9b28931bf199"      # Saad's iPhone

# Each repo carries its own target device_id, and optional asset filters:
#   asset_match   - a substring that MUST appear in the .ipa filename
#                   (case-insensitive). Use when a release ships more than
#                   one .ipa and you need a specific one picked every time.
#   asset_exclude - a substring that must NOT appear in the .ipa filename.
#                   Useful for positively excluding a sibling build (e.g.
#                   picking the iOS ipa by excluding "tvos") rather than
#                   relying on asset order.
#   label         - optional. Used as the state-tracking key instead of
#                   "owner/repo". REQUIRED whenever the same owner/repo
#                   appears more than once in this list (e.g. one repo
#                   shipping both a tvOS and an iOS build to two different
#                   devices) -- otherwise both entries would collide on the
#                   same state key and silently stomp on each other's
#                   install-confirmation tracking.
#   account_id    - optional. Overrides ACCOUNT_ID for just this repo, so a
#                   specific install can use a different Apple ID (e.g. to
#                   dodge one account's App ID creation quota).
#   account_ids   - optional. A list of account_ids to try IN ORDER for this
#                   repo instead of a single account_id. Each account that
#                   hits Apple's App ID quota error gets its OWN 24h cooldown
#                   (tracked separately from the others), and the script
#                   automatically moves on to the next account in the list
#                   within the same run. The repo itself is only skipped on
#                   a future run once every account in the list is cooling
#                   down. Takes priority over account_id when both are set.
#   account_email - optional. The Apple ID's email instead of its atvloadly
#                   account id; looked up from atvloadly's account list on
#                   each run. Used when account_id/account_ids are not set.
#                   If the email is not found, that repo is skipped (never
#                   installed with a different account).
#   include_prereleases - optional. Look at the newest non-draft release,
#                   pre-releases included, instead of GitHub's "latest"
#                   release (which ignores pre-releases).
REPOS = [
    {"owner": "skhan85", "repo": "NuvioTV", "device_id": DEVICE_APPLE_TV, "asset_match": None, "asset_exclude": None, "label": "skhan85/NuvioTV (fork)", "account_id": "0e67bd1d418cc7197c3618405589ba05"},
    {"owner": "skhan85", "repo": "NuvioMobile", "device_id": DEVICE_IPHONE, "asset_match": None, "asset_exclude": None, "label": None, "account_id": "0e67bd1d418cc7197c3618405589ba05"},
    # Player test (AetherEngine). Separate app ID (com.pyksel.nuviotvos), installs alongside the fork.
    # Remove this line after the test.
]

DOWNLOAD_DIR = os.path.expanduser("~/nuvio-ipas")
SERVE_BASE_URL = "http://192.168.85.100:8090"
STATE_FILE = os.path.expanduser("~/.nuvio_watcher_state.json")

# How long to wait for atvloadly to confirm an install actually succeeded
# or failed, by tailing its own docker logs, before giving up for this run.
INSTALL_CONFIRM_TIMEOUT_SECONDS = 360
INSTALL_CONFIRM_POLL_SECONDS = 15

# If an install fails specifically because of Apple's free-account App ID
# creation quota (10 new App IDs per rolling 7 days), don't retry that repo
# again for this long -- retrying sooner can't possibly succeed and just
# adds log noise.
APP_ID_QUOTA_COOLDOWN_HOURS = 24

# ---------------------------------------------------------------------------

def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            return json.load(f)
    return {}


def save_state(state):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


def check_release(owner, repo, asset_match=None, asset_exclude=None, include_prereleases=False):
    """Return (tag, ipa_asset_url, ipa_filename) for the latest release, or None.

    If asset_match is given, only an asset whose filename contains that
    substring (case-insensitive) is considered. If asset_exclude is given,
    any asset whose filename contains that substring is skipped. Otherwise
    the first .ipa asset found (after exclusion) is used.
    """
    if include_prereleases:
        url = f"https://api.github.com/repos/{owner}/{repo}/releases?per_page=10"
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        data = next((rel for rel in resp.json() if not rel.get("draft")), None)
        if data is None:
            return None
    else:
        url = f"https://api.github.com/repos/{owner}/{repo}/releases/latest"
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        data = resp.json()
    tag = data.get("tag_name")
    for asset in data.get("assets", []):
        name = asset.get("name", "")
        if not name.lower().endswith(".ipa"):
            continue
        if asset_match and asset_match.lower() not in name.lower():
            continue
        if asset_exclude and asset_exclude.lower() in name.lower():
            continue
        return tag, asset["browser_download_url"], name
    return None


def ipa_app_name(ipa_path):
    """The app's name as atvloadly logs it (CFBundleDisplayName, else CFBundleName),
    read from the .ipa's Info.plist. None if it can't be read."""
    try:
        with zipfile.ZipFile(ipa_path) as z:
            plist_name = next((n for n in z.namelist()
                               if re.fullmatch(r"Payload/[^/]+\.app/Info\.plist", n)), None)
            if not plist_name:
                return None
            info = plistlib.loads(z.read(plist_name))
        return (info.get("CFBundleDisplayName") or info.get("CFBundleName") or "").strip() or None
    except Exception as e:
        print(f"  (could not read app name from {ipa_path}: {e})")
        return None


def download_ipa(url, dest_path):
    os.makedirs(os.path.dirname(dest_path), exist_ok=True)
    with requests.get(url, stream=True, timeout=60) as r:
        r.raise_for_status()
        with open(dest_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1 << 16):
                f.write(chunk)


async def mcp_call(tool_name, arguments):
    async with streamable_http_client(ATVLOADLY_MCP_URL) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            result = await session.call_tool(tool_name, arguments)
            return result


async def discover():
    print("Devices:")
    devices = await mcp_call("get_device_list", {})
    print(devices)
    print("\nAccounts:")
    accounts = await mcp_call("get_account_list", {})
    print(accounts)


def _account_id_from_list(payload, email):
    """Find the id of the account whose fields contain `email`, in whatever shape
    atvloadly's get_account_list returns (a list of objects, or wrapped in one)."""
    email = email.lower()
    if isinstance(payload, dict):
        values = [str(v).lower() for v in payload.values() if isinstance(v, (str, int))]
        if email in values:
            for k in ("id", "account_id", "accountId", "ID", "_id"):
                if payload.get(k):
                    return str(payload[k])
        for v in payload.values():
            found = _account_id_from_list(v, email)
            if found:
                return found
    elif isinstance(payload, list):
        for v in payload:
            found = _account_id_from_list(v, email)
            if found:
                return found
    return None


async def resolve_account_id(email):
    """Return atvloadly's account id for an Apple ID email, or None."""
    result = await mcp_call("get_account_list", {})
    texts = [getattr(c, "text", "") for c in (getattr(result, "content", None) or [])]
    structured = getattr(result, "structuredContent", None)
    if structured:
        found = _account_id_from_list(structured, email)
        if found:
            return found
    for text in texts:
        try:
            found = _account_id_from_list(json.loads(text), email)
            if found:
                return found
        except ValueError:
            pass
        # Plain-text listing: take the 32-hex id nearest to the email.
        idx = text.lower().find(email.lower())
        if idx >= 0:
            ids = [(abs(m.start() - idx), m.group(0)) for m in re.finditer(r"\b[0-9a-f]{32}\b", text)]
            if ids:
                return min(ids)[1]
    return None


async def install_ipa(ipa_url, device_id, account_id=None):
    args = {"ipa_url": ipa_url, "device_id": device_id}
    effective_account_id = account_id or ACCOUNT_ID
    if effective_account_id:
        args["account_id"] = effective_account_id
    result = await mcp_call("install_app", args)
    print("install_app result:", result)
    return result


def wait_for_install_result(since_dt=None, app_name=None, timeout_seconds=INSTALL_CONFIRM_TIMEOUT_SECONDS,
                             poll_interval=INSTALL_CONFIRM_POLL_SECONDS):
    """Poll `docker logs atvloadly` for the first success/failure line since
    the install was triggered. Uses a relative "--since Ns" window (seconds
    elapsed, plus a safety buffer) rather than an absolute timestamp, since
    docker interprets a bare absolute timestamp in the host's local timezone
    and that caused real failures to be missed entirely.
    Returns (True, None) on confirmed success, (False, error_text) on
    confirmed failure, or (None, None) if nothing appeared within the
    timeout (treated as inconclusive -- caller should not trust it as done).

    atvloadly logs "Installing ipa success: <app name>" and "Installing ipa
    failed: <app name> ...". When app_name is known, only lines for THAT app
    count, so another app's install or a manual Refresh in the same window can
    no longer be mistaken for this one (that once marked a failed Nuvio install
    as done, and the release was never retried).
    """
    start = time.time()
    deadline = start + timeout_seconds
    while time.time() < deadline:
        elapsed_seconds = int(time.time() - start) + 15  # +15s safety buffer
        try:
            result = subprocess.run(
                ["docker", "logs", "atvloadly", "--since", f"{elapsed_seconds}s", "--tail", "200"],
                capture_output=True, text=True, timeout=30,
            )
            log_output = result.stdout + result.stderr
        except Exception as e:
            log_output = ""
            print(f"  (docker logs check failed: {e})")

        log_output = re.sub(r"\x1b\[[0-9;]*m", "", log_output)  # drop colour codes
        for line in log_output.splitlines():
            ok = re.search(r"Installing ipa success: (.+?)\s*$", line)
            if ok and (not app_name or ok.group(1).strip() == app_name):
                return True, None
            bad = re.search(r"Installing ipa failed: (.*)", line)
            if bad and (not app_name or bad.group(1).startswith(app_name)):
                return False, bad.group(0).strip()

        time.sleep(poll_interval)

    return None, None


def is_app_id_quota_error(error_text):
    if not error_text:
        return False
    return "maximum app id limit" in error_text.lower() or "app id limit has been reached" in error_text.lower()


def account_cooldown_key(account_id):
    return f"account_cooldown:{account_id}"


def is_account_on_cooldown(state, account_id):
    key = account_cooldown_key(account_id)
    if key not in state:
        return False
    cooldown_until = datetime.fromisoformat(state[key])
    return datetime.utcnow() < cooldown_until


def set_account_cooldown(state, account_id):
    cooldown_until = datetime.utcnow() + timedelta(hours=APP_ID_QUOTA_COOLDOWN_HOURS)
    state[account_cooldown_key(account_id)] = cooldown_until.isoformat()
    return cooldown_until


def main():
    if "--discover" in sys.argv:
        asyncio.run(discover())
        return

    state = load_state()
    changed = False

    for r in REPOS:
        key = r.get("label") or f"{r['owner']}/{r['repo']}"
        device_id = r["device_id"]
        asset_match = r.get("asset_match")
        asset_exclude = r.get("asset_exclude")

        # Build the ordered list of accounts to try for this repo. account_ids
        # (a rotation list) takes priority over a single account_id; falling
        # back to [None] means "use the global ACCOUNT_ID / atvloadly default".
        candidate_accounts = r.get("account_ids") or [r.get("account_id")]
        if not r.get("account_ids") and not r.get("account_id") and r.get("account_email"):
            try:
                resolved = asyncio.run(resolve_account_id(r["account_email"]))
            except Exception as e:
                print(f"[{key}] could not read atvloadly's account list: {e}")
                continue
            if not resolved:
                print(f"[{key}] skipping -- no atvloadly account found for {r['account_email']}")
                continue
            candidate_accounts = [resolved]

        # Only skip the whole repo if EVERY candidate account is currently
        # cooling down (accounts with no id, i.e. None, are never on cooldown --
        # there's nothing to track quota against).
        available_accounts = [a for a in candidate_accounts if a is None or not is_account_on_cooldown(state, a)]
        if not available_accounts:
            print(f"[{key}] skipping -- all {len(candidate_accounts)} candidate account(s) are on Apple App ID quota cooldown")
            continue

        try:
            release = check_release(r["owner"], r["repo"], asset_match, asset_exclude,
                                    r.get("include_prereleases", False))
        except Exception as e:
            print(f"[{key}] error checking release: {e}")
            continue

        if release is None:
            print(f"[{key}] no matching .ipa asset found in latest release")
            continue

        tag, asset_url, filename = release
        if state.get(key) == tag:
            print(f"[{key}] no new release (still {tag})")
            continue

        print(f"[{key}] new release detected: {tag} -> {filename}")
        dest_path = os.path.join(DOWNLOAD_DIR, filename)
        try:
            download_ipa(asset_url, dest_path)
        except Exception as e:
            print(f"[{key}] download failed: {e}")
            continue

        old_filename = state.get(f"{key}:filename")
        if old_filename and old_filename != filename:
            old_path = os.path.join(DOWNLOAD_DIR, old_filename)
            if os.path.exists(old_path):
                try:
                    os.remove(old_path)
                    print(f"[{key}] removed old file: {old_filename}")
                except Exception as e:
                    print(f"[{key}] could not remove old file {old_filename}: {e}")

        ipa_url = f"{SERVE_BASE_URL}/{filename}"
        install_succeeded = False
        app_name = ipa_app_name(dest_path)
        if app_name:
            print(f"[{key}] app name in atvloadly's log: {app_name}")

        for account_id in available_accounts:
            account_label = account_id or "(default account)"
            print(f"[{key}] attempting install with account {account_label}...")
            install_triggered_at = datetime.utcnow()
            try:
                asyncio.run(install_ipa(ipa_url, device_id, account_id))
            except Exception as e:
                print(f"[{key}] atvloadly install call failed for account {account_label}: {e}")
                break  # not an account-specific problem (e.g. atvloadly unreachable) -- stop for this run

            print(f"[{key}] waiting up to {INSTALL_CONFIRM_TIMEOUT_SECONDS}s for atvloadly to confirm the result...")
            success, error_text = wait_for_install_result(install_triggered_at, app_name)

            if success is True:
                state[key] = tag
                state[f"{key}:filename"] = filename
                changed = True
                install_succeeded = True
                print(f"[{key}] CONFIRMED install success for {tag} on device {device_id} using account {account_label}")
                break

            elif success is False:
                print(f"[{key}] CONFIRMED install FAILURE on account {account_label}: {error_text}")
                if is_app_id_quota_error(error_text) and account_id is not None:
                    cooldown_until = set_account_cooldown(state, account_id)
                    changed = True
                    print(f"[{key}] account {account_label} hit App ID quota -- cooling down until {cooldown_until.isoformat()}Z, trying next account if any")
                    continue  # try the next candidate account, if any
                else:
                    # A non-quota failure (or no account_id to rotate away from)
                    # isn't fixed by switching accounts -- stop here for this run.
                    # "device not found" / "afc service not available" usually just
                    # means the device was asleep or off Wi-Fi; the release stays
                    # unrecorded, so the next run tries it again.
                    if error_text and ("device not found" in error_text or "afc service not available" in error_text):
                        print(f"[{key}] device not reachable right now (asleep or off Wi-Fi?) -- will retry next run")
                    break

            else:
                print(f"[{key}] install result unknown after {INSTALL_CONFIRM_TIMEOUT_SECONDS}s "
                      f"(atvloadly may still be working) -- will re-check next run")
                break  # ambiguous -- don't try another account on top of a possibly-still-running install

        if not install_succeeded:
            print(f"[{key}] no account succeeded this run -- will retry next run (or once a cooldown expires)")
            # state[key] deliberately NOT updated in this case.

    if changed:
        save_state(state)


if __name__ == "__main__":
    main()
