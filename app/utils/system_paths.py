"""System-level path resolution that must never be hardcoded per-machine."""

import os
import sys


def get_downloads_dir():
    """The CURRENT machine/user's real Downloads folder - never a hardcoded path.
    This project gets deployed to different company machines/accounts, and a literal
    'C:\\Users\\<name>\\Downloads' only ever works on the one machine it was typed on.

    On Windows, reads the "User Shell Folders" registry entry for the Downloads GUID
    first - the only reliable way to get the RIGHT answer if a user has relocated
    their Downloads folder (right-click it > Properties > Location), which a plain
    <home>/Downloads guess has no way to know about. Falls back to <home>/Downloads
    (correct for the default, never-moved case, and works on any OS) if that registry
    read fails for any reason - e.g. not running on Windows, or the key is missing.
    Always ensures the resolved directory exists before returning it.

    Kept as a separate copy of scripts/shared/download_utils.py's identical helper
    rather than a shared import - app/ and scripts/ are launched as separate processes
    with different sys.path setups (scripts/open_gmail.py and shypple_process.py run
    as standalone subprocesses, not part of the Flask app's package), so there's no
    single import path that reliably reaches both without fragile path hacking."""
    downloads = None
    if sys.platform == "win32":
        try:
            import winreg
            with winreg.OpenKey(
                winreg.HKEY_CURRENT_USER,
                r"Software\Microsoft\Windows\CurrentVersion\Explorer\User Shell Folders",
            ) as key:
                value, _ = winreg.QueryValueEx(key, "{374DE290-123F-4565-9164-39C4925E467B}")
            downloads = os.path.expandvars(value)
        except Exception:
            downloads = None

    if not downloads:
        downloads = os.path.join(os.path.expanduser("~"), "Downloads")

    os.makedirs(downloads, exist_ok=True)
    return downloads
