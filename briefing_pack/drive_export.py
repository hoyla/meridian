"""Push a briefing-pack bundle into Google Drive as native Google Docs /
Sheets, with a subfolder of the raw .md/.xlsx kept for LLM use.

Target Drive layout (per export):

    <export folder>                    e.g. "Meridian — 2026-05-21-1439"
    ├── 01_Read_Me_First               Google Doc
    ├── 03_Leads                       Google Doc
    ├── 02_Findings                    Google Doc
    ├── 04_Data                        Google Sheet
    ├── 05_Groups                      Google Doc
    └── Markdown versions for use with LLMs etc/
        ├── 01_Read_Me_First.md … 05_Groups.md
        └── 04_Data.xlsx

Pipeline per Google-Doc artefact:
  1. Upload the *house-styled* `.docx` to Drive with conversion to a native
     Google Doc (Drive does the conversion via the target mimeType).
  2. Mint heading navigation anchors. Google's `.docx` importer does NOT
     assign the `headingId`s that drive `#heading=…` navigation, the
     document outline links, and tables of contents — it assigns them only
     when a heading paragraph is edited through the editor. We replay that
     edit programmatically: a batched "style-flip" pass that sets every
     heading to NORMAL_TEXT and then back to its own heading level. Proven
     by `scripts/drive_heading_anchor_test.py` (Touch C); style changes
     don't alter paragraph length, so all ranges stay valid across the two
     passes. The `.xlsx` → Sheet artefact needs no such step.

Idempotent: files are matched by name within their parent folder and
updated in place, so re-running an export refreshes content rather than
duplicating it. Each cycle normally lands in its own timestamped folder
anyway, so cross-cycle there is no collision.

Auth: OAuth user credentials (scope `drive.file` only — per-file access to
files the app creates/opens, which covers both creating the Doc and editing
it afterwards). Client secret at ~/.config/meridian/client_secret.json;
refresh token persisted to ~/.config/meridian/google-token.json.
"""

from __future__ import annotations

import contextlib
import logging
import os
import socket
from pathlib import Path

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

log = logging.getLogger(__name__)

SCOPES = ["https://www.googleapis.com/auth/drive.file"]
CONFIG_DIR = Path(os.path.expanduser("~/.config/meridian"))
CLIENT_SECRET = CONFIG_DIR / "client_secret.json"
TOKEN = CONFIG_DIR / "google-token.json"

DOCX_MIME = (
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
)
XLSX_MIME = (
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
)
MD_MIME = "text/markdown"
GDOC_MIME = "application/vnd.google-apps.document"
GSHEET_MIME = "application/vnd.google-apps.spreadsheet"
FOLDER_MIME = "application/vnd.google-apps.folder"

# Keep in sync with `briefing_pack.render._MARKDOWN_SUBFOLDER` — the local
# bundle uses the same subfolder name, and this mirrors it to Drive.
MARKDOWN_SUBFOLDER = "Markdown versions for use with LLMs etc"

# google-api-python-client creates its sockets with NO timeout, so a stalled
# read blocks forever (observed 2026-06-18: uploads hung in getresponse()
# while curl to the same endpoints returned instantly and the token was
# valid). Bounding the socket read makes a stall fail fast instead of hanging
# an unattended / cron run. 120s is generous for the ~1 MB artefacts.
DRIVE_SOCKET_TIMEOUT_S = 120


@contextlib.contextmanager
def _bounded_socket_reads(seconds: float = DRIVE_SOCKET_TIMEOUT_S):
    """Set a process-wide default socket timeout for the duration of the
    block, restoring whatever was there before. Scoped to the Drive calls so
    we don't impose a timeout on the rest of the process (e.g. DB sockets)."""
    prev = socket.getdefaulttimeout()
    socket.setdefaulttimeout(seconds)
    try:
        yield
    finally:
        socket.setdefaulttimeout(prev)

# How many updateParagraphStyle requests to send per batchUpdate when
# minting anchors. The Docs API accepts large batches, but chunking keeps
# any single request well within size limits on big documents.
_FLIP_CHUNK = 100

# Which bundle files convert to which native Google type, and the source
# mimeType to upload them as. Driven by what's present in the bundle dir,
# so the four-Doc + one-Sheet set fills in automatically as each styled
# artefact starts being generated.
_CONVERT: dict[str, tuple[str, str]] = {
    "01_Read_Me_First.docx": (GDOC_MIME, DOCX_MIME),
    "03_Leads.docx": (GDOC_MIME, DOCX_MIME),
    "02_Findings.docx": (GDOC_MIME, DOCX_MIME),
    "04_Data.xlsx": (GSHEET_MIME, XLSX_MIME),
    "05_Groups.docx": (GDOC_MIME, DOCX_MIME),
}

# Raw artefacts copied verbatim into the markdown subfolder (no conversion).
_RAW_SUFFIXES = (".md", ".xlsx")


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------

class TokenUnusableError(RuntimeError):
    """Raised (only when interactive=False) when the saved OAuth token is
    missing or can't be silently refreshed — so an unattended caller fails
    loud instead of blocking on a browser consent prompt."""


_REAUTH_HINT = (
    "Re-authorise once, interactively, by running the upload by hand: "
    "`python scrape.py --upload-to-drive <bundle_dir>`."
)


def get_credentials(*, interactive: bool = True) -> Credentials:
    """Load (and silently refresh) the persisted OAuth credentials.

    `interactive` (default True): if there is no usable token, open a
    browser for consent (the manual / first-run path). Set `interactive=
    False` for unattended callers (a scheduled run): instead of opening a
    browser — which would hang with no one to click it — raise
    `TokenUnusableError` so the caller can fail loud and notify.
    """
    creds: Credentials | None = None
    if TOKEN.exists():
        creds = Credentials.from_authorized_user_file(str(TOKEN), SCOPES)
    if creds and creds.valid:
        return creds

    # Try a silent refresh first (the common case — access token expired
    # but the refresh token is still good).
    if creds and creds.expired and creds.refresh_token:
        try:
            creds.refresh(Request())
        except Exception as exc:  # RefreshError etc. — token revoked/invalid
            if not interactive:
                raise TokenUnusableError(
                    f"Google OAuth token could not be refreshed ({exc}). "
                    + _REAUTH_HINT
                ) from exc
            creds = None  # fall through to the interactive flow

    if not creds or not creds.valid:
        if not interactive:
            raise TokenUnusableError(
                "No usable Google OAuth token (missing or unrefreshable). "
                + _REAUTH_HINT
            )
        if not CLIENT_SECRET.exists():
            raise FileNotFoundError(
                f"OAuth client secret not found at {CLIENT_SECRET}. "
                "Download a Desktop-app client from the GCP console and "
                "save it there."
            )
        flow = InstalledAppFlow.from_client_secrets_file(
            str(CLIENT_SECRET), SCOPES,
        )
        creds = flow.run_local_server(port=0)

    TOKEN.write_text(creds.to_json())
    os.chmod(TOKEN, 0o600)
    return creds


# ---------------------------------------------------------------------------
# Drive helpers
# ---------------------------------------------------------------------------

def _escape(s: str) -> str:
    """Escape a string for use in a Drive `q` query literal."""
    return s.replace("\\", "\\\\").replace("'", "\\'")


def _find_file(
    drive, name: str, parent_id: str | None, *, folder: bool = False,
) -> dict | None:
    """Return the first app-visible file/folder matching `name` within
    `parent_id` (or anywhere the app can see, if parent_id is None), else
    None. With the drive.file scope, list() only sees files the app created
    — exactly the ones we want to match for idempotent re-runs."""
    clauses = [f"name='{_escape(name)}'", "trashed=false"]
    if folder:
        clauses.append(f"mimeType='{FOLDER_MIME}'")
    if parent_id:
        clauses.append(f"'{parent_id}' in parents")
    resp = drive.files().list(
        q=" and ".join(clauses),
        spaces="drive",
        fields="files(id,name,webViewLink)",
    ).execute()
    files = resp.get("files", [])
    return files[0] if files else None


def _find_or_create_folder(
    drive, name: str, parent_id: str | None = None,
) -> str:
    existing = _find_file(drive, name, parent_id, folder=True)
    if existing:
        return existing["id"]
    body = {"name": name, "mimeType": FOLDER_MIME}
    if parent_id:
        body["parents"] = [parent_id]
    return drive.files().create(body=body, fields="id").execute()["id"]


def _upsert(
    drive, local_path: Path, name: str, parent_id: str,
    *, source_mime: str, target_mime: str | None = None,
) -> tuple[str, str]:
    """Create-or-update file `name` in `parent_id` from `local_path`,
    updating in place so re-runs stay idempotent (no duplicates). With
    `target_mime` set, the upload converts to that native Google type
    (Doc/Sheet) and the web link is returned; without it the file is stored
    verbatim and the link is ''."""
    media = MediaFileUpload(str(local_path), mimetype=source_mime, resumable=True)
    fields = "id,webViewLink" if target_mime else "id"
    existing = _find_file(drive, name, parent_id)
    if existing:
        f = drive.files().update(
            fileId=existing["id"], media_body=media, fields=fields,
        ).execute()
    else:
        body = {"name": name, "parents": [parent_id]}
        if target_mime:
            body["mimeType"] = target_mime
        f = drive.files().create(
            body=body, media_body=media, fields=fields,
        ).execute()
    return f["id"], f.get("webViewLink", "")


# ---------------------------------------------------------------------------
# Heading-anchor minting (the productionized Touch C)
# ---------------------------------------------------------------------------

def _heading_paragraphs(docs, doc_id: str) -> list[dict]:
    """Body-level heading paragraphs with their range, level and whether a
    headingId is already present."""
    doc = docs.documents().get(documentId=doc_id).execute()
    out: list[dict] = []
    for el in doc.get("body", {}).get("content", []):
        para = el.get("paragraph")
        if not para:
            continue
        style = para.get("paragraphStyle", {})
        named = style.get("namedStyleType", "")
        if not named.startswith("HEADING_"):
            continue
        out.append({
            "start": el["startIndex"],
            "end": el["endIndex"],
            "named": named,
            "has_id": bool(style.get("headingId")),
        })
    return out


def _style_request(h: dict, named: str) -> dict:
    return {"updateParagraphStyle": {
        "range": {"startIndex": h["start"], "endIndex": h["end"]},
        "paragraphStyle": {"namedStyleType": named},
        "fields": "namedStyleType",
    }}


def _run_batches(docs, doc_id: str, requests: list[dict]) -> None:
    for i in range(0, len(requests), _FLIP_CHUNK):
        docs.documents().batchUpdate(
            documentId=doc_id,
            body={"requests": requests[i:i + _FLIP_CHUNK]},
        ).execute()


def mint_heading_anchors(docs, doc_id: str) -> int:
    """Force Google to assign navigation anchors to every heading that
    imported without one, by replaying an editor-grade edit: flip each
    heading to NORMAL_TEXT, then back to its own level. Returns the number
    of headings touched. All of pass 1 runs before pass 2 so each heading
    experiences a genuine NORMAL→HEADING change."""
    targets = [h for h in _heading_paragraphs(docs, doc_id) if not h["has_id"]]
    if not targets:
        return 0
    _run_batches(docs, doc_id, [_style_request(h, "NORMAL_TEXT") for h in targets])
    _run_batches(docs, doc_id, [_style_request(h, h["named"]) for h in targets])
    return len(targets)


def fix_internal_heading_links(docs, doc_id: str) -> int:
    """Repoint in-document links (e.g. the Groups "Quick index") at the real
    headings. Markdown `#slug` links convert to dangling bookmark links on
    import; rewrite each to a native `headingId` link. The link's display
    text is the group name, which equals the heading text — so we match on
    text, with a prefix fallback for headings that carry a suffix (e.g. a
    draft marker). Must run after `mint_heading_anchors`. Returns the number
    of links repointed."""
    doc = docs.documents().get(documentId=doc_id).execute()

    heading_id_by_text: dict[str, str] = {}
    for el in doc.get("body", {}).get("content", []):
        para = el.get("paragraph")
        if not para:
            continue
        style = para.get("paragraphStyle", {})
        hid = style.get("headingId")
        if not (style.get("namedStyleType", "").startswith("HEADING_") and hid):
            continue
        text = "".join(
            e.get("textRun", {}).get("content", "")
            for e in para.get("elements", [])
        ).strip()
        if text:
            heading_id_by_text.setdefault(text, hid)
    headings = list(heading_id_by_text.items())

    def _resolve(link_text: str) -> str | None:
        if link_text in heading_id_by_text:
            return heading_id_by_text[link_text]
        # Heading carries a parenthetical suffix the link omits (e.g. a
        # "(draft …)" marker). The " (" boundary avoids matching a longer
        # sibling name that merely shares a prefix.
        prefix = link_text + " ("
        hits = [hid for text, hid in headings if text.startswith(prefix)]
        return hits[0] if len(hits) == 1 else None

    requests: list[dict] = []
    for el in doc.get("body", {}).get("content", []):
        para = el.get("paragraph")
        if not para:
            continue
        for e in para.get("elements", []):
            tr = e.get("textRun")
            if not tr:
                continue
            link = tr.get("textStyle", {}).get("link") or {}
            # Only internal-intent links (dangling bookmark or "#…" url) —
            # leave external https links and existing headingId links alone.
            if not (link.get("bookmarkId") or link.get("url", "").startswith("#")):
                continue
            hid = _resolve(tr.get("content", "").strip())
            if not hid:
                continue
            requests.append({"updateTextStyle": {
                "range": {"startIndex": e["startIndex"], "endIndex": e["endIndex"]},
                "textStyle": {"link": {"headingId": hid}},
                "fields": "link",
            }})

    if requests:
        _run_batches(docs, doc_id, requests)
    return len(requests)


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def export_bundle_to_drive(
    bundle_dir: str | Path,
    *,
    folder_name: str | None = None,
    parent_id: str | None = None,
    interactive: bool = True,
) -> dict:
    """Upload a local briefing-pack bundle to Drive. Returns a result dict
    with the export folder id, per-Doc/Sheet ids + links, and the raw-file
    ids in the markdown subfolder.

    `folder_name` defaults to `Meridian — <bundle dir name>`. `parent_id`
    optionally nests the export folder under an existing Drive folder; it
    defaults to the `MERIDIAN_DRIVE_PARENT_ID` env var. The parent may be a
    folder you created by hand — under `drive.file` the app can't *read* it
    but can *write into it* by ID, which is all we need.

    `interactive` is forwarded to `get_credentials`: leave True for a
    hand-run upload (may open a browser to re-auth); pass False for an
    unattended caller so a dead token raises `TokenUnusableError` rather
    than blocking on a consent prompt."""
    bundle_dir = Path(bundle_dir)
    if not bundle_dir.is_dir():
        raise NotADirectoryError(f"Not a bundle directory: {bundle_dir}")

    if parent_id is None:
        parent_id = os.environ.get("MERIDIAN_DRIVE_PARENT_ID") or None

    # All the network work runs under a bounded socket read timeout so a
    # stalled Google API response fails fast instead of hanging forever.
    with _bounded_socket_reads():
        creds = get_credentials(interactive=interactive)
        drive = build("drive", "v3", credentials=creds)
        docs = build("docs", "v1", credentials=creds)

        folder_name = folder_name or f"Meridian — {bundle_dir.name}"
        export_folder_id = _find_or_create_folder(drive, folder_name, parent_id)
        log.info("Export folder %r -> %s", folder_name, export_folder_id)

        results: dict = {
            "folder_id": export_folder_id,
            "folder_name": folder_name,
            "docs": {},
            "raw": {},
        }

        # 1. Convert the house-styled artefacts into native Google files.
        for fname, (target_mime, source_mime) in _CONVERT.items():
            p = bundle_dir / fname
            if not p.exists():
                continue
            name = p.stem  # drive name carries no extension
            file_id, link = _upsert(
                drive, p, name, export_folder_id,
                source_mime=source_mime, target_mime=target_mime,
            )
            entry = {"id": file_id, "link": link}
            if target_mime == GDOC_MIME:
                entry["anchors_minted"] = mint_heading_anchors(docs, file_id)
                entry["links_fixed"] = fix_internal_heading_links(docs, file_id)
                log.info("  %s -> Doc %s (%d anchors minted, %d internal links fixed)",
                         name, file_id, entry["anchors_minted"], entry["links_fixed"])
            else:
                log.info("  %s -> Sheet %s", name, file_id)
            results["docs"][name] = entry

        # 2. Markdown subfolder: the raw .md files + the .xlsx, verbatim.
        md_folder_id = _find_or_create_folder(
            drive, MARKDOWN_SUBFOLDER, export_folder_id,
        )
        results["markdown_folder_id"] = md_folder_id
        # Mirror the bundle's local markdown subfolder (same name) into Drive.
        local_md_dir = bundle_dir / MARKDOWN_SUBFOLDER
        if local_md_dir.is_dir():
            for p in sorted(local_md_dir.iterdir()):
                if p.is_file() and p.suffix.lower() in _RAW_SUFFIXES:
                    src_mime = XLSX_MIME if p.suffix.lower() == ".xlsx" else MD_MIME
                    file_id, _ = _upsert(
                        drive, p, p.name, md_folder_id, source_mime=src_mime,
                    )
                    results["raw"][p.name] = file_id
                    log.info("  raw %s -> file %s", p.name, file_id)

        return results


def main(argv: list[str] | None = None) -> None:
    import sys

    logging.basicConfig(level=logging.INFO, format="%(message)s")
    args = argv if argv is not None else sys.argv[1:]
    if not args:
        sys.exit(
            "usage: python -m briefing_pack.drive_export <bundle_dir> "
            "[folder_name]"
        )
    res = export_bundle_to_drive(
        args[0], folder_name=args[1] if len(args) > 1 else None,
    )
    print(f"\nExport folder: {res['folder_name']} ({res['folder_id']})")
    for name, d in res["docs"].items():
        extra = (f", {d['anchors_minted']} anchors"
                 if "anchors_minted" in d else "")
        print(f"  {name}: {d['link']}{extra}")
    print(f"  markdown subfolder: {len(res['raw'])} raw files")


if __name__ == "__main__":
    main()
