# service/app/integrations/drive_client.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
import re
from datetime import datetime

# Requires:
#   google-auth
#   google-api-python-client
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

import io


@dataclass(frozen=True)
class DriveItem:
    provider: str  # 'gdrive'
    provider_id: str
    name: str
    mime: str
    is_folder: bool
    parent_provider_id: Optional[str]
    modified_at: Optional[datetime]
    size_bytes: Optional[int]
    path: str


def _extract_drive_id(url: str) -> Optional[str]:
    """
    Extract Google Drive file/folder id from common URL formats.
    """
    url = (url or "").strip()
    if not url:
        return None

    # /folders/<id>
    m = re.search(r"/folders/([a-zA-Z0-9_-]+)", url)
    if m:
        return m.group(1)

    # /file/d/<id>
    m = re.search(r"/file/d/([a-zA-Z0-9_-]+)", url)
    if m:
        return m.group(1)

    # id=<id>
    m = re.search(r"[?&]id=([a-zA-Z0-9_-]+)", url)
    if m:
        return m.group(1)

    return None


class DriveClient:
    def __init__(self, sa_json_path: str):
        self.sa_json_path = (sa_json_path or "").strip()
        self._svc = None

    def enabled(self) -> bool:
        return bool(self.sa_json_path)

    def _service(self):
        if self._svc is not None:
            return self._svc
        creds = Credentials.from_service_account_file(
            self.sa_json_path,
            scopes=["https://www.googleapis.com/auth/drive.readonly"],
        )
        self._svc = build("drive", "v3", credentials=creds, cache_discovery=False)
        return self._svc

    def resolve_root(self, url: str) -> Optional[str]:
        return _extract_drive_id(url)

    def _get_file(self, file_id: str) -> Dict:
        svc = self._service()
        return (
            svc.files()
            .get(fileId=file_id, fields="id,name,mimeType,modifiedTime,size,parents")
            .execute()
        )

    def list_recursive(self, root_id: str, root_name: str = "", max_items: int = 5000) -> List[DriveItem]:
        """
        Recursively traverse folders. If root_id is a file, returns just that file.
        """
        svc = self._service()

        root_meta = self._get_file(root_id)
        is_folder = root_meta.get("mimeType") == "application/vnd.google-apps.folder"

        if not is_folder:
            return [self._meta_to_item(root_meta, parent_id=None, path=root_meta.get("name") or root_id)]

        out: List[DriveItem] = []
        stack: List[Tuple[str, str]] = [(root_id, root_name or (root_meta.get("name") or root_id))]

        while stack and len(out) < max_items:
            folder_id, folder_path = stack.pop()

            page_token = None
            while True:
                resp = (
                    svc.files()
                    .list(
                        q=f"'{folder_id}' in parents and trashed=false",
                        fields="nextPageToken, files(id,name,mimeType,modifiedTime,size,parents)",
                        pageSize=1000,
                        pageToken=page_token,
                    )
                    .execute()
                )
                files = resp.get("files") or []
                for f in files:
                    item = self._meta_to_item(f, parent_id=folder_id, path=f"{folder_path}/{f.get('name')}")
                    out.append(item)
                    if item.is_folder:
                        stack.append((item.provider_id, item.path))
                    if len(out) >= max_items:
                        break

                page_token = resp.get("nextPageToken")
                if not page_token or len(out) >= max_items:
                    break

        return out

    def download(self, file_id: str, max_mb: int = 40) -> bytes:
        """
        Download Drive file content as bytes. For Google Docs/Sheets/Slides you need export;
        we skip those in extractor router (safe).
        """
        svc = self._service()
        req = svc.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, req, chunksize=1024 * 1024)
        done = False
        max_bytes = max_mb * 1024 * 1024

        while not done:
            status, done = downloader.next_chunk()
            if fh.tell() > max_bytes:
                raise RuntimeError("Drive file too large")

        return fh.getvalue()

    @staticmethod
    def _meta_to_item(meta: Dict, parent_id: Optional[str], path: str) -> DriveItem:
        mime = meta.get("mimeType") or ""
        is_folder = mime == "application/vnd.google-apps.folder"
        mt = meta.get("modifiedTime")
        modified_at = None
        if mt:
            try:
                modified_at = datetime.fromisoformat(mt.replace("Z", "+00:00"))
            except Exception:
                modified_at = None

        size_bytes = None
        if meta.get("size") is not None:
            try:
                size_bytes = int(meta["size"])
            except Exception:
                size_bytes = None

        return DriveItem(
            provider="gdrive",
            provider_id=meta.get("id") or "",
            name=meta.get("name") or "",
            mime=mime,
            is_folder=is_folder,
            parent_provider_id=parent_id,
            modified_at=modified_at,
            size_bytes=size_bytes,
            path=path,
        )