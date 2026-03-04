from __future__ import annotations

import html
import re


_SCRIPT_STYLE_RE = re.compile(r"(?is)<(script|style|noscript|svg|canvas)[^>]*>.*?</\1>")
_COMMENT_RE = re.compile(r"(?is)<!--.*?-->")
_TAG_RE = re.compile(r"(?is)<[^>]+>")
_WS_RE = re.compile(r"[ \t\f\v]+")
_MULTI_NL_RE = re.compile(r"\n{3,}")


def extract_html_text(content: bytes) -> str:
    if not content:
        return ""

    text = content.decode("utf-8", errors="ignore")
    if not text:
        text = content.decode("latin-1", errors="ignore")

    text = _SCRIPT_STYLE_RE.sub(" ", text)
    text = _COMMENT_RE.sub(" ", text)
    text = _TAG_RE.sub(" ", text)
    text = html.unescape(text)
    text = text.replace("\r", "\n")
    text = _WS_RE.sub(" ", text)
    text = _MULTI_NL_RE.sub("\n\n", text)
    return text.strip()
