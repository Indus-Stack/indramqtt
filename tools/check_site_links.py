#!/usr/bin/env python3
"""Check every internal link and anchor of the built docs site.

Usage:
  python3 tools/check_site_links.py docs-site/dist

The script reads the HTML files of the build. It follows each internal link
(`a`, `img`, `script` and the `link` tags of style sheets and icons) and each
`#anchor`. It fetches nothing from the network and skips external URLs.

A link is broken when no file serves its target, or when the page exists and
has no element with the id of the anchor. Exit code: 0 when no link is broken,
1 when one is, 2 when the directory is missing.

The host serves `/_vercel/...` at run time. Those paths are not in the build,
so the script does not check them.
"""
import os
import re
import sys
from html.parser import HTMLParser
from urllib.parse import unquote, urldefrag, urljoin, urlparse

HOST_PREFIXES = ("/_vercel/",)
EXTERNAL = re.compile(r"^[a-z][a-z0-9+.-]*:", re.IGNORECASE)
LINK_RELS = {"stylesheet", "icon", "shortcut icon", "manifest"}


class Page(HTMLParser):
    """Collects the link targets and the ids of one HTML file."""

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.targets = []
        self.ids = set()

    def handle_starttag(self, tag, attrs):
        a = dict(attrs)
        if a.get("id"):
            self.ids.add(a["id"])
        if tag == "a" and a.get("name"):
            self.ids.add(a["name"])
        if tag == "a" and a.get("href") is not None:
            self.targets.append(a["href"])
        elif tag in ("img", "script") and a.get("src"):
            self.targets.append(a["src"])
        elif tag == "link" and a.get("href") and (a.get("rel") or "").lower() in LINK_RELS:
            self.targets.append(a["href"])


def page_url(dist, path):
    relative = os.path.relpath(path, dist).replace(os.sep, "/")
    if relative.endswith("index.html"):
        relative = relative[: -len("index.html")]
    return "/" + relative


def serving_file(dist, url_path):
    """Return the file that serves url_path, or None."""
    path = unquote(url_path)
    candidates = [path + "index.html"] if path.endswith("/") else [path, path + ".html", path + "/index.html"]
    for candidate in candidates:
        file = os.path.join(dist, candidate.lstrip("/"))
        if os.path.isfile(file):
            return file
    return None


def check(dist):
    """Return (pages, links checked, broken) for a build directory.

    `broken` is a list of (page URL, link as written, reason).
    """
    dist = os.path.abspath(dist)
    pages = {}
    for root, _, names in os.walk(dist):
        for name in names:
            if name.endswith(".html"):
                path = os.path.join(root, name)
                parser = Page()
                with open(path, encoding="utf-8", errors="replace") as handle:
                    parser.feed(handle.read())
                pages[path] = parser

    broken = []
    checked = 0
    for path, parser in sorted(pages.items()):
        here = page_url(dist, path)
        for href in parser.targets:
            href = href.strip()
            if not href or href.startswith(("//", "mailto:", "tel:", "javascript:", "data:")) or EXTERNAL.match(href):
                continue
            target, fragment = urldefrag(urljoin(here, href))
            url_path = urlparse(target).path or here
            if url_path.startswith(HOST_PREFIXES):
                continue
            checked += 1
            file = serving_file(dist, url_path)
            if file is None:
                broken.append((here, href, "no page or file at " + url_path))
            elif fragment and file in pages and unquote(fragment) not in pages[file].ids and fragment != "top":
                broken.append((here, href, "no element with id " + fragment + " on " + url_path))
    return len(pages), checked, broken


def main(argv):
    if len(argv) != 1:
        print(__doc__)
        return 2
    if not os.path.isdir(argv[0]):
        print("not a directory: " + argv[0])
        return 2
    pages, checked, broken = check(argv[0])
    print(f"pages: {pages}   internal links checked: {checked}   broken: {len(broken)}")
    for page, href, reason in broken:
        print(f"  {page}  ->  {href}   ({reason})")
    return 1 if broken else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
