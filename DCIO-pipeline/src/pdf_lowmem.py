"""Keep pdfplumber from holding every parsed page of a long filing in memory.

pdfplumber caches each Page's parsed objects until the whole PDF is closed, so
a single loop over pdf.pages keeps every page resident: 5.3GB for a 457-page
master-trust filing (2026-11-03), which OOM-killed whole chunk-runner batches
at their 8GB cap. install() makes pdf.pages free the previously used page's
cache whenever a different page is touched. Page.close() only flushes caches --
a page that's used again just re-parses from the PDF -- so extracted output is
unchanged; only peak memory drops (to ~0.2GB for the same filing).
"""
from pdfplumber.pdf import PDF

_installed = False


class _OnePageCachedList(list):
    """pdf.pages, except touching a page flushes the last other page touched."""

    def __init__(self, pages, tracker=None):
        super().__init__(pages)
        self._tracker = tracker if tracker is not None else [None]

    def _touch(self, page):
        last = self._tracker[0]
        if last is not None and last is not page:
            last.close()
        self._tracker[0] = page
        return page

    def __getitem__(self, idx):
        item = super().__getitem__(idx)
        if isinstance(idx, slice):
            return _OnePageCachedList(item, self._tracker)
        return self._touch(item)

    def __iter__(self):
        for page in super().__iter__():
            yield self._touch(page)


def install() -> None:
    global _installed
    if _installed:
        return
    original = PDF.pages.fget

    def pages(self):
        cached = getattr(self, "_lowmem_pages", None)
        if cached is None:
            cached = self._lowmem_pages = _OnePageCachedList(original(self))
        return cached

    PDF.pages = property(pages)
    _installed = True
