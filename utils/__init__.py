"""Shared helpers. A regular package (not a namespace package) so an
installed third-party ``utils`` distribution cannot shadow it, which would
silently drop config.py to its fallback log redactor."""
