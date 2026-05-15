# Export templates

Drop any file into this directory and it will be copied verbatim
(filename and all) into every export folder produced by
`scrape.py --briefing-pack` and `scrape.py --periodic-run`.

The intended use is a per-cycle intro file (e.g.
`01_Read_Me_First.md`) that orients a journalist landing on the
bundle. The leading `01_` numeric prefix is a deliberate
sort-first trick — most file viewers will list it above
`02_Leads.md`, `03_Findings.md`, `04_Data.xlsx`, and `05_Groups.md`
so it's the first thing the reader sees.

Edit the file in place between exports if the framing for this
cycle needs to differ. The version control story is the same as
any other file in the repo — commit if you want the change
persistent, leave uncommitted if it's a one-off tweak.

This `README.md` itself is excluded from the copy step — it's
documentation for this directory, not template content for the
export bundle.
