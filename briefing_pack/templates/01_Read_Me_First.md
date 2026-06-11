# How to use this data pack

## What it is

The files in this pack provide findings from analysing import/export reports for China/Europe trade using three different data sources: GACC (China's customs agency), Eurostat (EU-27) and HMRC (UK).

The system (which I've called Meridian for obscure reasons) routinely scrapes those reports, inserts them into a database, and attempts to correlate the different outputs from the China side and the Europe side.

The data is restricted to a set of 'harmonised system' (HS) product categories. We can configure this list. Since the exports are already huge I'd suggest we remove stuff we won't ever care about rather than just add more and more. But if you don't mind the noise, there's no technical bar to adding more.

Meridian spits out an export pack after receiving substantive new data from one of the three sources and cross-referencing it with existing data.

## Where to start, by time available

- **5 minutes** — open **03_Findings** and read the "Top movers this cycle" list and "Tier 1 — what's new this cycle". Those are the headline shifts since the last pack.
- **15 minutes** — then skim the "Top leads" section of **02_Leads** for story angles, and look up your beat's categories in 03_Findings Tier 2.
- **Digging in** — open **04_Data** (a multi-tab spreadsheet, summary tab first) to sort and filter everything, and use 03_Findings Tier 3 for the per-category detail behind any number.

## Before you quote a number

Four habits that will keep you out of trouble:

1. **Check the category in 05_Groups** so you know what the figure does and doesn't include — some categories are a single product code, others are broad bundles.
2. **Quote the 12-month figure, not "latest month".** The latest-month number is a direction hint; it swings wildly on lumpy categories (aircraft, ships) and the document will warn you when a swing is too extreme to quote.
3. **Respect the flags.** "⚠ low base" means the percentage rests on a small total — quote the absolute € amounts instead. 🔴 means the signal hasn't held up over recent months — verify before quoting. (Each document opens with a "Reading the numbers" key that explains all the conventions.)
4. **Keep the `finding/N` token** that ends every claim. It's the citation for that exact number — send it back to me and I can produce the full audit trail (source URLs, exchange rates, the arithmetic) for anything you want to publish.

## The files

### 03_Findings
The direct output from Meridian, showing data for each HS category — pure data, no AI involved in producing it. Three layers: what's new this cycle, the current state of play for every category, then full detail. This is the file to cite numbers from.

### 02_Leads
An LLM's interpretation of the Findings file: a "top leads" section with what it thinks is interesting, then remarks per category about what might be worth exploring. Useful for story angles — but where the other files are all pure data, this one is produced by GenAI and subject to the vagaries of that technology (the LLM is a local model running on my laptop, so it's not 'top of the range'). Treat it as a tip-sheet, not a source; every number it cites is machine-checked against the Findings, but verify against 03_Findings before quoting.

### 04_Data
All the data from the Findings, in a data-journalist friendly format. Multi-tab spreadsheet, summary page first — the fastest way to sort and filter by size, change, or stability.

### 05_Groups
A reference document explaining each HS product category that appears in the Findings file — what HS codes it covers, what the top contributing sub-codes are, and which adjacent categories sit nearby. Read a category's entry once before quoting its figure so you know exactly what is and isn't in it.

## Other ways to interrogate the data packs using LLMs

You'll see a subfolder "Markdown versions for use with LLMs etc" in the pack. This contains versions of the above files formatted in an LLM friendly way, plus the spreadsheet which is identical to 04_Data. These are the files you should point at other tools.

### Gemini
- Open the Data file in Google Sheets and ask Gemini to build some charts and notes about the tabs that look most interesting to you.

### NotebookLM
- Go to NotebookLM and import the Findings.md file from the subfolder as a source. Ask NotebookLM questions, use the options on the right to create tables and infographics to illustrate its observations.

## More detail about the tool

- Repo documentation: https://github.com/hoyla/meridian/blob/main/README.md
- How it works, and how to interpret the findings: https://github.com/hoyla/meridian/blob/main/docs/methodology.md
- Glossary of terms: https://github.com/hoyla/meridian/blob/main/docs/glossary.md
