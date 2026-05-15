# How to use this data pack

## What it is

The four files in this pack provide findings from analysing import/export reports for China/Europe trade using three different data sources: GACC, Eurostat and HMRC. 

The system (which I've called Meridian for obscure reasons) routinely scrapes those reports, inserts them into a database, and attempts to correlate the different outputs from the China side and the Europe side.

The data is restricted to a set of harmonised system (HS) product categories. We can configure this list. Since the exports are already huge I'd suggest we remove stuff we won't ever care about rather than just add more and more. But if you don't mind the noise, there's no technical bar to adding more.

Meridian spits out an export pack after receiving substantive new data from one of the three sources and cross-referencing it with existing data. 

The export pack consists of four files:

### Findings
This is the direct output from Meridian, showing data for each HS category. It also includes a section of "big movers" - the categories with the most noteworthy shifts since the last data update.

### Groups
A reference document explaining each HS product category that appears in the Findings file — what HS codes it covers, what the top contributing sub-codes are, and which adjacent categories sit nearby. Read this once before quoting a category figure so you know exactly what is and isn't in it.

### Data
This is all the data from the Findings, in a data-journalist friendly format. It's a multi-tab spreadsheet, with a summary page first.

### Leads
This is actually where I suggest you start. Where the above things should be pure data, this is an LLM's interpretation of the Findings file contents. There's a "top leads" section with what the LLM thinks is interesting, followed by a list of each of the categories with some remarks about what might be worth exploring from the findings. 

The LLM is a local model running on my laptop so it's not very powerful. 

## Other ways to interrogate the data packs using LLMs

### Gemini
- Open the Data file in Google Sheets and ask Gemini to build some charts and notes about the tabs that look most interesting to you.

### NotebookLM
- Go to NotebookLM and import the Findings file as a source. Ask NotebookLM questions, use the options on the right to create tables and infographics to illustrate its observations. 

## More detail about the tool

- Repo documentation: https://github.com/hoyla/meridian/blob/main/README.md
- How it works, and how to interpret the findings: https://github.com/hoyla/meridian/blob/main/docs/methodology.md
Glossary of terms: https://github.com/hoyla/meridian/blob/main/docs/glossary.md
