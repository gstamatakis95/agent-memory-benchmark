# Notices for the prompt files

The prompt templates under `prompts/<name>/v<N>/` are Engram's prompts (PLAN.md section 6). The following ones contain
text adapted from the prompts of Hindsight (`engine/retain/fact_extraction.py`, `engine/consolidation/prompts.py`,
`engine/consolidation/consolidator.py`, `engine/reflect/prompts.py`). Every such `template.txt` repeats the line
"Adapted from Hindsight, MIT License, Copyright (c) 2025 Vectorize AI, Inc." and the full MIT notice at the top of the
file, in `#! ` header lines that the loader strips before the prompt is rendered.

- `extract/v1`: selectivity, format, coreference, classification, temporal and entity sections (concise mode).
- `consolidate_route/v1`: default mission, mission-priority sentence, language rule, "prefer update over create",
  "later statements supersede".
- `consolidate_write/v1`: rules 4 and 5.
- `dedup_adjudicate/v1`: the near-duplicate reconciliation prompt (verified excerpt).
- `reflect/v1`: CRITICAL opening, default mission, "only use information from tool results", hierarchy, temporal
  rule, directive block and reminder, output rules, language rule.
- `reflect_structured/v1`: opening sentence.
- `page/v1`: integration sentence, preserve/merge/examples rule, "absence is not contradiction", refutation
  threshold, retirement rule.

`summarize/v1` and `page_full/v1` are Engram's own text and carry no Hindsight material.

The MIT notice of Hindsight:

```
MIT License

Copyright (c) 2025 Vectorize AI, Inc.

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
documentation files (the "Software"), to deal in the Software without restriction, including without limitation the
rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit
persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the
Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
```
