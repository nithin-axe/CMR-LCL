# Document Type Classifier Prompt

Reference copy of the classification logic. The live prompts are built in
`app/services/parsing/document_classifier.py` so the canonical type list
(`DOCUMENT_TYPES`), the name→type `REFERENCE_MAP`, and the `_FEW_SHOT_EXAMPLES` stay the
single source of truth. Edit the Python, then keep this file in sync for humans.

## Two paths

- **META (read-safe)** — `classify_email_meta`. Uses only list-view metadata (sender,
  subject, snippet, attachment file names). Never opens a mail, so it can't change
  read/unread state. Backs the bulk **Find Document Types** button.
- **DEEP** — `classify_email` (Gmail API) / `classify_documents` (shared core, also used
  by the delegated-mailbox **Deep analyze** action). Reads the real document bytes.

## Decision priority (both paths)

1. **Keyword in the document** — an explicit type/title printed on the document (deep) or
   present in the attachment file name / subject (meta), mapped through the reference map.
2. Otherwise, infer from the email **subject + body/preview** text.
3. If still unclear, `Other`.

## Multiple pages / multiple documents (deep only)

A single PDF can hold several different documents across its pages (e.g. pages 1–2 a Bill
of Lading, page 3 a Packing List, page 4 an EUR1). The deep path examines every page and
returns **one entry per distinct document type**, each with its page range.

## Canonical document types

From the operator reference table (right-hand column), plus the four extra types from the
row-by-row corrections (`T1 DOC`, `OR FILES`, `TRANSFER`, `EUR AND PHYTO`):

Final master bill of lading · Commercial invoice · Phytosanitary certificate ·
EUR1 certificate · Delivery order · MRN · Chedpp · Inspection report · Arrival notice ·
Cmr · Invoice Declration · Custome Import Doc · custome duties And Vat · Cargo Manifest ·
Certificate Of Inspection (COI) · Packing List · Certificate Of Orgin · No DOC ·
Release House BL · Draft HBL · final Value Declration · Release Master Bill · Draft MBL ·
T1 DOC · OR FILES · TRANSFER · EUR AND PHYTO · Other

## Reference name → type map (excerpt)

NCTS release → MRN · A release has been transferred → TRANSFER · M1 → Custome Import Doc ·
Amount Sheet → custome duties And Vat · Pin Sheet → Delivery order · HBL → Release House BL ·
Commercial invoice agricola → Invoice Declration · C.O.I → Certificate Of Inspection (COI) ·
PL / Packing List → Packing List · Factura / Final account → Commercial invoice ·
Balance Payment → final Value Declration · MEDEDELING STATUS VOORAANMELDING / DHL / Split List → Other ·
NCTS5_DEPARTURE write-off → T1 DOC.

## Output

- META: `{"type": "<exact value>", "confidence": 0.0, "reason": "<short>"}`
- DEEP attachment: `{"documents": [{"type": "<exact value>", "pages": "1-2", "confidence": 0.0, "reason": "<short>"}]}`
