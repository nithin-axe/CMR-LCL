# Multi-Attachment Extraction for a-cmr Labeled Emails

## Current Behavior

When an email in the `a-cmr` label has **multiple attachments**, the system now extracts **ALL of them**:

### Classification Logic

**File: `app/services/parsing/document_classifier.py`**
**Function: `classify_documents_cmr()`**

1. **First attachment** → Classified as **"Cmr"**
2. **All other attachments** → Classified as **"Other"**

### Example

Email with 3 attachments:
```
Attachment 1: CMR_Document.pdf     → Type: "Cmr"
Attachment 2: Packing_List.pdf     → Type: "Other"
Attachment 3: Invoice.pdf          → Type: "Other"
```

Result in Operations Process:
```
doc_types: ["Cmr", "Other", "Other"]
doc_attachment_indices: [0, 1, 2]
```

## How It Works

### Step 1: Identify CMR Attachment
The system checks each attachment to find which one is the CMR:

```python
# Check filename hints first
cmr_index = None
for i, att in enumerate(attachments or []):
    if filename_hint(att.get("filename", "")) == "Cmr":
        cmr_index = i
        break

# Check PDF text content
if cmr_index is None:
    for i, att in enumerate(attachments or []):
        page_texts = _extract_pdf_page_texts(att.get("data_bytes"))
        text = " ".join(page_texts).lower()
        is_cmr_text = bool(re.search(r"\bcmr\b|vrachtbrief|...", text))
        if is_cmr_text and not is_status_update:
            cmr_index = i
            break

# Default to first attachment
if cmr_index is None:
    cmr_index = 0
```

### Step 2: Classify All Attachments
```python
for i, att in enumerate(attachments or []):
    if i == cmr_index:
        type_override = "Cmr"
    else:
        type_override = "Other"
    # Extract containers from this attachment
    # Add to documents list
```

### Step 3: Extract Containers from Each
Each attachment gets its own container extraction:

```python
for i, att in enumerate(attachments or []):
    data_bytes = att.get("data_bytes")
    page_texts = _extract_pdf_page_texts(data_bytes)
    containers = find_container_numbers(" ".join(page_texts))
    # Store with attachment_index = i
```

## Operations Process Integration

### In `/operations/extract` endpoint:

```python
attachment_docs = [
    d for d in rec.get("documents", [])
    if d.get("source") == "attachment" and d.get("type") != "No DOC"
]

jobs.append({
    "message_id": mid,
    "containers": rec.get("containers") or [],
    "doc_types": [d.get("type") for d in attachment_docs],
    "doc_attachment_indices": [d.get("attachment_index") for d in attachment_docs],
})
```

### Result in Dashboard:

Each document appears as a separate row:
- **Cmr** document with its containers
- **Other** document(s) with their containers

### Upload Process:

Each document is uploaded separately:
1. Fetch Cmr document bytes
2. Verify against Shypple
3. Upload if needed
4. Fetch first "Other" document bytes
5. Verify against Shypple
6. Upload if needed
7. Fetch second "Other" document bytes
8. ... and so on

## Key Features

✅ **All attachments extracted** - Not just the CMR
✅ **Separate upload per attachment** - Each file uploaded individually
✅ **Container extraction per attachment** - Each document's containers identified
✅ **Attachment index tracking** - Disambiguates same-typed attachments
✅ **No silent drops** - "Other" documents are kept and uploaded

## Example Workflow

### Email arrives with 3 attachments:
```
CMR_20260729.pdf (CMR document)
Packing_List.pdf (Packing list)
Invoice_20260729.pdf (Invoice)
```

### Auto-classification:
```
doc_types: ["Cmr", "Other", "Other"]
containers: ["TEMU9681744", "HLBU6079748"]
```

### Operations Process shows:
```
Document 1: Cmr
  - Containers: TEMU9681744
  - Status: Verify on Shypple

Document 2: Other (Packing_List.pdf)
  - Containers: (extracted from content)
  - Status: Verify on Shypple

Document 3: Other (Invoice_20260729.pdf)
  - Containers: (extracted from content)
  - Status: Verify on Shypple
```

### Upload:
- All 3 documents uploaded to Shypple
- Each with its own type and containers
- Each verified before upload

## Configuration

**File: `app/services/parsing/document_classifier.py`**

To change the classification logic, modify:

```python
# Line ~1000: classify_documents_cmr()
# Change which attachment gets "Cmr" type
cmr_index = 0  # Currently: first attachment

# Change "Other" type for non-CMR attachments
type_override = "Other"  # Currently: "Other"
```

## Testing

1. Send email with 2+ attachments to a-cmr label
2. Open dashboard
3. Email appears with auto-classification
4. Click "Process"
5. Verify all attachments shown in review
6. Confirm and upload
7. All documents should upload to Shypple

## Troubleshooting

**Only CMR document showing?**
- Check if other attachments are being filtered out
- Verify `doc_types` includes all attachments
- Check `doc_attachment_indices` length matches `doc_types`

**"Other" documents not uploading?**
- Verify they're in the `needs_upload` list
- Check Shypple form accepts "Other" type
- Check container extraction is working

**Wrong attachment classified as CMR?**
- Check filename hints (CMR keyword detection)
- Check PDF text extraction (looking for "CMR" text)
- Manually override in Operations Process review
