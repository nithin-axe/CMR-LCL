# Email Workflow: Read/Unread and Starring

## Complete Email Lifecycle

### Stage 1: Email Arrives
- Email appears in Gmail inbox
- **Status**: Unread (default)
- **Star**: None

### Stage 2: Auto-Classification (Automatic)
- Dashboard detects new email
- Metadata-only classification runs (subject, sender, snippet, attachment names)
- **Email stays UNREAD** ✓ (no email opened)
- **Star**: None
- Document type badge appears on email row

### Stage 3: Manual Review & Verification (Operations Process)
- User selects email(s) and clicks "Process"
- Dashboard shows classification results for review
- User confirms and clicks "Looks good - open Shypple"
- Shypple browser opens and verifies containers/documents
- **Email stays UNREAD** ✓ (verification doesn't open email)
- **Star**: None

### Stage 4a: Documents Already Verified (No Upload Needed)
- All documents found on Shypple and verified as matching
- **Email gets YELLOW STAR** ✓
- **Email marked as READ** ✓ (processing finished successfully)
- Status: `up_to_date`
- Ready to move to "Processed - India filing" label

### Stage 4b: Documents Need Upload
- Some documents missing or different from Shypple
- User confirms upload in Shypple browser
- Documents uploaded successfully
- **Email gets YELLOW STAR** ✓
- **Email marked as READ** ✓ (processing finished successfully)
- Status: `uploaded`
- Ready to move to "Processed - India filing" label

### Stage 4c: Special Cases

**No Organization Set:**
- Shipment found but organization is empty
- **Email gets BLUE STAR** (immediate)
- Email forwarded to `nl.importsea@shypple.com`
- Email moved to `a-release-orders` label
- **Email stays UNREAD** (not processed, forwarded for handling)

**Shipment Cancelled/Deleted:**
- Shipment status is cancelled or deleted
- **Email gets PURPLE STAR**
- **Email stays UNREAD** (not processed)
- Status: `cancelled_or_deleted`

**No Record Found:**
- The document's own extracted container number came back with a genuinely empty
  results table (not just an org/ETA mismatch) - no fallback to the mail's SF number
  or subject-extracted container is attempted (a real example proved that fallback
  could point to a completely unrelated shipment whose SF reference just happened to
  be in the same email)
- Container numbers are searched with all spaces/dashes stripped (e.g. "MSCU 123456-7" -> "MSCU1234567") before being typed into Shypple's search box
- **Email gets PURPLE STAR**
- **Email marked as UNREAD** (needs manual attention)
- No label move - the email stays in its current label
- Status: `no_match`

## Key Rules

1. **Before Upload**: Email stays **UNREAD**
   - During classification
   - During verification
   - During upload preparation
   - Reason: Operator can see "unread" = "still needs attention"

2. **When finished, every outcome gets a YELLOW STAR** - which read state depends on
   whether processing actually succeeded:
   - **Processing finished successfully** (document uploaded, OR nothing needed
     uploading because it was already identical/up to date) -> marked **READ**. For
     both CMR (`up_to_date`, `uploaded`) and LCL (`lcl_done`) alike.
   - **Something needs manual attention** (no document found, no date found,
     shipment fails verification, etc.) -> marked **UNREAD**, so it stays visibly
     flagged for the operator to look at rather than disappearing into the read
     state.
   - Both pipelines agree on this split. Since `_mark_source_email_read` and
     `_mark_source_email_unread` both write into the same `job["read_status"]`/
     `job["read_error"]` fields, each also sets `job["read_action"]` ("read"/"unread")
     so the dashboard's per-job detail line shows the right wording.

3. **Special Cases**: Different stars for different outcomes
   - **Blue star**: No organization (forwarded, not uploaded)
   - **Purple star**: Cancelled/deleted shipment, OR no record found for any
     extracted container (both leave the email unread)

## Implementation Details

### Functions in `shypple_process.py`

**`_star_source_email(job, color)`**
- Calls Gmail automation to set colored star
- Records `job["star_status"]` ("done"/"failed")
- Records `job["star_color"]` for dashboard display

**`_mark_source_email_unread(job)`**
- Calls Gmail automation to mark email as unread
- Records `job["read_status"]` ("done"/"failed") and `job["read_action"] = "unread"`
- Called for error/needs-manual-attention outcomes (no document found, no date
  found, shipment fails verification, etc.), for both the CMR and LCL
  Arrivals/Release pipelines

**`_mark_source_email_read(job)`**
- Calls Gmail automation to mark email as read
- Records `job["read_status"]` ("done"/"failed") and `job["read_action"] = "read"`
- Called whenever processing finishes successfully - a document was uploaded, OR
  nothing needed uploading because it was already identical - for both pipelines

**`verify_existing_document_before_upload(page, job, doc_type, mail_bytes, mail_mime, containers, attachment_index=None)`**
- LCL-only. Before uploading an Arrival Notice/Delivery Order, scrapes
  `#shipment-document-table`, and if a document matching the same doc_type AND
  container is already there: downloads it, saves a local copy to the system
  Downloads folder, and compares its content against the email's own copy (same
  Gemini-based `compare_document_versions` mechanism CMR uses). Returns
  `{"exists", "same", "differences", "reason"}` - `handle_arrival_notice`/
  `handle_delivery_order` skip the upload (read + yellow star) when `same` is true,
  or pause on `awaiting_lcl_document_diff_confirmation` (showing the field-by-field
  `differences` in the Operations Process panel) and wait for the operator before
  uploading when `exists` is true but `same` is false.

### Calling Points

**`_verify_and_upload_documents`** (Documents verified, no upload needed):
```python
_star_source_email(job, "yellow")
_mark_source_email_read(job)
set_job_status(job, "up_to_date", ...)
```

**`_verify_and_upload_documents`** (Documents uploaded successfully):
```python
_star_source_email(job, "yellow")
_mark_source_email_read(job)
set_job_status(job, "uploaded", ...)
```

**`handle_arrival_notice` / `handle_delivery_order`** (existing document identical - no upload needed):
```python
_star_source_email(job, "yellow")
_mark_source_email_read(job)
set_job_status(job, "lcl_done", ...)
```

**`handle_arrival_notice` / `handle_delivery_order`** (document uploaded successfully):
```python
_star_source_email(job, "yellow")
_mark_source_email_read(job)
set_job_status(job, "lcl_done", ...)
```

**Line ~900** (No organization):
```python
_star_source_email(job, "blue")  # Immediate, before confirmation
```

**Line ~950** (Cancelled/deleted):
```python
_star_source_email(job, "purple")
```

## Testing Checklist

- [ ] Send test email to monitored mailbox
- [ ] Email appears in dashboard (unread)
- [ ] Document type badge appears (auto-classification)
- [ ] Email still unread in Gmail
- [ ] Select email and click "Process"
- [ ] Review shows classification results
- [ ] Click "Looks good - open Shypple"
- [ ] Shypple verifies documents
- [ ] After verification completes:
  - [ ] Email has YELLOW STAR in Gmail
  - [ ] Email is READ whether a document was actually uploaded or nothing needed
        uploading (already identical) - both are successful outcomes
  - [ ] Dashboard shows "uploaded"/"lcl_done" (document uploaded) or
        "up_to_date"/"lcl_done" (nothing needed uploading) status
  - [ ] Operations Process panel shows "Marked source email as read"
  - [ ] An error/needs-attention outcome (no document found, no date found,
        verification failed) leaves the email UNREAD instead
- [ ] LCL only: re-process a mail whose Arrival Notice/Delivery order is already on
      Shypple with DIFFERENT content than the email's copy -> confirm the
      `awaiting_lcl_document_diff_confirmation` banner shows the field-by-field diff
      table before allowing the (re-)upload

## Troubleshooting

**Email opened/marked read during classification?**
- Check that auto-classification uses `classify_email_meta()` (metadata-only) -
  `_mark_source_email_read`/`_mark_source_email_unread` only ever run after
  upload/verification, never before

**Yellow star not appearing?**
- Check Gmail automation is running (`scripts/open_gmail.py`)
- Verify `_star_source_email()` is being called
- Check Flask logs for errors

**Email's read/unread state (or the panel's "Marked source email as ..." line) looks wrong?**
- Confirm which outcome actually happened: a successful finish (document uploaded,
  or nothing needed uploading) should be READ; an error/needs-attention outcome
  should be UNREAD - check the job's log lines for "Marked the source email as
  read"/"...as unread", or `job["read_action"]`
- Check if email is from delegated mailbox (starts with "pw_")
- Check Gmail automation logs for errors

**LCL document-diff confirmation never shows, or a re-upload happens with no chance to review?**
- Confirm `verify_existing_document_before_upload` is passing
  `label="lcl-arrivals---release"` through to `_compare_document_versions_remote` -
  without it, the compare's email-side fetch can resolve under the wrong label
- Check the job's log line "... already on Shypple but DIFFERS from the email's
  document" landed before the upload, and that `job["document_diff_preview"]` is
  populated
