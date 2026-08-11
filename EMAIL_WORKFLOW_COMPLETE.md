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
- **Email marked as READ** ✓
- Status: `up_to_date`
- Ready to move to "Processed - India filing" label

### Stage 4b: Documents Need Upload
- Some documents missing or different from Shypple
- User confirms upload in Shypple browser
- Documents uploaded successfully
- **Email gets YELLOW STAR** ✓
- **Email marked as READ** ✓
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
- No matching shipment on Shypple for any container
- **Email marked as UNREAD** (needs manual attention)
- Email moved to `a-cmr-no-record` label
- Status: `no_match`

## Key Rules

1. **Before Upload**: Email stays **UNREAD**
   - During classification
   - During verification
   - During upload preparation
   - Reason: Operator can see "unread" = "still needs attention"

2. **After Successful Upload**: Email gets **YELLOW STAR** + **MARKED READ**
   - Indicates: "Done - ready to move to Processed label"
   - Matches existing "Process yellow-starred" workflow

3. **Special Cases**: Different stars for different outcomes
   - **Blue star**: No organization (forwarded, not uploaded)
   - **Purple star**: Cancelled/deleted shipment
   - **No star**: No record found (marked unread instead)

## Implementation Details

### Functions in `shypple_process.py`

**`_star_source_email(job, color)`**
- Calls Gmail automation to set colored star
- Records `job["star_status"]` ("done"/"failed")
- Records `job["star_color"]` for dashboard display

**`_mark_source_email_read(job)`**
- Calls Gmail automation to mark email as read
- Records `job["read_status"]` ("done"/"failed")
- Only called AFTER successful upload

### Calling Points

**Line ~1100** (Documents verified, no upload needed):
```python
_star_source_email(job, "yellow")
_mark_source_email_read(job)
set_job_status(job, "up_to_date", ...)
```

**Line ~1200+** (Documents uploaded successfully):
```python
_star_source_email(job, "yellow")
_mark_source_email_read(job)
set_job_status(job, "uploaded", ...)
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
  - [ ] Email is marked as READ in Gmail
  - [ ] Dashboard shows "uploaded" or "up_to_date" status

## Troubleshooting

**Email marked as read too early?**
- Check that auto-classification uses `classify_email_meta()` (metadata-only)
- Verify `_mark_source_email_read()` is only called after upload

**Yellow star not appearing?**
- Check Gmail automation is running (`scripts/open_gmail.py`)
- Verify `_star_source_email()` is being called
- Check Flask logs for errors

**Email still unread after upload?**
- Verify `_mark_source_email_read()` was called
- Check if email is from delegated mailbox (starts with "pw_")
- Check Gmail automation logs for errors
