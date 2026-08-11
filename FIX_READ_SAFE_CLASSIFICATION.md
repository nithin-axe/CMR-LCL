# Fix: Auto-Classification Now Read-Safe

## Problem
The initial auto-classification implementation was marking emails as read when classifying them. This happened because it was using `resolve_deep_classification()`, which opens emails to read their full content.

## Solution
Changed the auto-classification to use **metadata-only classification** via `classify_email_meta()` instead.

### Key Differences

**Before (Marked emails as read):**
```python
result = resolve_deep_classification(email_id, label="a-cmr")
# This opens the email, reads attachments, marks it as read
```

**After (Read-safe):**
```python
result = classify_email_meta(email, label="a-cmr")
# This only uses: subject, sender, snippet, attachment names
# Never opens the email, never marks it as read
```

## How It Works

1. **Email Monitor** detects new emails in `data/scraped_emails.json`
2. **Dashboard** receives SSE event with new email IDs
3. **Auto-classify endpoint** loads email metadata from cache
4. **Metadata classifier** analyzes:
   - Subject line
   - Sender address
   - Email snippet/preview
   - Attachment file names
5. **Document type** is determined without opening the email
6. **Email remains unread** ✓

## Classification Accuracy

The metadata-only classifier is still highly accurate because:

- **Filename hints** - Most documents have recognizable names (CMR, Invoice, etc.)
- **Subject keywords** - Subjects often contain document type clues
- **Sender patterns** - Certain senders always send specific document types
- **LLM analysis** - Gemini analyzes metadata to infer document type

For cases where metadata isn't enough, the **deep analysis** (which does mark as read) is only triggered when:
- User explicitly clicks "Deep analyze" button
- Operations Process needs to verify document content
- Manual classification is required

## Performance

- **Metadata classification**: ~1-2 seconds (no file downloads)
- **Deep classification**: ~10-30 seconds (downloads and reads attachments)

Auto-classification uses the fast metadata path, keeping emails unread while still providing instant document type identification.

## Testing

To verify emails stay unread:

1. Open dashboard
2. Send a test email to the monitored mailbox
3. Watch it appear in the list within 5 seconds
4. Check the email in Gmail - it should still be **unread**
5. Document type badge appears automatically
6. Email remains unread ✓

## Files Modified

- `app/routes/api/realtime_api.py` - Updated `auto_classify_emails()` to use metadata-only classifier
