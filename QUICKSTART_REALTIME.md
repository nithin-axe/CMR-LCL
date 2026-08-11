# Quick Start: Live Email Updates & Auto-Classification

## What's New

Your dashboard now has two powerful features:

### 1. Live Email Updates
- **Before**: Email list refreshed every 10 seconds
- **Now**: New emails appear instantly (within 5 seconds)
- **How**: Server-Sent Events (SSE) stream from backend to frontend

### 2. Automatic Document Type Detection
- **Before**: Manual "Classify" button needed for each email
- **Now**: Document types identified automatically when emails arrive
- **How**: Background service classifies new emails without marking them read

## How It Works

### For Users
1. Open the dashboard - monitoring starts automatically
2. New emails arrive in Gmail
3. Email appears in your inbox within 5 seconds
4. Document type badge appears automatically within 10 seconds
5. No manual action needed!

### For Developers

**Starting the app:**
```bash
python run.py
```

The monitoring service starts automatically. You'll see in the logs:
```
[INFO] Email monitoring started
[RealTime] Email monitoring started
```

**Checking monitoring status:**
```bash
curl http://localhost:40000/api/emails/monitor/status
```

Response:
```json
{
  "success": true,
  "monitoring": true,
  "check_interval": 5
}
```

**Manually triggering classification:**
```bash
curl -X POST http://localhost:40000/api/emails/auto_classify \
  -H "Content-Type: application/json" \
  -d '{"email_ids": ["email_id_1", "email_id_2"]}'
```

## Configuration

### Adjust monitoring frequency
Edit `app/services/common/email_monitor.py`:

```python
self.check_interval = 5  # Change to 3 for faster detection, 10 for slower
```

Lower values = faster detection but higher CPU usage
Higher values = slower detection but lower CPU usage

## Monitoring & Debugging

### Browser Console
Open browser DevTools (F12) and look for `[RealTime]` messages:

```
[RealTime] Email monitoring started
[RealTime] New emails arrived: 2
[RealTime] Auto-classified 2 email(s)
```

### Flask Logs
Check the terminal running `python run.py`:

```
[INFO] Email monitoring started
[INFO] Detected 2 new email(s)
[INFO] Auto-classified 2 email(s)
```

### Network Tab
In DevTools Network tab, look for:
- `stream?client_id=...` - SSE connection (should show "pending")
- `auto_classify` - Classification requests

## Troubleshooting

### Emails not appearing in real-time?

**Check 1: Is monitoring running?**
```bash
curl http://localhost:40000/api/emails/monitor/status
```
Should show `"monitoring": true`

**Check 2: Is the scraper updating emails?**
Check if `data/scraped_emails.json` is being updated:
```bash
ls -la data/scraped_emails.json
# Should show recent timestamp
```

**Check 3: Browser console errors?**
Open DevTools (F12) → Console tab
Look for red error messages starting with `[RealTime]`

### Classifications not appearing?

**Check 1: Are emails being classified?**
Look for `[RealTime] Auto-classified X email(s)` in console

**Check 2: Do emails have attachments?**
Body-only emails may not classify. Check the email has a PDF/document attached.

**Check 3: Is the classifier working?**
Test manually:
```bash
curl -X POST http://localhost:40000/api/emails/classify \
  -H "Content-Type: application/json" \
  -d '{"id": "email_id"}'
```

## Performance Tips

### For faster detection
- Reduce `check_interval` to 3 seconds
- Increase Playwright scraper frequency (if possible)

### For lower CPU usage
- Increase `check_interval` to 10 seconds
- Disable auto-classification if not needed

### For better scalability
- Consider Redis for multi-server deployments
- Use Celery for background classification tasks

## API Reference

### Real-time Endpoints

**Stream (SSE)**
```
GET /api/emails/stream?client_id=unique_id
```
Returns: Server-Sent Events stream

**Auto-classify**
```
POST /api/emails/auto_classify
Body: {"email_ids": ["id1", "id2"]}
```
Returns: Classifications with types and containers

**Monitor Status**
```
GET /api/emails/monitor/status
```
Returns: Monitoring status and check interval

**Start Monitoring**
```
POST /api/emails/monitor/start
```
Returns: Success message

**Stop Monitoring**
```
POST /api/emails/monitor/stop
```
Returns: Success message

## Next Steps

1. **Test it out** - Send a test email and watch it appear in real-time
2. **Monitor performance** - Check CPU/memory usage in your environment
3. **Adjust settings** - Tune `check_interval` for your needs
4. **Scale up** - Consider Redis integration for multi-server deployments

## Support

For issues or questions:
1. Check the browser console for `[RealTime]` errors
2. Check Flask logs for backend errors
3. Review `REALTIME_IMPLEMENTATION.md` for detailed architecture
4. Test individual endpoints with curl commands above

Enjoy your live dashboard! 🚀
