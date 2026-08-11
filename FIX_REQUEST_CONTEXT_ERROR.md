# Fix: Request Context Error in SSE Stream

## Problem
The SSE stream endpoint was throwing a `RuntimeError: Working outside of request context` error. This happened because the generator function was trying to access Flask's `request` object after the response headers had already been sent.

## Root Cause
In Flask, the `request` context is only available during the initial request handling. Once a generator starts yielding data (streaming response), the request context is no longer available. Trying to access `request.args` inside the generator caused the error.

## Solution
Capture the `client_id` parameter **before** creating the generator function, while the request context is still active.

### Before (Broken)
```python
def email_stream():
    def generate():
        client_id = request.args.get("client_id", "default")  # ERROR: No request context!
        # ...
    return Response(generate(), ...)
```

### After (Fixed)
```python
def email_stream():
    # Capture client_id BEFORE creating the generator
    client_id = request.args.get("client_id", "default")  # OK: Request context is active
    
    def generate():
        # Use the captured client_id
        ws_service.subscribe(client_id)
        # ...
    return Response(generate(), ...)
```

## Key Changes
1. Moved `client_id = request.args.get(...)` outside the generator function
2. The generator now uses the captured `client_id` variable via closure
3. Request context is available when we need it (before streaming starts)

## Testing
The SSE stream should now work without errors:
1. Open browser DevTools → Network tab
2. Look for `stream?client_id=...` request
3. Should show status 200 with "pending" (streaming)
4. No more "Working outside of request context" errors

## Files Modified
- `app/routes/api/realtime_api.py` - Fixed `email_stream()` function
