# FastSQS Preset Example

This example demonstrates how to use FastSQS middleware presets for quick setup.
Concurrency is configured on the app (`FastSQS(max_concurrent_messages=...)`);
presets only assemble cross-cutting middleware.

## Available Presets

### Production Preset
```python
app = FastSQS(max_concurrent_messages=15)
app.use_preset("production")
```

Includes:
- LoggingMiddleware (with context)
- TimingMsMiddleware

### Development Preset
```python
app.use_preset("development")
```

Includes:
- LoggingMiddleware (verbose, includes record)
- TimingMsMiddleware

### Minimal Preset
```python
app.use_preset("minimal")
```

Includes:
- LoggingMiddleware
- TimingMsMiddleware

## Usage

Instead of manually configuring each middleware:
```python
app.add_middleware(LoggingMiddleware())
app.add_middleware(TimingMsMiddleware())
# ... more middleware
```

Use a preset:
```python
app.use_preset("production")
```

> Retries are handled by SQS (visibility timeout + `maxReceiveCount` + native
> dead-letter queue), not in-process.
