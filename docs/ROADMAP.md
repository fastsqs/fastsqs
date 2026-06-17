# fastsqs — improvement backlog

Honest self-critique vs the genre (FastAPI, FastStream, AWS Lambda Powertools).
Tackle one at a time. `[done]` items are already shipped.

- `[done]` Remove the legacy `run_middlewares` runner (it ran every `after` even
  when `before` didn't enter → unbalanced cleanup). Only the balanced
  `run_middleware_stack` remains.
- `[done]` **P0 + P1.2 — dependency injection** via `fast-depends` (adopted, not
  built). `@app.route(...)` passively applies `inject()` when a handler declares
  `Depends(...)` params (no `@inject` needed); plain handlers are untouched.
  Injected deps are type-checked. Validated on real Lambda. Cost: +fast-depends
  +anyio, Python >= 3.10 (was 3.8). Annotation-based injection (#P1.2) now holds
  for `Depends` params. `ctx` is still an untyped dict — see P1.3 below.

---

## P0 — identity decision: is fastsqs "FastAPI-style"?

The biggest gap. We brand it FastAPI-style but lack the defining FastAPI feature.
Pick a direction, then the P1 items follow from it.

- **Option A — honor the brand:** add a minimal **dependency injection** (the P1
  cluster below: `Depends`-like + annotation-based injection + typed context).
- **Option B — right-size the brand:** drop "FastAPI-style", call it a
  *lightweight, typed SQS router*; keep the current name-based injection but
  document it as such. Cheaper, honest.

## P1 — handler DX (depends on the P0 choice; do as a cluster if Option A)

1. **Dependency injection.** No `Depends()` / `Context()`. Today you can't
   declaratively inject a DB session / service / config; the `ctx` dict is a
   manual, untyped substitute. (FastAPI/FastStream both have DI.)
2. **Annotation-based injection (replace name-magic).** Handlers get args by
   *parameter name* (`msg`/`ctx`/`record`/`context`/`payload`) via
   `select_kwargs`. Rename → silently no injection; positional-only params get
   nothing (documented limitation). Peers inject by *type annotation* (explicit,
   IDE-checkable). Make injection annotation-driven.
3. **Typed context.** `ctx` is a free-form dict; stringly-typed keys, typos fail
   silently. `ProcessingContext` (types.py) is doc-only, not the real API.
   Consider a typed context object / attribute access.

## P2 — routing surprises

4. **Make flexible-matching opt-in / explicit.** Auto-deriving message-type
   variants from class names (Camel/snake/kebab) is implicit magic no peer does,
   and can collide (we emit a warning). Default it off, or require opt-in, or at
   least document loudly. On by default today.
5. **Subrouter semantics.** Nested subrouters re-dispatch on the *same* key,
   which is confusing (a test was dropped because the behavior was unclear vs.
   FastAPI's clean `APIRouter` include). Either fix the model or document it
   precisely (or drop subrouters if include_router covers the need).

## P3 — positioning / docs (deliberate trade-offs, just be explicit)

6. **"Fewer batteries" is intentional — document it.** No idempotency, no
   metrics/tracing/structured logger, no in-process retry/circuit-breaker (all
   removed on purpose: SQS-native + lean). vs Powertools this is "less included".
   Add a docs note: "for idempotency/observability, compose your own middleware
   or use aws-lambda-powertools alongside fastsqs."

## P4 — release hygiene (gates v1.0.0)

7. **CHANGELOG + version discipline.** No CHANGELOG; 0.5.x → 1.0.0 broke a lot
   (removed ~9 middleware classes, `enable_partial_batch_failure` now raises,
   `use_preset` signature, masking gone, DLQ middleware gone, `skip_group_on_error`
   added). Write a CHANGELOG with breaking changes + migration, then bump to
   1.0.0. (The two-DLQ ambiguity — the last API blocker — is already resolved.)
