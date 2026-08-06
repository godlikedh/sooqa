# Review checklist

- Is the change limited to one coherent scope?
- Are behavior and failure paths tested?
- Are secrets and user-controlled paths, URLs, commands, and captions handled
  safely?
- Are migrations, retries, idempotency, and operational behavior documented when
  applicable?
- Does `just check` pass?
