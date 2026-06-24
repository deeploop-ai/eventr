# HOCON parser spike

Validates [`github.com/gurkankaymak/hocon`](https://github.com/gurkankaymak/hocon) against eventr §8.2.2 acceptance criteria before v2.0-alpha.

```bash
cd spike/hocon
go test -v ./...
```

Test fixtures mirror `eventr-design.md` §8.3 (linear) and §8.4 (`depends_on` map + `route`).

The `ConfigToMap` helper in `config_tree.go` is a spike prototype for the production loader path: HOCON → `map[string]any` → `PipelineConfig`.
