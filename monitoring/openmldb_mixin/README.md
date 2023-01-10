# OpenMLDB Mixin

A set of configurable, reusable, and extensible alert and rules for prometheus and grafana dashboard configuration

## Requirement

- Golang >= 1.17
- [mixtool](https://github.com/monitoring-mixins/mixtool)
- [jsonnetfmt](https://github.com/google/go-jsonnet)

```bash
$ go install github.com/monitoring-mixins/mixtool/cmd/mixtool@latest
$ go install github.com/google/go-jsonnet/cmd/jsonnetfmt@latest
```

## Build

```bash
make
```

## Use

1. a config example for prometheus server is `prometheus_example.yml`
2. grafana dashboard avaiable in `dashboards_out/openmldb_dashboard.json`

Refer  [promtheus get started](https://prometheus.io/docs/prometheus/latest/getting_started/) and [grafana get started](https://grafana.com/docs/grafana/latest/getting-started/getting-started-prometheus/) on how to configure prometheus and grafana
