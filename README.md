
## promql-sdk

promql-sdk make tsdb can use promql, implemented by prometheus remote-read.

## How to use it

### 1. init

```
c := &promql_sdk.ReadConfig{
    URL:     "http://127.0.0.1:8086/api/v1/prom/read?db=prometheus",    //influxdb
    Timeout: 1 * time.Minute,
}
configs := []*promql_sdk.ReadConfig{c}
err := promql_sdk.Init(configs)
```

### 2. query instant

```
query := `(1 - avg by(instance) (rate(node_cpu_seconds_total{mode="idle"}[5m])))* 100`
res, err := promql_sdk.Query(query)
```

query result:
```
{"resultType":"vector","result":[{"metric":{"instance":"node1"},"value":[1652086175.251,"76.00700000021607"]},{"metric":{"instance":"node2"},"value":[1652086175.251,"72.67599999997765"]}]}
```

### 3. query range

```
query := `(1 - avg by(instance) (rate(node_cpu_seconds_total{mode="idle"}[5m])))* 100`
res, err = promql_sdk.QueryRange(query, startTs, endTs, step)
```

query range result:
```
 {"resultType":"matrix","result":[{"metric":{"instance":"node1"},"values":[[1652086115,"88.31412083342268"],[1652086175,"76.00700000021607"]]},{"metric":{"instance":"node2"},"values":[[1652086115,"87.37343999993503"],[1652086175,"72.67599999997765"]]}]}
```
