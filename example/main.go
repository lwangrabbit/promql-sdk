package main

import (
	"encoding/json"
	"log"
	"time"

	"github.com/lwangrabbit/promql-sdk"
)

func main() {
	// init
	configs := []*promql_sdk.ReadConfig{
		&promql_sdk.ReadConfig{
			URL:     "http://192.168.0.1:8086/api/v1/prom/read?db=prometheus",
			Timeout: 10 * time.Second,
		},
		&promql_sdk.ReadConfig{
			URL:     "http://192.168.0.2:8086/api/v1/prom/read?db=prometheus",
			Timeout: 10 * time.Second,
		},
		&promql_sdk.ReadConfig{
			URL:     "http://192.168.0.3:8086/api/v1/prom/read?db=prometheus",
			Timeout: 10 * time.Second,
		},
	}
	err := promql_sdk.Init(configs, promql_sdk.LookBackDelta(3*time.Minute))
	if err != nil {
		panic(err)
	}

	query := `(1 - avg by(instance) (rate(node_cpu_seconds_total{mode="idle"}[5m])))* 100`

	// query instant
	res, err := promql_sdk.Query(query)
	if err != nil {
		panic(err)
	}
	bs, err := json.Marshal(res)
	// {"resultType":"vector","result":[{"metric":{"instance":"node1"},"value":[1652086175.251,"76.00700000021607"]},{"metric":{"instance":"node2"},"value":[1652086175.251,"72.67599999997765"]}],"stats":{"timings":{"evalTotalTime":0.147102008,"resultSortTime":0,"queryPreparationTime":0.146951773,"innerEvalTime":0.000137064,"execQueueTime":0.000004767,"execTotalTime":0.147114756}}}
	log.Println("query result: ", string(bs))

	// query range
	startTs := time.Now().Unix() - 10*60
	endTs := time.Now().Unix()
	res, err = promql_sdk.QueryRange(query, startTs, endTs, 60)
	bs, err = json.Marshal(res)
	//  {"resultType":"matrix","result":[{"metric":{"instance":"node1"},"values":[[1652086115,"88.31412083342268"],[1652086175,"76.00700000021607"]]},{"metric":{"instance":"node2"},"values":[[1652086115,"87.37343999993503"],[1652086175,"72.67599999997765"]]}],"stats":{"timings":{"evalTotalTime":0.062898956,"resultSortTime":0.000003029,"queryPreparationTime":0.062201867,"innerEvalTime":0.000630189,"execQueueTime":0.000001961,"execTotalTime":0.062914757}}}
	log.Println("query range: ", string(bs))
}
