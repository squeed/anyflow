package main

import (
	"fmt"
	"net/http"
	"os"

	log "github.com/Sirupsen/logrus"
	flag "github.com/spf13/pflag"

	"github.com/prometheus/client_golang/prometheus"
)

func init() {
	log.SetFormatter(&log.TextFormatter{})
	log.SetOutput(os.Stdout)
	log.SetLevel(log.DebugLevel)

	prometheus.MustRegister(packetsTotal)
}

func parseArgs() (Config, error) {
	listFields := false

	c := Config{
		AggregatorDefinitions: map[string]AggregatorDefinition{},
	}

	var aggdefs []string
	flag.StringArrayVar(&aggdefs, "aggregator", nil,
		"Aggregator to create. Format: <name>,<Source>:<Column>,...")

	flag.StringVar(&c.FlowListenAddress, "flow-port", ":10001",
		"UDP port to receive flows")

	flag.StringVar(&c.MetricListenAddress, "metric-port", ":8080",
		"TCP port to serve metrics")

	flag.BoolVar(&listFields, "list-fields", false, "List fields and quit")

	flag.Parse()

	if listFields {
		for _, s := range availSources {
			PrintFields(s)
		}
		os.Exit(1)
	}

	if len(aggdefs) == 0 {
		flag.Usage()
		return c, fmt.Errorf("no aggegators specified")
	}

	for _, d := range aggdefs {
		agg, err := ParseAggregator(d)
		if err != nil {
			return c, err
		}
		c.AggregatorDefinitions[agg.Name] = *agg
	}

	return c, nil
}

func main() {
	os.Exit(run())
}

func run() int {
	c, err := parseArgs()
	if err != nil {
		log.Fatal(err)
		return 1
	}

	app, err := InitApp(c)
	if err != nil {
		log.Fatal(err)
		return 1
	}
	app.Start()

	http.Handle("/metrics", prometheus.Handler())
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`<html>
            <head><title>Anyflow Metrics Server</title></head>
            <body>
            <h1>Anyflow Metrics Server</h1>
            <p><a href="/metrics">Metrics</a></p>
            </body>
            </html>`))
	})

	log.Infof("HTTP listening on %s", c.MetricListenAddress)
	if err := http.ListenAndServe(c.MetricListenAddress, nil); err != nil {
		panic(fmt.Errorf("Error starting HTTP server: %s", err))
	}

	return 0
}
