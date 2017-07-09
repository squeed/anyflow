package main

import (
	"net"

	log "github.com/Sirupsen/logrus"

	"github.com/jesk78/anyflow/proto/netflow"
)

const DEFAULT_QUEUE_DEPTH = 100
const METRIC_PREFIX = "netflow"

type Config struct {
	AggregatorDefinitions map[string]AggregatorDefinition

	RDepth uint
	EDepth uint
	EPool  uint

	FlowListenAddress   string
	MetricListenAddress string
}

type AggregatorDefinition struct {
	Name          string
	Fields        []Field
	CounterFields []CounterField
}

type Field struct {
	Name      string
	Source    SourceID
	Field     uint16 // same as netflow "type" key
	Stringify func([]byte) string
}

type CounterField struct {
	Name     string
	Source   SourceID
	Field    uint16
	Floatify func([]byte) float64
}

type App struct {
	Config       Config
	RecordChan   chan *netflow.Record
	EnricherChan chan EnrichedRecord
	QuitChan     chan interface{}
	Sources      []Source
	Aggregators  []Aggregator

	FlowSock *net.UDPConn
}

func InitApp(c Config) (*App, error) {
	if c.EDepth == 0 {
		c.EDepth = DEFAULT_QUEUE_DEPTH
	}
	if c.RDepth == 0 {
		c.RDepth = DEFAULT_QUEUE_DEPTH
	}
	log.Debug(c)

	a := App{
		Config:       c,
		RecordChan:   make(chan *netflow.Record, c.RDepth),
		EnricherChan: make(chan EnrichedRecord, c.EDepth),
		QuitChan:     make(chan interface{}),
	}

	var err error

	needEnrichers := map[SourceID]interface{}{}
	for _, d := range c.AggregatorDefinitions {
		agg, err := NewAggregator(d)
		if err != nil {
			return nil, err
		}
		a.Aggregators = append(a.Aggregators, *agg)

		for _, f := range d.Fields {
			needEnrichers[f.Source] = nil
		}
	}

	// TODO: get rid of this switch
	for sourceID, _ := range needEnrichers {
		switch sourceID {
		case P_FLOW:
			//ignore; the flow is always there
		case P_GEO:
			e, err := NewGeoIPEnricher()
			if err != nil {
				return nil, err
			}
			a.Sources = append(a.Sources, e)
		default:
			// will never get here.
			panic("config parse error: unknown enricher")
		}
	}

	log.Debugf("Created pipeline")

	log.Info("Listening for flows on ", a.Config.FlowListenAddress)
	addr, err := net.ResolveUDPAddr("udp", a.Config.FlowListenAddress)
	if err != nil {
		return nil, err
	}

	a.FlowSock, err = net.ListenUDP("udp", addr)
	if err != nil {
		return nil, err
	}

	return &a, nil
}

func (a *App) Start() {
	log.Info("starting flow pipeline")
	go Receive(a.FlowSock, a.RecordChan)
	go a.enrich()
	go a.aggregate()
}

func (a *App) Stop() {
	a.FlowSock.Close()
	close(a.QuitChan)
}

func (a *App) enrich() {
	for {
		select {
		case r := <-a.RecordChan:
			log.Debug("got record!")
			a.EnricherChan <- a.EnrichRecord(r)
		case <-a.QuitChan:
			return
		}
	}
}

func (a *App) aggregate() {
	for {
		select {
		case r := <-a.EnricherChan:
			for _, agg := range a.Aggregators {
				agg.Update(r)
			}
		case <-a.QuitChan:
			return
		}
	}
}

func (a *App) EnrichRecord(r *netflow.Record) EnrichedRecord {

	er := EnrichedRecord{
		P_FLOW: r,
	}

	for _, source := range a.Sources {
		log.Debug("enriching with source ", source.Name())
		// TODO: error handling
		source.Enrich(er)
	}

	return er
}
