package main

import "github.com/jesk78/anyflow/proto/netflow"

type EnrichedRecord map[SourceID]*netflow.Record

type SourceID uint8

const (
	P_FLOW SourceID = iota
	P_GEO
	P_AS
)

var availSources = map[SourceID]Source{
	P_FLOW: (*FlowSource)(nil),
	P_GEO:  (*GeoIPEnricher)(nil),
}

type Source interface {
	Id() SourceID
	Enrich(EnrichedRecord) error
	Name() string
	Fields() map[string]Field
}

func StringValue(val string, typ uint16) *netflow.Value {
	return &netflow.Value{
		Value:  []byte(val),
		Type:   typ,
		Length: uint16(len(val)),
	}
}

func sourceNames(e map[SourceID]Source) map[string]Source {
	out := map[string]Source{}
	for _, s := range availSources {
		out[s.Name()] = s
	}
	return out
}

// Simple wrapper for the flow enricher
type FlowSource struct {
}

func (f *FlowSource) Id() SourceID {
	return P_FLOW
}

func (f *FlowSource) Enrich(r EnrichedRecord) error {
	panic("Should never happen")
	return nil
}

func (f *FlowSource) Name() string {
	return "flow"
}

func (f *FlowSource) Fields() map[string]Field {
	out := map[string]Field{}
	for id, typ := range netflow.Nf9FieldMap {
		out[typ.Type] = Field{
			Name:      typ.Type,
			Source:    P_FLOW,
			Field:     id,
			Stringify: typ.Stringify,
		}
	}
	return out
}

func bytesToString(b []byte) string {
	return string(b)
}
