package main

import (
	"fmt"
	"strings"

	"github.com/jesk78/anyflow/proto/netflow"
)

//ParseAggregator parses a metric's command-line definition.
// It should look like:
//   name,Source:Column<,Source:Column...>
// e.g.
//   traffic_by_region,geoip:src_country,geoip:src_region
//
func ParseAggregator(input string) (*AggregatorDefinition, error) {
	parts := strings.Split(input, ",")
	if len(parts) < 2 {
		return nil, fmt.Errorf("Invalid aggregator spec '%s': too short (did you forget the name?)", input)
	}

	def := AggregatorDefinition{
		Name:          parts[0],
		Fields:        make([]Field, 0, len(parts)-1),
		CounterFields: nil,
	}

	sources := sourceNames(availSources)

	for _, part := range parts[1:] {
		vardef := strings.SplitN(part, ":", 2)
		if len(vardef) != 2 {
			return nil, fmt.Errorf("malformed variable spec '%s': not of form <source>:<field>", part)
		}

		src, ok := sources[vardef[0]]
		if !ok {
			return nil, fmt.Errorf("invalid variable spec: source %s not found", vardef[0])
		}

		sourceFields := src.Fields()
		field, ok := sourceFields[vardef[1]]
		if !ok {
			return nil, fmt.Errorf("invalid variable spec: variable %s not found", part)
		}

		def.Fields = append(def.Fields, field)
	}

	def.CounterFields = defaultCFs()

	return &def, nil
}

// defaultCFs returns the standard CounterFields for an aggregator.
// TODO: actually make this customizable
func defaultCFs() []CounterField {
	return []CounterField{
		{
			Name:     "in_bytes",
			Source:   P_FLOW,
			Field:    1,
			Floatify: bytesToFloat,
		},
	}
}

// Turn a netflow []byte in to a counter value
func bytesToFloat(b []byte) float64 {
	return float64(netflow.BytesToUint64(b))
}
