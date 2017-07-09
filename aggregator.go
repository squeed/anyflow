package main

import (
	"fmt"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
)

type Aggregator struct {
	Def    AggregatorDefinition
	fields map[uint32]Field
	cs     map[uint32]counter
}

type counter struct {
	c  *prometheus.CounterVec
	cf *CounterField
}

// NewAggregator
func NewAggregator(d AggregatorDefinition) (*Aggregator, error) {
	a := Aggregator{
		Def:    d,
		fields: map[uint32]Field{},
		cs:     map[uint32]counter{},
	}

	fieldNames := []string{}

	for _, f := range d.Fields {
		fieldNames = append(fieldNames, f.Name)
		a.fields[fkey(f.Source, f.Field)] = f
	}

	for _, f := range d.CounterFields {
		ctr := counter{
			c: prometheus.NewCounterVec(
				prometheus.CounterOpts{
					Name: fmt.Sprintf("%s_%s_%s", METRIC_PREFIX, d.Name, f.Name),
					Help: "some stuff",
				},
				fieldNames,
			),
			cf: &f,
		}
		a.cs[fkey(f.Source, f.Field)] = ctr
		if err := prometheus.Register(ctr.c); err != nil {
			return nil, errors.Wrap(err, "Failed to register counter")
		}
	}

	return &a, nil
}

type counterUpdate struct {
	c   *prometheus.CounterVec
	val float64
}

func (a *Aggregator) Extract(r EnrichedRecord) (prometheus.Labels, []counterUpdate) {
	labels := prometheus.Labels{}
	cu := []counterUpdate{}

	for sourceID, record := range r {
		for _, value := range record.Values {
			k := fkey(sourceID, value.Type)
			c, ok := a.cs[k]
			if ok {
				cu = append(cu, counterUpdate{
					c:   c.c,
					val: c.cf.Floatify(value.Value),
				})
				continue
			}

			field, ok := a.fields[k]
			if !ok {
				continue
			}

			labels[field.Name] = field.Stringify(value.Value)
		}
	}

	return labels, cu
}

func (a *Aggregator) Update(r EnrichedRecord) {
}

// fkey combines the u8 provider id and the u16 field id in to a single
// u32 value for internal use
func fkey(source SourceID, field uint16) uint32 {
	return uint32(source)<<16 + uint32(field)
}
