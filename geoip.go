package main

import (
	"fmt"

	"github.com/jesk78/anyflow/proto/netflow"
	"github.com/oschwald/maxminddb-golang"
)

type GeoIPEnricher struct {
	db *maxminddb.Reader
}

// Magic incantation to confirm interface compliance
var _ Source = (*GeoIPEnricher)(nil)

func NewGeoIPEnricher() (*GeoIPEnricher, error) {
	g := GeoIPEnricher{}
	return &g, nil
}

func (g *GeoIPEnricher) Id() SourceID {
	return P_GEO
}

func (g *GeoIPEnricher) Name() string {
	return "geoip"
}

// Used for parsing
func (g *GeoIPEnricher) Fields() map[string]Field {
	return map[string]Field{
		"src_country": {
			"src_country",
			P_GEO,
			1,
			bytesToString,
		},
		"src_region": {
			"src_region",
			P_GEO,
			2,
			bytesToString,
		},
		"dst_country": {
			"dst_country",
			P_GEO,
			3,
			bytesToString,
		},
		"dst_regioin": {
			"dst_region",
			P_GEO,
			4,
			bytesToString,
		},
	}
}

// TODO: actually implement
func (g *GeoIPEnricher) Enrich(r EnrichedRecord) error {
	flow, ok := r[P_FLOW]
	if !ok {
		return fmt.Errorf("record has no flow")
	}

	out := netflow.Record{}

	for _, v := range flow.Values {
		if v.Type == 8 /*IPv4_SRC_ADDR*/ {

			// DO GEOIP HERE
			geoIPCountry := "JA"
			//geoIPRegion := "Tokyo"
			out.Values = append(out.Values, netflow.Value{
				Value:  []byte(geoIPCountry),
				Type:   1,
				Length: uint16(len(geoIPCountry)),
			})
		}
	}

	r[P_GEO] = &out
	return nil
}
