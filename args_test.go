package main

import (
	"reflect"
	"strconv"
	"testing"

	"github.com/kr/pretty"
)

func TestParseAggregator(t *testing.T) {
	testCases := []struct {
		input  string
		result *AggregatorDefinition
		errstr string
	}{
		/*
		  Invalid entries
		*/
		{
			"",
			nil,
			"Invalid aggregator spec '': too short (did you forget the name?)",
		},
		{
			"name",
			nil,
			"Invalid aggregator spec 'name': too short (did you forget the name?)",
		},
		{
			"name,var",
			nil,
			"malformed variable spec 'var': not of form <source>:<field>",
		},
		{
			"name,foo:bar",
			nil,
			"invalid variable spec: source foo not found",
		},
		{
			"name,flow:INVALID",
			nil,
			"invalid variable spec: variable flow:INVALID not found",
		},

		/*
			Simple cases
		*/
		{
			"name,flow:PROTOCOL",
			&AggregatorDefinition{
				Name: "name",
				Fields: []Field{
					{
						Name:   "PROTOCOL",
						Source: P_FLOW,
						Field:  uint16(4),
					},
				},
				CounterFields: nil,
			},
			"",
		},
		{
			"name,flow:PROTOCOL,geoip:src_country",
			&AggregatorDefinition{
				Name: "name",
				Fields: []Field{
					{
						Name:   "PROTOCOL",
						Source: P_FLOW,
						Field:  4,
					},
					{
						Name:   "src_country",
						Source: P_GEO,
						Field:  1,
					},
				},
				CounterFields: nil,
			},
			"",
		},
	}

	for i, tc := range testCases {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			result, err := ParseAggregator(tc.input)
			if err != nil {
				if tc.errstr == "" {
					t.Errorf("expected no error, got %q", err)
				} else if tc.errstr != err.Error() {
					t.Errorf("expected error '%q', got '%q'", tc.errstr, err)
				}
				return
			} else if tc.errstr != "" {
				t.Errorf("Expected error '%q', got none", tc.errstr)
			}

			// comparing functions doesn't work, yay
			for i := 0; i < len(result.Fields); i++ {
				result.Fields[i].Stringify = nil
			}

			if !reflect.DeepEqual(tc.result, result) {
				t.Errorf("want %+v, got %+v\ndiff: %s", tc.result, result, pretty.Diff(tc.result, result))
			}
		})
	}
}
