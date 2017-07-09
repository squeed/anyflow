package main

import "testing"

func TestFkey(t *testing.T) {
	v := fkey(SourceID(99), 0)
	if v != 6488064 {
		t.Errorf("fkey expected %v got %v", 6488064, v)
	}
	v = fkey(SourceID(99), 132)
	if v != 6488196 {
		t.Errorf("fkey expected %v got %v", 6488196, v)
	}

}

func TestExtract(t *testing.T) {
}
