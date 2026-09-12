package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadWorkloadAndDeterministicChoices(t *testing.T) {
	path := filepath.Join(t.TempDir(), "workload.json")
	data := []byte(`{"name":"test","seed":7,"operations":[{"name":"strong-read","share":0.5,"consistency":"strong","operation":"get","namespace":"/strong/"},{"name":"eventual-write","share":0.5,"consistency":"eventual","operation":"put","namespace":"/eventual/"}]}`)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	spec, err := loadWorkload(path)
	if err != nil {
		t.Fatal(err)
	}
	first := chooseOperations(spec, 20)
	second := chooseOperations(spec, 20)
	for index := range first {
		if first[index].Name != second[index].Name {
			t.Fatalf("choice %d differs: %q and %q", index, first[index].Name, second[index].Name)
		}
	}
}

func TestLoadWorkloadRejectsInvalidShares(t *testing.T) {
	path := filepath.Join(t.TempDir(), "workload.json")
	data := []byte(`{"name":"bad","operations":[{"name":"only","share":0.9,"consistency":"strong","operation":"get","namespace":"/strong/"}]}`)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadWorkload(path); err == nil {
		t.Fatal("loadWorkload accepted shares that do not sum to one")
	}
}
