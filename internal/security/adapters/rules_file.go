package adapters

import (
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/nickemma/meridian/internal/security/domain"
)

// readRules decodes exactly one strict JSON array of client rules.
func readRules(path string) ([]domain.Rule, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	var rules []domain.Rule
	decoder := json.NewDecoder(file)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&rules); err != nil {
		return nil, err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return nil, fmt.Errorf("client policy must contain one JSON array")
	}
	return rules, nil
}

func writeRules(path string, rules []domain.Rule) error {
	data, err := json.MarshalIndent(rules, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(data, '\n'), 0600)
}
