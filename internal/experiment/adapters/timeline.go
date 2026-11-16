package adapters

import (
	"encoding/json"
	"io"

	"github.com/nickemma/meridian/internal/experiment/domain"
	"github.com/nickemma/meridian/internal/experiment/ports"
)

// JSONLTimeline writes one JSON event per line.
type JSONLTimeline struct{ encoder *json.Encoder }

var _ ports.Timeline = JSONLTimeline{}

func NewJSONLTimeline(w io.Writer) JSONLTimeline { return JSONLTimeline{encoder: json.NewEncoder(w)} }

func (t JSONLTimeline) Write(e domain.Event) error { return t.encoder.Encode(e) }
