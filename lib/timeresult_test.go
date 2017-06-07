package statsdaemon

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestTimerResult(t *testing.T) {
	exp := map[string]float64{
		"test.upper": 10.0,
		"test.lower": 0.0,
		"test.mean":  5.0,
		"test.count": 2.0,
	}
	tr := NewTimerResult(exp)
	tr.Input("test.upper", 10.0)
	tr.Input("test.lower", 0.0)
	tr.Input("test.mean", 5.0)
	tr.Input("test.count", 2.0)
	assert.True(t, tr.Check())
}

func TestTimerResultMissed(t *testing.T) {
	exp := map[string]float64{
		"test.upper": 10.0,
		"test.lower": 0.0,
		"test.mean":  5.0,
		"test.count": 2.0,
	}
	tr := NewTimerResult(exp)
	tr.Input("test.upper", 10.0)
	tr.Input("test.lower", 0.0)
	tr.Input("test.mean", 5.0)
	assert.False(t, tr.Check())
}

func TestTimerResultNok(t *testing.T) {
	exp := map[string]float64{
		"test.upper": 10.0,
		"test.lower": 0.0,
		"test.mean":  5.0,
		"test.count": 1.0,
	}
	tr := NewTimerResult(exp)
	tr.Input("test.upper", 10.0)
	tr.Input("test.lower", 0.0)
	tr.Input("test.mean", 5.0)
	tr.Input("test.count", 2.0)
	assert.False(t, tr.Check())
}
