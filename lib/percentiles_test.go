package statsdaemon

import (
	"testing"
	"github.com/stretchr/testify/assert"
)

func TestPercentiles_Set(t *testing.T) {
	p := Percentiles{}
	assert.IsType(t, Percentiles{}, p)
	err := p.Set("0.9")
	assert.NoError(t, err)
	err = p.Set("fail")
	assert.Error(t, err)
	assert.Equal(t, "[0_9]", p.String())
}

func TestPercentile_Sring(t *testing.T) {
	p := Percentile{
		float: 0.9,
		str: "0_9",
	}
	assert.Equal(t, "0_9", p.String())
}