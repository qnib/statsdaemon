package statsdaemon

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestPercentiles_Set(t *testing.T) {
	p := Percentiles{}
	assert.IsType(t, Percentiles{}, p)
	err := p.Set("90")
	assert.NoError(t, err)
	err = p.Set("fail")
	assert.Error(t, err)
	assert.Equal(t, "[90]", p.String())
}

func TestPercentile_Sring(t *testing.T) {
	p := Percentile{
		float: 90,
		str:   "90",
	}
	assert.Equal(t, "90", p.String())
}
