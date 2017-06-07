package statsdaemon

import (
	"github.com/qnib/qframe-types"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewStatsdPacketFromPacket(t *testing.T) {
	p := &Packet{
		Bucket:   "gaugor",
		ValFlt:   333,
		ValStr:   "",
		Modifier: "g",
		Sampling: float32(1),
	}
	sd := NewStatsdPacketFromPacket(p)
	assert.IsType(t, &qtypes.StatsdPacket{}, sd)
	assert.Equal(t, p.Bucket, sd.Bucket)
	assert.Equal(t, p.ValFlt, sd.ValFlt)
	assert.Equal(t, p.ValStr, sd.ValStr)
	p.ValStr = "-"
	sd = NewStatsdPacketFromPacket(p)
	assert.Equal(t, p.ValStr, sd.ValStr)
}
