package statsdaemon

import (
	"testing"
	"github.com/stretchr/testify/assert"
	"bytes"
)

func NewMP() MsgParser {
	return MsgParser{
		debug: false,
	}
}

func TestParseLineGauge(t *testing.T) {
	mp := MsgParser{
		debug: true,
	}
	d := []byte("gaugor:333|g")
	packet := mp.parseLine(d)
	assert.NotEqual(t, packet, nil)
	assert.Equal(t, "gaugor", packet.Bucket)
	assert.Equal(t, float64(333), packet.ValFlt)
	assert.Equal(t, "", packet.ValStr)
	assert.Equal(t, "g", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)

	d = []byte("gaugor:-10|g")
	packet = mp.parseLine(d)
	assert.NotEqual(t, packet, nil)
	assert.Equal(t, "gaugor", packet.Bucket)
	assert.Equal(t, float64(10), packet.ValFlt)
	assert.Equal(t, "-", packet.ValStr)
	assert.Equal(t, "g", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)

	d = []byte("gaugor:+4|g")
	packet = mp.parseLine(d)
	assert.NotEqual(t, packet, nil)
	assert.Equal(t, "gaugor", packet.Bucket)
	assert.Equal(t, float64(4), packet.ValFlt)
	assert.Equal(t, "+", packet.ValStr)
	assert.Equal(t, "g", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)

	// >max(int64) && <max(uint64)
	d = []byte("gaugor:18446744073709551606|g")
	packet = mp.parseLine(d)
	assert.NotEqual(t, packet, nil)
	assert.Equal(t, "gaugor", packet.Bucket)
	assert.Equal(t, float64(18446744073709551606), packet.ValFlt)
	assert.Equal(t, "", packet.ValStr)
	assert.Equal(t, "g", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)

	// float values
	d = []byte("gaugor:3.3333|g")
	packet = mp.parseLine(d)
	assert.NotEqual(t, packet, nil)
	assert.Equal(t, "gaugor", packet.Bucket)
	assert.Equal(t, float64(3.3333), packet.ValFlt)
	assert.Equal(t, "", packet.ValStr)
	assert.Equal(t, "g", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)
}

func TestParseLineCount(t *testing.T) {
	mp := MsgParser{
		debug: true,
	}
	d := []byte("gorets:2|c|@0.1")
	packet := mp.parseLine(d)
	assert.NotEqual(t, packet, nil)
	assert.Equal(t, "gorets", packet.Bucket)
	assert.Equal(t, float64(2), packet.ValFlt)
	assert.Equal(t, "c", packet.Modifier)
	assert.Equal(t, float32(0.1), packet.Sampling)

	d = []byte("gorets:4|c")
	packet = mp.parseLine(d)
	assert.NotEqual(t, packet, nil)
	assert.Equal(t, "gorets", packet.Bucket)
	assert.Equal(t, float64(4), packet.ValFlt)
	assert.Equal(t, "c", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)

	d = []byte("gorets:-4|c")
	packet = mp.parseLine(d)
	assert.NotEqual(t, packet, nil)
	assert.Equal(t, "gorets", packet.Bucket)
	assert.Equal(t, float64(-4), packet.ValFlt)
	assert.Equal(t, "c", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)

	d = []byte("gorets:1.25|c")
	packet = mp.parseLine(d)
	assert.NotEqual(t, packet, nil)
	assert.Equal(t, "gorets", packet.Bucket)
	assert.Equal(t, 1.25, packet.ValFlt)
	assert.Equal(t, "c", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)
}

func TestParseLineTimer(t *testing.T) {
	mp := MsgParser{
		debug: true,
	}
	d := []byte("glork:320|ms")
	packet := mp.parseLine(d)
	assert.NotEqual(t, packet, nil)
	assert.Equal(t, "glork", packet.Bucket)
	assert.Equal(t, float64(320), packet.ValFlt)
	assert.Equal(t, "ms", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)

	d = []byte("glork:320|ms|@0.1")
	packet = mp.parseLine(d)
	assert.NotEqual(t, packet, nil)
	assert.Equal(t, "glork", packet.Bucket)
	assert.Equal(t, float64(320), packet.ValFlt)
	assert.Equal(t, "ms", packet.Modifier)
	assert.Equal(t, float32(0.1), packet.Sampling)

	d = []byte("glork:3.7211|ms")
	packet = mp.parseLine(d)
	assert.NotEqual(t, packet, nil)
	assert.Equal(t, "glork", packet.Bucket)
	assert.Equal(t, float64(3.7211), packet.ValFlt)
	assert.Equal(t, "ms", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)
}

func TestParseLineSet(t *testing.T) {
	mp := MsgParser{
		debug: true,
	}
	d := []byte("uniques:765|s")
	packet := mp.parseLine(d)
	assert.NotEqual(t, packet, nil)
	assert.Equal(t, "uniques", packet.Bucket)
	assert.Equal(t, "765", packet.ValStr)
	assert.Equal(t, "s", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)
}

func TestParseLineMisc(t *testing.T) {
	mp := MsgParser{
		debug: true,
	}

	d := []byte("a.key.with-0.dash:4|c")
	packet := mp.parseLine(d)
	assert.NotEqual(t, packet, nil)
	assert.Equal(t, "a.key.with-0.dash", packet.Bucket)
	assert.Equal(t, float64(4), packet.ValFlt)
	assert.Equal(t, "c", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)

	d = []byte("a.key.with 0.space:4|c")
	packet = mp.parseLine(d)
	assert.Equal(t, "a.key.with_0.space", packet.Bucket)
	assert.Equal(t, float64(4), packet.ValFlt)
	assert.Equal(t, "c", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)

	d = []byte("a.key.with/0.slash:4|c")
	packet = mp.parseLine(d)
	assert.Equal(t, "a.key.with-0.slash", packet.Bucket)
	assert.Equal(t, float64(4), packet.ValFlt)
	assert.Equal(t, "c", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)

	d = []byte("a.key.with@#*&%$^_0.garbage:4|c")
	packet = mp.parseLine(d)
	assert.Equal(t, "a.key.with_0.garbage", packet.Bucket)
	assert.Equal(t, float64(4), packet.ValFlt)
	assert.Equal(t, "c", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)

	mp.prefix = "test."
	d = []byte("prefix:4|c")
	packet = mp.parseLine(d)
	assert.Equal(t, "test.prefix", packet.Bucket)
	assert.Equal(t, float64(4), packet.ValFlt)
	assert.Equal(t, "c", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)
	mp.prefix = ""
	mp.postfix = ".test"
	d = []byte("postfix:4|c")
	packet = mp.parseLine(d)
	assert.Equal(t, "postfix.test", packet.Bucket)
	assert.Equal(t, float64(4), packet.ValFlt)
	assert.Equal(t, "c", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)
	mp.postfix = ""

	d = []byte("a.key.with-0.dash:4|c\ngauge:3|g")
	parser := NewParser(bytes.NewBuffer(d), true, mp.debug, mp.maxUdpPacketSize, mp.prefix, mp.postfix)
	packet, more := parser.Next()
	assert.Equal(t, more, true)
	assert.Equal(t, "a.key.with-0.dash", packet.Bucket)
	assert.Equal(t, float64(4), packet.ValFlt)
	assert.Equal(t, "c", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)

	packet, more = parser.Next()
	assert.Equal(t, more, false)
	assert.Equal(t, "gauge", packet.Bucket)
	assert.Equal(t, 3.0, packet.ValFlt)
	assert.Equal(t, "", packet.ValStr)
	assert.Equal(t, "g", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)

	d = []byte("a.key.with-0.dash:4\ngauge3|g")
	packet = mp.parseLine(d)
	if packet != nil {
		t.Fail()
	}

	d = []byte("a.key.with-0.dash:4")
	packet = mp.parseLine(d)
	if packet != nil {
		t.Fail()
	}

	d = []byte("gorets:5m")
	packet = mp.parseLine(d)
	if packet != nil {
		t.Fail()
	}

	d = []byte("gorets")
	packet = mp.parseLine(d)
	if packet != nil {
		t.Fail()
	}

	d = []byte("gorets:")
	packet = mp.parseLine(d)
	if packet != nil {
		t.Fail()
	}

	d = []byte("gorets:5|mg")
	packet = mp.parseLine(d)
	if packet != nil {
		t.Fail()
	}

	d = []byte("gorets:5|ms|@")
	packet = mp.parseLine(d)
	if packet != nil {
		t.Fail()
	}

	d = []byte("")
	packet = mp.parseLine(d)
	if packet != nil {
		t.Fail()
	}

	d = []byte("gorets:xxx|c")
	packet = mp.parseLine(d)
	if packet != nil {
		t.Fail()
	}

	d = []byte("gaugor:xxx|g")
	packet = mp.parseLine(d)
	if packet != nil {
		t.Fail()
	}

	d = []byte("gaugor:xxx|z")
	packet = mp.parseLine(d)
	if packet != nil {
		t.Fail()
	}

	d = []byte("deploys.test.myservice4:100|t")
	packet = mp.parseLine(d)
	if packet != nil {
		t.Fail()
	}

	d = []byte("up-to-colon:")
	packet = mp.parseLine(d)
	if packet != nil {
		t.Fail()
	}

	d = []byte("up-to-pipe:1|")
	packet = mp.parseLine(d)
	if packet != nil {
		t.Fail()
	}
}

func TestMultiLine(t *testing.T) {
	mp := MsgParser{
		debug: true,
	}
	b := bytes.NewBuffer([]byte("a.key.with-0.dash:4|c\ngauge:3|g"))
	parser := NewParser(b, true, mp.debug, mp.maxUdpPacketSize, mp.prefix, mp.postfix)
	packet, more := parser.Next()
	assert.NotEqual(t, packet, nil)
	assert.Equal(t, more, true)
	assert.Equal(t, "a.key.with-0.dash", packet.Bucket)
	assert.Equal(t, float64(4), packet.ValFlt)
	assert.Equal(t, "c", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)

	packet, more = parser.Next()
	assert.NotEqual(t, packet, nil)
	assert.Equal(t, more, false)
	assert.Equal(t, "gauge", packet.Bucket)
	assert.Equal(t, 3.0, packet.ValFlt)
	assert.Equal(t, "", packet.ValStr)
	assert.Equal(t, "g", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)
}
