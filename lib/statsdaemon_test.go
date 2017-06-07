package statsdaemon

import (
	"bytes"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/zpatrick/go-config"

	"github.com/qnib/qframe-types"
	"github.com/stretchr/testify/assert"
)

func NewCfg() *config.Config {
	return NewPreCfg(map[string]string{})
}

func NewPreCfg(pre map[string]string) *config.Config {
	cfgMap := map[string]string{
		"log.level":                 "info",
		"statsd.test":               "0",
		"statsd.address":            ":8125",
		"statsd.debug":              "false",
		"statsd.resent-gauges":      "false",
		"statsd.persist-count-keys": "60",
	}
	for k, v := range pre {
		cfgMap[k] = v
	}
	cfg := config.NewConfig(
		[]config.Provider{
			config.NewStatic(cfgMap),
		},
	)
	return cfg
}

func TestNewStatsdaemonPercentiles(t *testing.T) {
	pre := map[string]string{"statsd.percentiles": "0.9"}
	cfg := NewPreCfg(pre)
	sd := NewStatsdaemon(cfg)
	assert.Equal(t, "[0_9]", sd.Percentiles.String())
	pre = map[string]string{"statsd.percentiles": "0.9,0.95"}
	cfg = NewPreCfg(pre)
	sd = NewStatsdaemon(cfg)
	assert.Equal(t, "[0_9 0_95]", sd.Percentiles.String())
}

func TestPacketHandlerReceiveCounter(t *testing.T) {
	pre := map[string]string{"statsd.receive-counter": "countme"}
	cfg := NewPreCfg(pre)
	sd := NewStatsdaemon(cfg)

	p := &Packet{
		Bucket:   "gorets",
		ValFlt:   100,
		Modifier: "c",
		Sampling: float32(1),
	}
	sd.packetHandler(p)
	assert.Equal(t, sd.Counters["countme"], float64(1))

	sd.packetHandler(p)
	assert.Equal(t, sd.Counters["countme"], float64(2))
}

func TestPacketHandlerCount(t *testing.T) {
	cfg := NewCfg()
	sd := NewStatsdaemon(cfg)

	p := &Packet{
		Bucket:   "gorets",
		ValFlt:   100,
		Modifier: "c",
		Sampling: float32(1),
	}
	sd.packetHandler(p)
	assert.Equal(t, sd.Counters["gorets"], float64(100))

	p.ValFlt = float64(3)
	sd.packetHandler(p)
	assert.Equal(t, sd.Counters["gorets"], float64(103))

	p.ValFlt = float64(-4)
	sd.packetHandler(p)
	assert.Equal(t, sd.Counters["gorets"], float64(99))

	p.ValFlt = float64(-100)
	sd.packetHandler(p)
	assert.Equal(t, sd.Counters["gorets"], float64(-1))
}

func TestPacketHandlerGauge(t *testing.T) {
	cfg := NewCfg()
	sd := NewStatsdaemon(cfg)

	p := &Packet{
		Bucket:   "gaugor",
		ValFlt:   333,
		ValStr:   "",
		Modifier: "g",
		Sampling: float32(1),
	}
	sd.packetHandler(p)
	assert.Equal(t, sd.Gauges["gaugor"], float64(333))

	// -10
	p.ValFlt = 10
	p.ValStr = "-"
	sd.packetHandler(p)
	assert.Equal(t, sd.Gauges["gaugor"], float64(323))

	// +4
	p.ValFlt = 4
	p.ValStr = "+"
	sd.packetHandler(p)
	assert.Equal(t, sd.Gauges["gaugor"], float64(327))

	// <0 overflow
	p.ValFlt = 10
	p.ValStr = ""
	sd.packetHandler(p)
	p.ValFlt = 20
	p.ValStr = "-"
	sd.packetHandler(p)
	assert.Equal(t, sd.Gauges["gaugor"], float64(0))

	// >MaxFloat64 overflow
	p.ValFlt = float64(math.MaxFloat64 - 10)
	p.ValStr = ""
	sd.packetHandler(p)
	p.ValFlt = 20
	p.ValStr = "+"
	sd.packetHandler(p)
	assert.Equal(t, sd.Gauges["gaugor"], float64(math.MaxFloat64))
}

func TestPacketHandlerTimer(t *testing.T) {
	cfg := NewCfg()
	sd := NewStatsdaemon(cfg)

	p := &Packet{
		Bucket:   "glork",
		ValFlt:   float64(320),
		Modifier: "ms",
		Sampling: float32(1),
	}
	sd.packetHandler(p)
	assert.Equal(t, len(sd.Timers["glork"]), 1)
	assert.Equal(t, sd.Timers["glork"][0], float64(320))

	p.ValFlt = float64(100)
	sd.packetHandler(p)
	assert.Equal(t, len(sd.Timers["glork"]), 2)
	assert.Equal(t, sd.Timers["glork"][1], float64(100))
}

func TestPacketHandlerSet(t *testing.T) {
	cfg := NewCfg()
	sd := NewStatsdaemon(cfg)
	p := &Packet{
		Bucket:   "uniques",
		ValStr:   "765",
		Modifier: "s",
		Sampling: float32(1),
	}
	sd.packetHandler(p)
	assert.Equal(t, len(sd.Sets["uniques"]), 1)
	assert.Equal(t, sd.Sets["uniques"][0], "765")

	p.ValStr = "567"
	sd.packetHandler(p)
	assert.Equal(t, len(sd.Sets["uniques"]), 2)
	assert.Equal(t, sd.Sets["uniques"][1], "567")
}

func TestProcessCounters(t *testing.T) {
	cfg := NewCfg()
	sd := NewStatsdaemon(cfg)

	var buffer bytes.Buffer
	now := int64(1418052649)

	sd.Counters["gorets"] = float64(123)

	num := sd.ProcessCounters(&buffer, now)
	assert.Equal(t, num, int64(1))
	assert.Equal(t, buffer.String(), "gorets 123 1418052649\n")

	// run processCounters() enough times to make sure it purges items
	for i := 0; i < sd.Int("persist-count-keys")+10; i++ {
		num = sd.ProcessCounters(&buffer, now)
	}
	lines := bytes.Split(buffer.Bytes(), []byte("\n"))

	// expect two more lines - the good one and an empty one at the end
	assert.Equal(t, len(lines), sd.Int("persist-count-keys")+2)
	assert.Equal(t, string(lines[0]), "gorets 123 1418052649")
	assert.Equal(t, string(lines[sd.Int("persist-count-keys")]), "gorets 0 1418052649")
}

func TestProcessGauges(t *testing.T) {
	cfg := NewCfg()
	sd := NewStatsdaemon(cfg)
	var buffer bytes.Buffer
	now := int64(1418052649)

	num := sd.ProcessGauges(&buffer, now)
	assert.Equal(t, num, int64(0))
	assert.Equal(t, buffer.String(), "")

	p := &Packet{
		Bucket:   "gaugor",
		ValFlt:   12345,
		ValStr:   "",
		Modifier: "g",
		Sampling: 1.0,
	}
	sd.packetHandler(p)
	num = sd.ProcessGauges(&buffer, now)
	assert.Equal(t, num, int64(1))
	num = sd.ProcessGauges(&buffer, now+20)
	assert.Equal(t, num, int64(1))
	assert.Equal(t, buffer.String(), "gaugor 12345 1418052649\ngaugor 12345 1418052669\n")

	buffer = bytes.Buffer{}
	p.ValFlt = 12346.75
	sd.packetHandler(p)
	p.ValFlt = 12347.25
	sd.packetHandler(p)
	num = sd.ProcessGauges(&buffer, now+40)
	assert.Equal(t, num, int64(1))
	assert.Equal(t, buffer.String(), "gaugor 12347.25 1418052689\n")
}

func TestProcessDeleteGauges(t *testing.T) {
	pre := map[string]string{"statsd.delete-gauges": "true"}
	cfg := NewPreCfg(pre)
	sd := NewStatsdaemon(cfg)
	var buffer bytes.Buffer

	now := int64(1418052649)

	p := &Packet{
		Bucket:   "gaugordelete",
		ValFlt:   12345,
		ValStr:   "",
		Modifier: "g",
		Sampling: 1.0,
	}

	sd.packetHandler(p)
	num := sd.ProcessGauges(&buffer, now)
	assert.Equal(t, num, int64(1))
	assert.Equal(t, buffer.String(), "gaugordelete 12345 1418052649\n")

	num = sd.ProcessGauges(&buffer, now+20)
	assert.Equal(t, num, int64(0))
	assert.Equal(t, buffer.String(), "gaugordelete 12345 1418052649\n")
}

func TestProcessSets(t *testing.T) {
	cfg := NewCfg()
	sd := NewStatsdaemon(cfg)
	now := int64(1418052649)

	var buffer bytes.Buffer

	// three unique values
	sd.Sets["uniques"] = []string{"123", "234", "345"}
	num := sd.ProcessSets(&buffer, now)
	assert.Equal(t, num, int64(1))
	assert.Equal(t, buffer.String(), "uniques 3 1418052649\n")

	// one value is repeated
	buffer.Reset()
	sd.Sets["uniques"] = []string{"123", "234", "234"}
	num = sd.ProcessSets(&buffer, now)
	assert.Equal(t, num, int64(1))
	assert.Equal(t, buffer.String(), "uniques 2 1418052649\n")

	// make sure sets are purged
	num = sd.ProcessSets(&buffer, now)
	assert.Equal(t, num, int64(0))
}

func TestProcessTimers(t *testing.T) {
	// Some data with expected mean of 20
	cfg := NewCfg()
	sd := NewStatsdaemon(cfg)

	sd.Timers["response_time"] = []float64{0, 30, 30}

	now := int64(1418052649)

	var buffer bytes.Buffer
	num := sd.ProcessTimers(&buffer, now)

	lines := bytes.Split(buffer.Bytes(), []byte("\n"))

	assert.Equal(t, num, int64(1))
	assert.Equal(t, string(lines[0]), "response_time.mean 20 1418052649")
	assert.Equal(t, string(lines[1]), "response_time.upper 30 1418052649")
	assert.Equal(t, string(lines[2]), "response_time.lower 0 1418052649")
	assert.Equal(t, string(lines[3]), "response_time.count 3 1418052649")

	num = sd.ProcessTimers(&buffer, now)
	assert.Equal(t, num, int64(0))
}

func TestProcessTimersUpperPercentile(t *testing.T) {
	pre := map[string]string{"statsd.percentiles": "75"}
	cfg := NewPreCfg(pre)
	sd := NewStatsdaemon(cfg)

	// Some data with expected 75% of 2
	sd.Timers["response_time"] = []float64{0, 1, 2, 3}

	now := int64(1418052649)

	var buffer bytes.Buffer
	num := sd.ProcessTimers(&buffer, now)

	lines := bytes.Split(buffer.Bytes(), []byte("\n"))

	assert.Equal(t, num, int64(1))
	assert.Equal(t, string(lines[0]), "response_time.upper_75 2 1418052649")
}

func TestProcessTimersUpperPercentilePostfix(t *testing.T) {
	pre := map[string]string{
		"statsd.postfix":     ".test",
		"statsd.percentiles": "75",
	}
	cfg := NewPreCfg(pre)
	sd := NewStatsdaemon(cfg)

	// Some data with expected 75% of 2
	sd.Timers["postfix_response_time.test"] = []float64{0, 1, 2, 3}

	now := int64(1418052649)

	var buffer bytes.Buffer
	num := sd.ProcessTimers(&buffer, now)

	lines := bytes.Split(buffer.Bytes(), []byte("\n"))

	assert.Equal(t, num, int64(1))
	assert.Equal(t, string(lines[0]), "postfix_response_time.upper_75.test 2 1418052649")
	flag.Set("postfix", "")
}

func TestProcessTimesLowerPercentile(t *testing.T) {
	pre := map[string]string{"statsd.percentiles": "-75"}
	cfg := NewPreCfg(pre)
	sd := NewStatsdaemon(cfg)
	sd.Timers["time"] = []float64{0, 1, 2, 3}
	now := int64(1418052649)
	var buffer bytes.Buffer
	num := sd.ProcessTimers(&buffer, now)

	lines := bytes.Split(buffer.Bytes(), []byte("\n"))

	assert.Equal(t, num, int64(1))
	assert.Equal(t, string(lines[0]), "time.lower_75 1 1418052649")
}

func TestStatsDaemonParseLine(t *testing.T) {
	cfg := NewCfg()
	sd := NewStatsdaemon(cfg)
	sd.ParseLine("gorets:100|c")
	assert.Equal(t, float64(100), sd.Counters["gorets"])
	sd.ParseLine("gorets:3|c")
	assert.Equal(t, float64(103), sd.Counters["gorets"])
	sd.ParseLine("gorets:-4|c")
	assert.Equal(t, float64(99), sd.Counters["gorets"])
	sd.ParseLine("gorets:-100|c")
	assert.Equal(t, float64(-1), sd.Counters["gorets"])
	//Gauges
	sd.ParseLine("testGauge:+100|g")
	assert.Equal(t, float64(100), sd.Gauges["testGauge"])

}

func TestStatsDaemonFanOutCounters(t *testing.T) {
	cfg := NewCfg()
	qchan := qtypes.NewQChan()
	sd := NewNamedStatsdaemon("statsd", cfg, qchan)
	qchan.Broadcast()
	dc := qchan.Data.Join()
	sd.ParseLine("gorets:100|c")
	sd.ParseLine("gorets:3|c")
	now := time.Unix(1495028544, 0)
	sd.FanOutCounters(now)
	select {
	case val := <-dc.Read:
		assert.IsType(t, qtypes.Metric{}, val)
		met := val.(qtypes.Metric)
		assert.Equal(t, float64(103), met.Value)
		assert.Equal(t, "gorets", met.Name)
	case <-time.After(500 * time.Millisecond):
		t.Fatal("metrics receive timeout")
	}
	sd.FanOutCounters(now)
	select {
	case val := <-dc.Read:
		assert.IsType(t, qtypes.Metric{}, val)
		met := val.(qtypes.Metric)
		assert.Equal(t, float64(0), met.Value)
		assert.Equal(t, "gorets", met.Name)
	case <-time.After(500 * time.Millisecond):
		t.Fatal("metrics receive timeout")
	}

}

func TestStatsDaemonFanOutGauges(t *testing.T) {
	cfg := NewCfg()
	qchan := qtypes.NewQChan()
	sd := NewNamedStatsdaemon("statsd", cfg, qchan)
	qchan.Broadcast()
	dc := qchan.Data.Join()
	sd.ParseLineSdPkt("testGauge:100|g")
	gid := GenID("testGauge")
	assert.Equal(t, float64(100), sd.Gauges[gid])
	now := time.Unix(1495028544, 0)
	sd.FanOutGauges(now)
	select {
	case val := <-dc.Read:
		assert.IsType(t, qtypes.Metric{}, val)
		met := val.(qtypes.Metric)
		assert.Equal(t, float64(100), met.Value)
		assert.Equal(t, "testGauge", met.Name)
	case <-time.After(1500 * time.Millisecond):
		t.Fatal("metrics receive timeout")
	}
	sd.ParseLineSdPkt("testGauge:-50|g")
	assert.Equal(t, float64(50), sd.Gauges[gid])
	sd.FanOutGauges(now)
	select {
	case val := <-dc.Read:
		assert.IsType(t, qtypes.Metric{}, val)
		met := val.(qtypes.Metric)
		assert.Equal(t, float64(50), met.Value)
		assert.Equal(t, "testGauge", met.Name)
	case <-time.After(1500 * time.Millisecond):
		t.Fatal("metrics receive timeout")
	}
	sd.ParseLineSdPkt("testGauge:10|g")
	assert.Equal(t, float64(10), sd.Gauges[gid])
	sd.ParseLineSdPkt("testGauge:+10|g")
	assert.Equal(t, float64(20), sd.Gauges[gid])
}

func TestStatsDaemonFanOutGaugesDelete(t *testing.T) {
	pre := map[string]string{"statsd.delete-gauges": "true"}
	cfg := NewPreCfg(pre)
	qchan := qtypes.NewQChan()
	sd := NewNamedStatsdaemon("statsd", cfg, qchan)
	qchan.Broadcast()
	dc := qchan.Data.Join()
	sd.ParseLineSdPkt("testGauge:100|g")
	gid := GenID("testGauge")
	assert.Equal(t, float64(100), sd.Gauges[gid])
	now := time.Unix(1495028544, 0)
	sd.FanOutGauges(now)
	select {
	case val := <-dc.Read:
		assert.IsType(t, qtypes.Metric{}, val)
		met := val.(qtypes.Metric)
		assert.Equal(t, float64(100), met.Value)
		assert.Equal(t, "testGauge", met.Name)
	case <-time.After(1500 * time.Millisecond):
		t.Fatal("metrics receive timeout")
	}
	sd.ParseLineSdPkt("testGauge:-50|g")
	assert.Equal(t, float64(0), sd.Gauges[gid])
	sd.FanOutGauges(now)
	select {
	case val := <-dc.Read:
		assert.IsType(t, qtypes.Metric{}, val)
		met := val.(qtypes.Metric)
		assert.Equal(t, float64(0), met.Value)
		assert.Equal(t, "testGauge", met.Name)
	case <-time.After(1500 * time.Millisecond):
		t.Fatal("metrics receive timeout")
	}
}

func TestStatsDaemonFanOutSets(t *testing.T) {
	cfg := NewCfg()
	qchan := qtypes.NewQChan()
	sd := NewNamedStatsdaemon("test", cfg, qchan)
	qchan.Broadcast()
	dc := qchan.Data.Join()
	sd.ParseLine("testSet:100|s")
	assert.Equal(t, 1, len(sd.Sets["testSet"]))
	assert.Equal(t, "100", sd.Sets["testSet"][0])

	now := time.Unix(1495028544, 0)
	sd.FanOutSets(now)
	select {
	case val := <-dc.Read:
		assert.IsType(t, qtypes.Metric{}, val)
		met := val.(qtypes.Metric)
		assert.Equal(t, float64(1), met.Value)
		assert.Equal(t, "testSet", met.Name)
	case <-time.After(1500 * time.Millisecond):
		t.Fatal("metrics receive timeout")
	}
	sd.ParseLine("testSet:100|s")
	sd.ParseLine("testSet:200|s")
	assert.Equal(t, 2, len(sd.Sets["testSet"]))
	assert.Equal(t, "200", sd.Sets["testSet"][1])
	sd.FanOutSets(now)
	select {
	case val := <-dc.Read:
		assert.IsType(t, qtypes.Metric{}, val)
		met := val.(qtypes.Metric)
		assert.Equal(t, float64(2), met.Value)
		assert.Equal(t, "testSet", met.Name)
	case <-time.After(1500 * time.Millisecond):
		t.Fatal("metrics receive timeout")
	}
}

func TestStatsDaemonFanOutTimers(t *testing.T) {
	cfg := NewCfg()
	qchan := qtypes.NewQChan()
	sd := NewNamedStatsdaemon("test", cfg, qchan)
	qchan.Broadcast()
	dc := qchan.Data.Join()
	sd.ParseLine("testTimer:100|ms")
	assert.Equal(t, float64(100), sd.Timers["testTimer"][0])
	now := time.Unix(1495028544, 0)
	sd.FanOutTimers(now)
	exp := map[string]float64{
		"testTimer.upper": 100.0,
		"testTimer.lower": 100.0,
		"testTimer.mean":  100.0,
		"testTimer.count": 1.0,
	}
	tr := NewTimerResult(exp)
	for {
		select {
		case val := <-dc.Read:
			assert.IsType(t, qtypes.Metric{}, val)
			met := val.(qtypes.Metric)
			tr.Input(met.Name, met.Value)
		case <-time.After(1500 * time.Millisecond):
			t.Fatal("timeout")
		}
		if tr.Check() {
			break
		}
	}
	sd.ParseLine("testTimer:100|ms")
	sd.ParseLine("testTimer:200|ms")
	sd.FanOutTimers(now)
	exp = map[string]float64{
		"testTimer.upper": 200.0,
		"testTimer.lower": 100.0,
		"testTimer.mean":  150.0,
		"testTimer.count": 2.0,
	}
	tr = NewTimerResult(exp)
	for {
		select {
		case val := <-dc.Read:
			assert.IsType(t, qtypes.Metric{}, val)
			met := val.(qtypes.Metric)
			tr.Input(met.Name, met.Value)
		case <-time.After(1500 * time.Millisecond):
			t.Fatal("timeout")
		}
		if tr.Check() {
			break
		}
	}
}

func TestStatsDaemonFanOutTimersPercentiles(t *testing.T) {
	pre := map[string]string{"statsd.percentiles": "90"}
	cfg := NewPreCfg(pre)
	qchan := qtypes.NewQChan()
	sd := NewNamedStatsdaemon("statsd", cfg, qchan)
	now := time.Unix(1495028544, 0)
	qchan.Broadcast()
	dc := qchan.Data.Join()
	sd.ParseLine("testTimer:100|ms")
	sd.ParseLine("testTimer:200|ms")
	sd.FanOutTimers(now)
	exp := map[string]float64{
		"testTimer.upper":    200.0,
		"testTimer.upper_90": 200.0,
		"testTimer.lower":    100.0,
		"testTimer.mean":     150.0,
		"testTimer.count":    2.0,
	}
	tr := NewTimerResult(exp)
	for {
		select {
		case val := <-dc.Read:
			assert.IsType(t, qtypes.Metric{}, val)
			met := val.(qtypes.Metric)
			tr.Input(met.Name, met.Value)
		case <-time.After(1500 * time.Millisecond):
			fmt.Println(tr.Result())
			t.Fatal("timeout")
		}
		if tr.Check() {
			break
		}
	}
}

func TestStatsDaemonFanOutTimersMorePercentiles(t *testing.T) {
	pre := map[string]string{
		"statsd.percentiles": "50,90,95,99",
	}
	cfg := NewPreCfg(pre)
	qchan := qtypes.NewQChan()
	sd := NewNamedStatsdaemon("statsd", cfg, qchan)
	now := time.Unix(1495028544, 0)
	qchan.Broadcast()
	dc := qchan.Data.Join()
	sd.ParseLine("testTimer:80|ms")
	sd.ParseLine("testTimer:80|ms")
	sd.ParseLine("testTimer:80|ms")
	sd.ParseLine("testTimer:80|ms")
	sd.ParseLine("testTimer:80|ms")
	sd.ParseLine("testTimer:80|ms")
	sd.ParseLine("testTimer:80|ms")
	sd.ParseLine("testTimer:80|ms")
	sd.ParseLine("testTimer:100|ms")
	sd.ParseLine("testTimer:100|ms")
	sd.ParseLine("testTimer:100|ms")
	sd.ParseLine("testTimer:100|ms")
	sd.ParseLine("testTimer:100|ms")
	sd.ParseLine("testTimer:200|ms")
	sd.FanOutTimers(now)
	exp := map[string]float64{
		"testTimer.upper":    200.0,
		"testTimer.upper_50": 80.0,
		"testTimer.upper_90": 100.0,
		"testTimer.upper_95": 100.0,
		"testTimer.upper_99": 200.0,
		"testTimer.lower":    80.0,
		"testTimer.mean":     95.71428571428571,
		"testTimer.count":    14.0,
	}
	tr := NewTimerResult(exp)
	for {
		select {
		case val := <-dc.Read:
			assert.IsType(t, qtypes.Metric{}, val)
			met := val.(qtypes.Metric)
			tr.Input(met.Name, met.Value)
		case <-time.After(1500 * time.Millisecond):
			fmt.Printf(tr.Result())
			t.Fatal("timeout")
		}
		if tr.Check() {
			break
		}
	}
}

/******************* StatsdPackets
Handling StatsdPackets*/

func TestStatsdPacketHandlerGauge(t *testing.T) {
	cfg := NewCfg()
	sd := NewStatsdaemon(cfg)
	sp := qtypes.NewStatsdPacket("gaugor", "333", "g")
	sd.HandlerStatsdPacket(sp)
	bkey := GenID("gaugor")
	assert.Equal(t, sd.Gauges[bkey], float64(333))

	// -10
	sp.ValFlt = 10
	sp.ValStr = "-"
	sd.HandlerStatsdPacket(sp)
	assert.Equal(t, sd.Gauges[bkey], float64(323))

	// +4
	sp.ValFlt = 4
	sp.ValStr = "+"
	sd.HandlerStatsdPacket(sp)
	assert.Equal(t, sd.Gauges[bkey], float64(327))

	// <0 overflow
	sp.ValFlt = 10
	sp.ValStr = ""
	sd.HandlerStatsdPacket(sp)
	sp.ValFlt = 20
	sp.ValStr = "-"
	sd.HandlerStatsdPacket(sp)
	assert.Equal(t, sd.Gauges[bkey], float64(0))

	// >MaxFloat64 overflow
	sp.ValFlt = float64(math.MaxFloat64 - 10)
	sp.ValStr = ""
	sd.HandlerStatsdPacket(sp)
	sp.ValFlt = 20
	sp.ValStr = "+"
	sd.HandlerStatsdPacket(sp)
	assert.Equal(t, sd.Gauges[bkey], float64(math.MaxFloat64))
}

func TestStatsdPacketHandlerGaugeWithDims(t *testing.T) {
	cfg := NewCfg()
	sd := NewStatsdaemon(cfg)
	sp := qtypes.NewStatsdPacketDims("gaugor", "333", "g", qtypes.NewDimensionsPre(map[string]string{"key1": "val1"}))
	sd.HandlerStatsdPacket(sp)
	bkey := GenID("gaugor_key1=val1")
	assert.Equal(t, sd.Gauges[bkey], float64(333))

	// -10
	sp.ValFlt = 10
	sp.ValStr = "-"
	sd.HandlerStatsdPacket(sp)
	assert.Equal(t, sd.Gauges[bkey], float64(323))

	// +4
	sp.ValFlt = 4
	sp.ValStr = "+"
	sd.HandlerStatsdPacket(sp)
	assert.Equal(t, sd.Gauges[bkey], float64(327))

	// <0 overflow
	sp.ValFlt = 10
	sp.ValStr = ""
	sd.HandlerStatsdPacket(sp)
	sp.ValFlt = 20
	sp.ValStr = "-"
	sd.HandlerStatsdPacket(sp)
	assert.Equal(t, sd.Gauges[bkey], float64(0))

	// >MaxFloat64 overflow
	sp.ValFlt = float64(math.MaxFloat64 - 10)
	sp.ValStr = ""
	sd.HandlerStatsdPacket(sp)
	sp.ValFlt = 20
	sp.ValStr = "+"
	sd.HandlerStatsdPacket(sp)
	assert.Equal(t, sd.Gauges[bkey], float64(math.MaxFloat64))
}

/*
func TestMultipleUDPSends(t *testing.T) {
	ctx := NewCtx(NewSet())
	sd := NewStatsdaemon(ctx)
	addr := "127.0.0.1:8126"

	address, _ := net.ResolveUDPAddr("udp", addr)
	listener, err := net.ListenUDP("udp", address)
	assert.Equal(t, nil, err)

	sd.In = make(chan *Packet, MAX_UNPROCESSED_PACKETS)

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		sd.ParseTo(listener, false)
		wg.Done()
	}()

	conn, err := net.DialTimeout("udp", addr, 50*time.Millisecond)
	assert.Equal(t, nil, err)

	n, err := conn.Write([]byte("deploys.test.myservice:2|c"))
	assert.Equal(t, nil, err)
	assert.Equal(t, len("deploys.test.myservice:2|c"), n)

	n, err = conn.Write([]byte("deploys.test.my:service:2|c"))

	n, err = conn.Write([]byte("deploys.test.myservice:1|c"))
	assert.Equal(t, nil, err)
	assert.Equal(t, len("deploys.test.myservice:1|c"), n)

	select {
	case pack := <-sd.In:
		assert.Equal(t, "deploys.test.myservice", pack.Bucket)
		assert.Equal(t, float64(2), pack.ValFlt)
		assert.Equal(t, "c", pack.Modifier)
		assert.Equal(t, float32(1), pack.Sampling)
	case <-time.After(150 * time.Millisecond):
		t.Fatal("packet receive timeout")
	}

	select {
	case pack := <-sd.In:
		assert.Equal(t, "deploys.test.myservice", pack.Bucket)
		assert.Equal(t, float64(1), pack.ValFlt)
		assert.Equal(t, "c", pack.Modifier)
		assert.Equal(t, float32(1), pack.Sampling)
	case <-time.After(150 * time.Millisecond):
		t.Fatal("packet receive timeout")
	}

	listener.Close()
	wg.Wait()
}
/*
func BenchmarkManyDifferentSensors(t *testing.B) {
	r := rand.New(rand.NewSource(438))
	for i := 0; i < 1000; i++ {
		bucket := "response_time" + strconv.Itoa(i)
		for i := 0; i < 10000; i++ {
			a := float64(r.Uint32() % 1000)
			timers[bucket] = append(timers[bucket], a)
		}
	}

	for i := 0; i < 1000; i++ {
		bucket := "count" + strconv.Itoa(i)
		for i := 0; i < 10000; i++ {
			a := float64(r.Uint32() % 1000)
			counters[bucket] = a
		}
	}

	for i := 0; i < 1000; i++ {
		bucket := "gauge" + strconv.Itoa(i)
		for i := 0; i < 10000; i++ {
			a := float64(r.Uint32() % 1000)
			gauges[bucket] = a
		}
	}

	var buff bytes.Buffer
	now := time.Now().Unix()
	t.ResetTimer()
	processTimers(&buff, now, commonPercentiles)
	processCounters(&buff, now)
	processGauges(&buff, now)
}

*/

func BenchmarkOneBigTimer(t *testing.B) {
	pre := map[string]string{"statsd.percentiles": "99"}
	cfg := NewPreCfg(pre)
	sd := NewStatsdaemon(cfg)

	r := rand.New(rand.NewSource(438))
	bucket := "response_time"
	for i := 0; i < 10000000; i++ {
		a := float64(r.Uint32() % 1000)
		sd.Timers[bucket] = append(sd.Timers[bucket], a)
	}

	var buff bytes.Buffer
	t.ResetTimer()
	sd.ProcessTimers(&buff, time.Now().Unix())
}

func BenchmarkLotsOfTimers(t *testing.B) {
	pre := map[string]string{"statsd.percentiles": "99"}
	cfg := NewPreCfg(pre)
	sd := NewStatsdaemon(cfg)

	r := rand.New(rand.NewSource(438))
	for i := 0; i < 1000; i++ {
		bucket := "response_time" + strconv.Itoa(i)
		for i := 0; i < 10000; i++ {
			a := float64(r.Uint32() % 1000)
			sd.Timers[bucket] = append(sd.Timers[bucket], a)
		}
	}

	var buff bytes.Buffer
	t.ResetTimer()
	sd.ProcessTimers(&buff, time.Now().Unix())
}

func BenchmarkParseLineCounter(b *testing.B) {
	mp := NewMP()
	d1 := []byte("a.key.with-0.dash:4|c|@0.5")
	d2 := []byte("normal.key.space:1|c")

	for i := 0; i < b.N; i++ {
		mp.parseLine(d1)
		mp.parseLine(d2)
	}
}

func BenchmarkParseLineGauge(b *testing.B) {
	mp := NewMP()
	d1 := []byte("gaugor.whatever:333.4|g")
	d2 := []byte("gaugor.whatever:-5|g")

	for i := 0; i < b.N; i++ {
		mp.parseLine(d1)
		mp.parseLine(d2)
	}
}

func BenchmarkParseLineTimer(b *testing.B) {
	mp := NewMP()
	d1 := []byte("glork.some.keyspace:3.7211|ms")
	d2 := []byte("glork.some.keyspace:11223|ms")

	for i := 0; i < b.N; i++ {
		mp.parseLine(d1)
		mp.parseLine(d2)
	}
}

func BenchmarkParseLineSet(b *testing.B) {
	mp := NewMP()
	d1 := []byte("setof.some.keyspace:hiya|s")
	d2 := []byte("setof.some.keyspace:411|s")

	for i := 0; i < b.N; i++ {
		mp.parseLine(d1)
		mp.parseLine(d2)
	}
}

func BenchmarkPacketHandlerCounter(b *testing.B) {
	mp := NewMP()
	cfg := NewCfg()
	sd := NewStatsdaemon(cfg)

	d1 := mp.parseLine([]byte("a.key.with-0.dash:4|c|@0.5"))
	d2 := mp.parseLine([]byte("normal.key.space:1|c"))

	for i := 0; i < b.N; i++ {
		sd.packetHandler(d1)
		sd.packetHandler(d2)
	}
}

func BenchmarkPacketHandlerGauge(b *testing.B) {
	mp := NewMP()
	cfg := NewCfg()
	sd := NewStatsdaemon(cfg)

	d1 := mp.parseLine([]byte("gaugor.whatever:333.4|g"))
	d2 := mp.parseLine([]byte("gaugor.whatever:-5|g"))

	for i := 0; i < b.N; i++ {
		sd.packetHandler(d1)
		sd.packetHandler(d2)
	}
}

func BenchmarkPacketHandlerTimer(b *testing.B) {
	mp := NewMP()
	cfg := NewCfg()
	sd := NewStatsdaemon(cfg)

	d1 := mp.parseLine([]byte("glork.some.keyspace:3.7211|ms"))
	d2 := mp.parseLine([]byte("glork.some.keyspace:11223|ms"))

	for i := 0; i < b.N; i++ {
		sd.packetHandler(d1)
		sd.packetHandler(d2)
	}
}

func BenchmarkPacketHandlerSet(b *testing.B) {
	mp := NewMP()
	cfg := NewCfg()
	sd := NewStatsdaemon(cfg)

	d1 := mp.parseLine([]byte("setof.some.keyspace:hiya|s"))
	d2 := mp.parseLine([]byte("setof.some.keyspace:411|s"))

	for i := 0; i < b.N; i++ {
		if i&0xff == 0xff {
			sd.Sets = make(map[string][]string)
		}
		sd.packetHandler(d1)
		sd.packetHandler(d2)
	}
}
