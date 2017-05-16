package statsdaemon

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/zpatrick/go-config"
	"io"
	"log"
	"math"
	"net"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"time"
	"syscall"
	"github.com/qnib/qframe-types"
)

const (
	MAX_UNPROCESSED_PACKETS = 1000
	TCP_READ_SIZE           = 4096
)

type StatsDaemon struct {
	Name			string
	Parser			MsgParser
	Signalchan 		chan os.Signal
	Cfg   			*config.Config
	In          	chan *Packet
	Counters		map[string]float64
	Gauges			map[string]float64
	Timers			map[string]Float64Slice
	CountInactivity	map[string]int64
	Sets 			map[string][]string
	ReceiveCounter	string
	QChan			qtypes.QChan


}

func NewStatsdaemon(cfg *config.Config) StatsDaemon {
	sd := StatsDaemon{
		Name:				"statsd",
		Parser: 			MsgParser{debug: true},
		Signalchan: 		make(chan os.Signal, 1),
		Cfg: 				cfg,
		In: 				make(chan *Packet, MAX_UNPROCESSED_PACKETS),
		Counters: 			make(map[string]float64),
		Gauges:          	make(map[string]float64),
		Timers:          	make(map[string]Float64Slice),
		CountInactivity:	make(map[string]int64),
		Sets:   			make(map[string][]string),
	}
	sd.ReceiveCounter, _ = sd.Cfg.StringOr("receive-counter", "")
	return sd

}

func NewNamedStatsdaemon(name string, cfg *config.Config, qchan qtypes.QChan) StatsDaemon {
	sd := NewStatsdaemon(cfg)
	sd.Name = name
	sd.QChan = qchan
	return sd
}

func (sd *StatsDaemon) StringOr(path, alt string) string {
	res, err := sd.Cfg.String(path)
	if err != nil {
		res = alt
	}
	return res
}

func (sd *StatsDaemon) String(path string) string {
	return sd.StringOr(path, "")
}

func (sd *StatsDaemon) Bool(path string) bool {
	return sd.BoolOr(path, false)
}

func (sd *StatsDaemon) BoolOr(path string, alt bool) bool {
	res, err := sd.Cfg.Bool(path)
	if err != nil {
		res = alt
	}
	return res
}

func (sd *StatsDaemon) IntOr(path string, alt int) int {
	res, err := sd.Cfg.Int(path)
	if err != nil {
		res = alt
	}
	return res
}

func (sd *StatsDaemon) Int(path string) int {
	return sd.IntOr(path, 0)
}

func (sd *StatsDaemon) Run() {
	signal.Notify(sd.Signalchan, syscall.SIGTERM)
	go sd.startUDPListener()
	go sd.startTCPListener()
	sd.monitor()
}

func (sd *StatsDaemon) startUDPListener() {
	serviceAddress  := sd.String("address")
	address, _ := net.ResolveUDPAddr("udp", serviceAddress)
	log.Printf("listening on %s", address)
	listener, err := net.ListenUDP("udp", address)
	if err != nil {
		log.Fatalf("ERROR: ListenUDP - %s", err)
	}
	sd.ParseTo(listener, false)
}

func (sd *StatsDaemon) startTCPListener() {
	serviceAddress := sd.StringOr("tcpaddr", "")
	if serviceAddress == "" {
		return
	}
	address, _ := net.ResolveTCPAddr("tcp", serviceAddress)
	log.Printf("listening on %s", address)
	listener, err := net.ListenTCP("tcp", address)
	if err != nil {
		log.Fatalf("ERROR: ListenTCP - %s", err)
	}
	defer listener.Close()

	for {
		conn, err := listener.AcceptTCP()
		if err != nil {
			log.Fatalf("ERROR: AcceptTCP - %s", err)
		}
		go sd.ParseTo(conn, true)
	}
}

func (sd *StatsDaemon) ParseTo(conn io.ReadCloser, partialReads bool) {
	defer conn.Close()

	maxUdpPacketSize := sd.Int("max-udp-packet-size")
	prefix := sd.String("prefix")
	postfix := sd.String("postfix")
	debug := sd.Bool("debug")
	parser := NewParser(conn, partialReads, debug, maxUdpPacketSize, prefix, postfix)
	for {
		p, more := parser.Next()
		if p != nil {
			sd.In <- p
		}

		if !more {
			break
		}
	}
}

func (sd *StatsDaemon) monitor() {
	flushInterval := sd.Int("flush-interval")
	period := time.Duration(flushInterval) * time.Second
	ticker := time.NewTicker(period)
	for {
		select {
		case sig := <-sd.Signalchan:
			fmt.Printf("!! Caught signal %v... shutting down\n", sig)
			if err := sd.submit(time.Now().Add(period)); err != nil {
				log.Printf("ERROR: %s", err)
			}
			return
		case <-ticker.C:
			if err := sd.submit(time.Now().Add(period)); err != nil {
				log.Printf("ERROR: %s", err)
			}
		case s := <-sd.In:
			sd.packetHandler(s)
		}
	}
}

func (sd *StatsDaemon) packetHandler(s *Packet) {
	if sd.ReceiveCounter != "" {
		v, ok := sd.Counters[sd.ReceiveCounter]
		if !ok || v < 0 {
			sd.Counters[sd.ReceiveCounter] = 0
		}
		sd.Counters[sd.ReceiveCounter] += 1
	}

	switch s.Modifier {
	case "ms":
		_, ok := sd.Timers[s.Bucket]
		if !ok {
			var t Float64Slice
			sd.Timers[s.Bucket] = t
		}
		sd.Timers[s.Bucket] = append(sd.Timers[s.Bucket], s.ValFlt)
	case "g":
		gaugeValue, _ := sd.Gauges[s.Bucket]

		if s.ValStr == "" {
			gaugeValue = s.ValFlt
		} else if s.ValStr == "+" {
			// watch out for overflows
			if s.ValFlt > (math.MaxFloat64 - gaugeValue) {
				gaugeValue = math.MaxFloat64
			} else {
				gaugeValue += s.ValFlt
			}
		} else if s.ValStr == "-" {
			// subtract checking for negative numbers
			if s.ValFlt > gaugeValue {
				gaugeValue = 0
			} else {
				gaugeValue -= s.ValFlt
			}
		}

		sd.Gauges[s.Bucket] = gaugeValue
	case "c":
		_, ok := sd.Counters[s.Bucket]
		if !ok {
			sd.Counters[s.Bucket] = 0
		}
		sd.Counters[s.Bucket] += s.ValFlt * float64(1/s.Sampling)
	case "s":
		_, ok := sd.Sets[s.Bucket]
		if !ok {
			sd.Sets[s.Bucket] = make([]string, 0)
		}
		sd.Sets[s.Bucket] = append(sd.Sets[s.Bucket], s.ValStr)
	}
}

func (sd *StatsDaemon) SubmitMetrics() (err error) {
	now := time.Now()
	sd.FanOutCounters(now)
	return
}

func (sd *StatsDaemon) FanOutMetrics() {
	now := time.Now()
	sd.FanOutCounters(now)

}

func (sd *StatsDaemon) ParseLine(msg string) (err error) {
	p := sd.Parser.parseLine([]byte(msg))
	sd.packetHandler(p)
	return
}

func (sd *StatsDaemon) FanOutCounters(now time.Time) int64 {
	var num int64
	// continue sending zeros for counters for a short period of time even if we have no new data
	dims := map[string]string{}
	for bucket, value := range sd.Counters {
		m := qtypes.NewExt(sd.Name, bucket, qtypes.Counter, value, dims, now, false)
		sd.QChan.Data.Send(m)
		delete(sd.Counters, bucket)
		sd.CountInactivity[bucket] = 0
		num++
	}
	for bucket, purgeCount := range sd.CountInactivity {
		if purgeCount > 0 {
			m := qtypes.NewExt(sd.Name, bucket, qtypes.Counter, 0.0, dims, now, false)
			sd.QChan.Data.Send(m)
			num++
		}
		sd.CountInactivity[bucket] += 1
		if sd.CountInactivity[bucket] > int64(sd.Int("persist-count-keys")) {
			delete(sd.CountInactivity, bucket)
		}
	}
	return num
}

type Packet struct {
	Bucket   string
	ValFlt   float64
	ValStr   string
	Modifier string
	Sampling float32
}

func sanitizeBucket(bucket string) string {
	b := make([]byte, len(bucket))
	var bl int

	for i := 0; i < len(bucket); i++ {
		c := bucket[i]
		switch {
		case (c >= byte('a') && c <= byte('z')) || (c >= byte('A') && c <= byte('Z')) || (c >= byte('0') && c <= byte('9')) || c == byte('-') || c == byte('.') || c == byte('_'):
			b[bl] = c
			bl++
		case c == byte(' '):
			b[bl] = byte('_')
			bl++
		case c == byte('/'):
			b[bl] = byte('-')
			bl++
		}
	}
	return string(b[:bl])
}

func (sd *StatsDaemon) submit(deadline time.Time) (err error) {
	var buffer bytes.Buffer
	var num int64

	graphiteAddress := sd.String("graphite")

	if graphiteAddress == "-" {
		return
	}
	now := time.Now().Unix()

	client, err := net.Dial("tcp", graphiteAddress)
	if err != nil {
		if sd.Bool("debug") {
			log.Printf("WARNING: resetting counters when in debug mode")
			sd.ProcessCounters(&buffer, now)
			sd.ProcessGauges(&buffer, now)
			//sd.ProcessTimers(&buffer, now, percentThreshold)
			sd.ProcessSets(&buffer, now)
		}
		errmsg := fmt.Sprintf("dialing %s failed - %s", graphiteAddress, err)
		return errors.New(errmsg)
	}
	defer client.Close()

	err = client.SetDeadline(deadline)
	if err != nil {
		return err
	}

	num += sd.ProcessCounters(&buffer, now)
	num += sd.ProcessGauges(&buffer, now)
	//num += processTimers(&buffer, now, percentThreshold)
	num += sd.ProcessSets(&buffer, now)
	if num == 0 {
		return nil
	}

	if sd.Bool("debug") {
		for _, line := range bytes.Split(buffer.Bytes(), []byte("\n")) {
			if len(line) == 0 {
				continue
			}
			log.Printf("DEBUG: %s", line)
		}
	}

	_, err = client.Write(buffer.Bytes())
	if err != nil {
		errmsg := fmt.Sprintf("failed to write stats - %s", err)
		return errors.New(errmsg)
	}

	log.Printf("sent %d stats to %s", num, graphiteAddress)

	return
}

func (sd *StatsDaemon) ProcessCounters(buffer *bytes.Buffer, now int64) int64 {
	var num int64
	// continue sending zeros for counters for a short period of time even if we have no new data
	for bucket, value := range sd.Counters {
		fmt.Fprintf(buffer, "%s %s %d\n", bucket, strconv.FormatFloat(value, 'f', -1, 64), now)
		delete(sd.Counters, bucket)
		sd.CountInactivity[bucket] = 0
		num++
	}
	for bucket, purgeCount := range sd.CountInactivity {
		if purgeCount > 0 {
			fmt.Fprintf(buffer, "%s 0 %d\n", bucket, now)
			num++
		}
		sd.CountInactivity[bucket] += 1
		if sd.CountInactivity[bucket] > int64(sd.Int("persist-count-keys")) {
			delete(sd.CountInactivity, bucket)
		}
	}
	return num
}

func (sd *StatsDaemon) ProcessGauges(buffer *bytes.Buffer, now int64) int64 {
	var num int64

	for bucket, currentValue := range sd.Gauges {
		fmt.Fprintf(buffer, "%s %s %d\n", bucket, strconv.FormatFloat(currentValue, 'f', -1, 64), now)
		num++
		if ! sd.Bool("resent-gauges") {
			delete(sd.Gauges, bucket)
		}
	}
	return num
}

func (sd *StatsDaemon) ProcessSets(buffer *bytes.Buffer, now int64) int64 {
	num := int64(len(sd.Sets))
	for bucket, set := range sd.Sets {

		uniqueSet := map[string]bool{}
		for _, str := range set {
			uniqueSet[str] = true
		}

		fmt.Fprintf(buffer, "%s %d %d\n", bucket, len(uniqueSet), now)
		delete(sd.Sets, bucket)
	}
	return num
}

func (sd *StatsDaemon) ProcessTimers(buffer *bytes.Buffer, now int64, pctls Percentiles) int64 {
	var num int64
	postfix := sd.String("postfix")
	for bucket, timer := range sd.Timers {
		bucketWithoutPostfix := bucket[:len(bucket)-len(postfix)]
		num++

		sort.Sort(timer)
		min := timer[0]
		max := timer[len(timer)-1]
		maxAtThreshold := max
		count := len(timer)

		sum := float64(0)
		for _, value := range timer {
			sum += value
		}
		mean := sum / float64(len(timer))

		for _, pct := range pctls {
			if len(timer) > 1 {
				var abs float64
				if pct.float >= 0 {
					abs = pct.float
				} else {
					abs = 100 + pct.float
				}
				// poor man's math.Round(x):
				// math.Floor(x + 0.5)
				indexOfPerc := int(math.Floor(((abs / 100.0) * float64(count)) + 0.5))
				if pct.float >= 0 {
					indexOfPerc -= 1 // index offset=0
				}
				maxAtThreshold = timer[indexOfPerc]
			}

			var tmpl string
			var pctstr string
			if pct.float >= 0 {
				tmpl = "%s.upper_%s%s %s %d\n"
				pctstr = pct.str
			} else {
				tmpl = "%s.lower_%s%s %s %d\n"
				pctstr = pct.str[1:]
			}
			threshold_s := strconv.FormatFloat(maxAtThreshold, 'f', -1, 64)
			fmt.Fprintf(buffer, tmpl, bucketWithoutPostfix, pctstr, postfix, threshold_s, now)
		}

		mean_s := strconv.FormatFloat(mean, 'f', -1, 64)
		max_s := strconv.FormatFloat(max, 'f', -1, 64)
		min_s := strconv.FormatFloat(min, 'f', -1, 64)

		fmt.Fprintf(buffer, "%s.mean%s %s %d\n", bucketWithoutPostfix, postfix, mean_s, now)
		fmt.Fprintf(buffer, "%s.upper%s %s %d\n", bucketWithoutPostfix, postfix, max_s, now)
		fmt.Fprintf(buffer, "%s.lower%s %s %d\n", bucketWithoutPostfix, postfix, min_s, now)
		fmt.Fprintf(buffer, "%s.count%s %d %d\n", bucketWithoutPostfix, postfix, count, now)

		delete(sd.Timers, bucket)
	}
	return num
}

type MsgParser struct {
	reader       		io.Reader
	buffer       		[]byte
	partialReads 		bool
	done         		bool
	debug				bool
	maxUdpPacketSize 	int
	prefix 				string
	postfix				string

}

func NewParser(reader io.Reader, partialReads, debug bool, maxUdpPacketSize int, prefix, postfix string) *MsgParser {
	return &MsgParser{
		reader, []byte{},
		partialReads, false, debug,
		maxUdpPacketSize,
		prefix, postfix}
}

func (mp *MsgParser) Next() (*Packet, bool) {
	buf := mp.buffer

	for {
		line, rest := mp.lineFrom(buf)

		if line != nil {
			mp.buffer = rest
			return mp.parseLine(line), true
		}

		if mp.done {
			return mp.parseLine(rest), false
		}

		idx := len(buf)
		end := idx
		if mp.partialReads {
			end += TCP_READ_SIZE
		} else {
			end += int(mp.maxUdpPacketSize)
		}
		if cap(buf) >= end {
			buf = buf[:end]
		} else {
			tmp := buf
			buf = make([]byte, end)
			copy(buf, tmp)
		}

		n, err := mp.reader.Read(buf[idx:])
		buf = buf[:idx+n]
		if err != nil {
			if err != io.EOF {
				log.Printf("ERROR: %s", err)
			}

			mp.done = true

			line, rest = mp.lineFrom(buf)
			if line != nil {
				mp.buffer = rest
				return mp.parseLine(line), len(rest) > 0
			}

			if len(rest) > 0 {
				return mp.parseLine(rest), false
			}

			return nil, false
		}
	}
}

func (mp *MsgParser) lineFrom(input []byte) ([]byte, []byte) {
	split := bytes.SplitAfterN(input, []byte("\n"), 2)
	if len(split) == 2 {
		return split[0][:len(split[0])-1], split[1]
	}

	if !mp.partialReads {
		if len(input) == 0 {
			input = nil
		}
		return input, []byte{}
	}

	if bytes.HasSuffix(input, []byte("\n")) {
		return input[:len(input)-1], []byte{}
	}

	return nil, input
}

func (mp *MsgParser) parseLine(line []byte) *Packet {
	split := bytes.SplitN(line, []byte{'|'}, 3)
	if len(split) < 2 {
		mp.logParseFail(line)
		return nil
	}

	keyval := split[0]
	typeCode := string(split[1])

	sampling := float32(1)
	if strings.HasPrefix(typeCode, "c") || strings.HasPrefix(typeCode, "ms") {
		if len(split) == 3 && len(split[2]) > 0 && split[2][0] == '@' {
			f64, err := strconv.ParseFloat(string(split[2][1:]), 32)
			if err != nil {
				log.Printf(
					"ERROR: failed to ParseFloat %s - %s",
					string(split[2][1:]),
					err,
				)
				return nil
			}
			sampling = float32(f64)
		}
	}

	split = bytes.SplitN(keyval, []byte{':'}, 2)
	if len(split) < 2 {
		mp.logParseFail(line)
		return nil
	}
	name := string(split[0])
	val := split[1]
	if len(val) == 0 {
		mp.logParseFail(line)
		return nil
	}

	var (
		err      error
		floatval float64
		strval   string
	)

	switch typeCode {
	case "c":
		floatval, err = strconv.ParseFloat(string(val), 64)
		if err != nil {
			log.Printf("ERROR: failed to ParseFloat %s - %s", string(val), err)
			return nil
		}
	case "g":
		var s string

		if val[0] == '+' || val[0] == '-' {
			strval = string(val[0])
			s = string(val[1:])
		} else {
			s = string(val)
		}
		floatval, err = strconv.ParseFloat(s, 64)
		if err != nil {
			log.Printf("ERROR: failed to ParseFloat %s - %s", string(val), err)
			return nil
		}
	case "s":
		strval = string(val)
	case "ms":
		floatval, err = strconv.ParseFloat(string(val), 64)
		if err != nil {
			log.Printf("ERROR: failed to ParseFloat %s - %s", string(val), err)
			return nil
		}
	default:
		log.Printf("ERROR: unrecognized type code %q", typeCode)
		return nil
	}

	return &Packet{
		Bucket:   sanitizeBucket(mp.prefix + string(name) + mp.postfix),
		ValFlt:   floatval,
		ValStr:   strval,
		Modifier: typeCode,
		Sampling: sampling,
	}
}

func (mp *MsgParser) logParseFail(line []byte) {
	if mp.debug {
		log.Printf("ERROR: failed to parse line: %q\n", string(line))
	}
}
