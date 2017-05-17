package main

import (
	"os"
	"github.com/codegangsta/cli"
	"github.com/zpatrick/go-config"
	"github.com/ChristianKniep/statsdaemon/lib"
)

const (
 	VERSION = "0.7.1"
)

func Run(ctx *cli.Context) {
	cfg := config.NewYAMLFile(ctx.String("config")
	sd := statsdaemon.NewStatsdaemon(cfg)
	sd.Run()
}

func main() {
	app := cli.NewApp()
	app.Name = "Port of Etsy's statsd, written in Go (originally based amir/gographite."
	app.Usage = "statsdaemon [options]"
	app.Version = VERSION
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "address",
			Value: ":8125",
			Usage: "UDP service address",
		},
		cli.StringFlag{
			Name:  "tcpaddr",
			Value: "",
			Usage: "TCP service address",
		},
		cli.IntFlag{
			Name:  "max-udp-packet-size",
			Value: 1472,
			Usage: "Maximum UDP packet size",
		},
		cli.StringFlag{
			Name:  "graphite",
			Value: "127.0.0.1:2003",
			Usage: "Graphite service address (or - to disable)",
		},
		cli.IntFlag{
			Name:  "flush-interval",
			Value: 10,
			Usage: "Flush interval (seconds)",
		},
		cli.BoolFlag{
			Name:  "debug",
			Usage: "print statistics sent to graphite",
		},
		cli.BoolFlag{
			Name:  "resent-gauges",
			Usage: "do sent values to graphite for inactive gauges, as opposed to not sending the previous value",
		},
		cli.IntFlag{
			Name:  "persist-count-keys",
			Value: 60,
			Usage: "number of flush-intervals to persist count keys",
		},
		cli.StringFlag{
			Name:  "receive-counter",
			Value: "",
			Usage: "Metric name for total metrics received per interval",
		},
		cli.StringFlag{
			Name:  "prefix",
			Value: "",
			Usage: "Prefix for all stats",
		},
		cli.StringFlag{
			Name:  "postfix",
			Value: "",
			Usage: "Postfix for all stats",
		},
		cli.StringFlag{
			Name:  "percentiles",
			Value: "",
			Usage: "Comma separated list of percentiles",
		},
	}
	app.Action = Run
	app.Run(os.Args)
}



