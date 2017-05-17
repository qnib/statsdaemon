package statsdaemon


type Packet struct {
	Bucket   string
	ValFlt   float64
	ValStr   string
	Modifier string
	Sampling float32
}

