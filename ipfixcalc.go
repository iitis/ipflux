package main

import (
	"flag"
)

var (
	optDbg    = flag.Int("dbg", 1, "debugging level")
	optListen = flag.String("udp", ":4739", "UDP address to listen on")
)


func main() {
	flag.Parse()
	dbgSet(*optDbg)

	// start ipfix parsers
	ipfix := ipfixInit()

	// listen on UDP
	udpch := udpChannel()

	main: for {
		select {
		case pkt, ok := <-udpch:
			if !ok { dbg(0, "udpch", "channel closed"); break main; }

			dbg(0, "udpch", "hi there! %d bytes from %s", len(pkt.payload), pkt.source.String())
			for i, msg := range ipfix.parsePkt(pkt) {
				dbg(0, "udpch", "msg %d: %#v", i, msg)
			}
		}
	}

	return;
}