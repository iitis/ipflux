package main

import (
	"net"
)

const UDPCHAN_LENGTH = 1000

type UdpPacket struct {
	payload    []byte
	source     *net.UDPAddr
}

func udpChannel() <-chan *UdpPacket {
	dbg(2, "udpChannel", "will listen on %s", *optListen)

	// resolve
	udpaddr, err := net.ResolveUDPAddr("udp", *optListen)
	if err != nil { dieErr("udpaddr", err) }

	// "connect"
	udpconn, err := net.ListenUDP("udp", udpaddr)
	if err != nil { dieErr("udpconn", err) }

	// start reader
	ch := make(chan *UdpPacket, UDPCHAN_LENGTH)
	go udpReader(udpconn, ch)

	return ch
}

func udpReader(udpconn *net.UDPConn, ch chan *UdpPacket) {
	buf := make([]byte, 65507) // see RFC7011 10.3.3, but use max UDP payload length

	reader: for {
		// try receiving
		n, addr, err := udpconn.ReadFromUDP(buf)
		switch {
		case n == 0:
			dbg(2, "udpReader", "null read")
		case err != nil:
			dbgErr(0, "udpReader", err)
			break reader
		}

		// all ok, repack
		pkt := &UdpPacket{}
		pkt.payload = make([]byte, n)
		copy(pkt.payload, buf)
		pkt.source = addr

		// send
		ch <- pkt
	}

	close(ch)
}
