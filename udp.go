/*
 * ipflux - store IPFIX UDP stream in InfluxDB
 * Copyright (C) 2018-2019 IITiS PAN Gliwice <https://www.iitis.pl/>
 * Author: Pawel Foremski <pjf@iitis.pl>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package main

import (
	"net"
)

func udpSource(addr string, len int) (queue chan *Req, pool chan *Req) {
	dbg(2, "udpSource", "will listen on %s", addr)

	// resolve
	udpaddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil { dieErr("udpaddr", err) }

	// "connect"
	udpconn, err := net.ListenUDP("udp", udpaddr)
	if err != nil { dieErr("udpconn", err) }

	// create memory pool and queue
	queue = make(chan *Req, len)
	pool = make(chan *Req, cap(queue))

	// populate the pool
	for i := 0; i < cap(pool); i++ {
		up := &Req{}
		up.udpBuf = make([]byte, 1500) // see RFC7011 10.3.3, assume Ethernet
		pool <- up
	}

	// start reader
	go udpReader(udpconn, queue, pool)

	return
}

func udpReader(udpconn *net.UDPConn, queue chan *Req, pool chan *Req) {
reader: for up := range pool {
		// try receiving
		n, addr, err := udpconn.ReadFromUDP(up.udpBuf)

		// interpret
		switch {
		case n == 0:
			dbg(2, "udpReader", "null read")
			pool <- up
			continue
		case err != nil:
			dbgErr(0, "udpReader", err)
			break reader
		default: // ok, send for processing
			up.udpPayload = up.udpBuf[0:n]
			up.udpSource = addr.String()
			queue <- up
		}
	}

	close(queue)
}
