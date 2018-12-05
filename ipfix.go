package main

import (
	"github.com/calmh/ipfix"
)

type Ipfix struct {
	// TODO: LRU with eviction
	sessions map[string]*ipfix.Session
}

func ipfixInit() *Ipfix {
	ifx := &Ipfix{}
	ifx.sessions = make(map[string]*ipfix.Session)
	return ifx
}

// TODO: sure to return a slice of bytes instead of a pointer?
func (ifx *Ipfix) parsePkt(pkt *UdpPacket) []ipfix.Message {
	// find session
	key := pkt.source.String()
	ss, ok := ifx.sessions[key]
	if !ok {
		dbg(1, "parsePkt/sessions", "creating new IPFIX session for '%s'", key)
		ss = ipfix.NewSession()
		ifx.sessions[key] = ss
	}

	// try parsing
	msgs, err := ss.ParseBufferAll(pkt.payload)
	if err != nil {
		dbgErr(1, "parsePkt/parser", err)
		return nil
	}

	return msgs
}
