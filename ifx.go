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
	"time"
	"github.com/calmh/ipfix"
	"github.com/hashicorp/golang-lru"
)

type Ifx struct {
	sources     *lru.Cache  // holds IfxStream
}

type IfxStream struct {
	sess         *ipfix.Session
	intp         *ipfix.Interpreter
}

func NewIfx(size int) (ifx *Ifx, err error) {
	ifx = &Ifx{}
	ifx.sources, err = lru.New(size)
	return
}

func (ifx *Ifx) ifxFind(req *Req) {
	key := req.udpSource

	// find session
	val, ok := ifx.sources.Get(key)
	if ok {
		req.ifxStream = val.(*IfxStream)
	} else {
		//dbg(1, "findSource", "creating new IPFIX session for '%s'", key)
		req.ifxStream = &IfxStream{}
		req.ifxStream.sess = ipfix.NewSession()
		req.ifxStream.intp = ipfix.NewInterpreter(req.ifxStream.sess)
		ifx.sources.Add(key, req.ifxStream)
	}
}

func (req *Req) ifxParseAll() error {
	var err error
	req.ifxMsgs, err = req.ifxStream.sess.ParseBufferAll(req.udpPayload)
	return err
}

// ifxHeader interprets IPFIX message header
func (req *Req) ifxHeader(msg *ipfix.Message) {
	req.ifxTime = time.Unix(int64(msg.Header.ExportTime), 0).UTC()
	req.ifxSeqNum = msg.Header.SequenceNumber
	req.ifxDomainId = msg.Header.DomainID
}

// ifxInterpret interprets an IPFIX record
func (req *Req) ifxInterpret(rec *ipfix.DataRecord) []ipfix.InterpretedField {
	if len(req.ifxFields) < cap(req.ifxFields) {
		req.ifxFields = req.ifxFields[0:cap(req.ifxFields)]
	}

	// try interpreting; handles fixFields==nil, may return nil as well
	fields := req.ifxStream.intp.InterpretInto(*rec, req.ifxFields)
	if fields != nil {
		req.ifxFields = fields
	}

	return fields
}
