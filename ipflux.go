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
	"encoding/hex"
	"net"
	"fmt"
	"strings"
	"time"
	"flag"
	"github.com/calmh/ipfix"
	influx "github.com/influxdata/influxdb1-client/v2"
)

var (
	optDbg            = flag.Int("dbg", 1, "debugging level")

	optListen         = flag.String("udp", ":4739", "UDP address to listen on")
	optRcvSize        = flag.Int("rcv", 1000, "Receive packet buffer size")

	optKeyMAC         = flag.Bool("keyMAC", false, "use MAC addresses as InfluxDB tags")
	optKeyInterface   = flag.Bool("keyInterface", false, "use interface name as InfluxDB tag")
	optDropOut        = flag.Bool("dropOut", true, "drop data on \"output\" flow direction")

	optIpfixCache     = flag.Int("ipfixCache", 1000, "IPFIX session cache size")

	optInflux         = flag.String("influx", "http://localhost:8086", "InfluxDB server address")
	optInfluxInsecure = flag.Bool("influxInsecure", false, "skip HTTPS cert validation for InfluxDB")
	optInfluxUser     = flag.String("influxUser", "", "InfluxDB username")
	optInfluxPass     = flag.String("influxPass", "", "InfluxDB password")
	optInfluxTimeout  = flag.Duration("influxTimeout", time.Duration(15 * time.Second), "InfluxDB write timeout")
	optInfluxDB       = flag.String("influxDB", "ipflux", "InfluxDB database name")
	optInfluxRP       = flag.String("influxRP", "", "InfluxDB retention policy")
	optInfluxBatch    = flag.Int("influxBatch", 10000, "max InfluxDB write batch size")
	optInfluxTime     = flag.Duration("influxTime", time.Duration(time.Second), "InfluxDB write interval")
)

type Req struct {
	udpBuf          []byte    // raw UDP Read() buffer
	udpPayload      []byte    // udpBuf re-sliced
	udpSource       string    // udp packet source address

	// valid after ifxFind()
	ifxStream       *IfxStream

	// valid after ifxParseAll()
	ifxMsgs         []ipfix.Message

	// valid after ifxHeader()
	ifxTime         time.Time
	ifxDomainId     uint32
	ifxSeqNum       uint32

	// valid after ifxInterpet()
	ifxFields       []ipfix.InterpretedField
}

// fmtIPv6 returns IPv6 address in full form as string
func fmtIPv6(ip *net.IP) string {
	var ret strings.Builder

	dst := make([]byte, 32)
	_ = hex.Encode(dst, *ip)
	
	ret.Grow(8*4 + 7)
	ret.Write(dst[0:4]);   ret.WriteByte(':')
	ret.Write(dst[4:8]);   ret.WriteByte(':')
	ret.Write(dst[8:12]);  ret.WriteByte(':')
	ret.Write(dst[12:16]); ret.WriteByte(':')
	ret.Write(dst[16:20]); ret.WriteByte(':')
	ret.Write(dst[20:24]); ret.WriteByte(':')
	ret.Write(dst[24:28]); ret.WriteByte(':')
	ret.Write(dst[28:32])

	return ret.String()
}

// fmtMAC returns MAC address in full form as string
func fmtMAC(mac []byte) string {
	var ret strings.Builder

	dst := make([]byte, 12)
	_ = hex.Encode(dst, mac)
	
	ret.Grow(6*2 + 5)
	ret.Write(dst[0:2]);   ret.WriteByte(':')
	ret.Write(dst[2:4]);   ret.WriteByte(':')
	ret.Write(dst[4:6]);   ret.WriteByte(':')
	ret.Write(dst[6:8]);   ret.WriteByte(':')
	ret.Write(dst[8:10]);  ret.WriteByte(':')
	ret.Write(dst[10:12])

	return ret.String()
}

// rewrite rewrites an IPFIX data record into an InfluxDB point; drops non-IP data
func (req *Req) rewrite(rec *ipfix.DataRecord) (*influx.Point, error) {
	fields := req.ifxInterpret(rec)
	if len(fields) == 0 { return nil, fmt.Errorf("no fields") }

	isIP := false
	tags := make(map[string]string)
	vals := make(map[string]interface{})

	// TODO: filter/switch by numeric IDs?
	for _, field := range fields {
		// store a few fields also as InfluxDB tags (indexed, always a string value)
		switch field.Name {
		case "sourceIPv4Address":
			isIP = true
			v := field.Value.(*net.IP).String()
			tags["src_ip"] = v
			vals[field.Name] = v
			continue
		case "destinationIPv4Address":
			isIP = true
			v := field.Value.(*net.IP).String()
			tags["dst_ip"] = v
			vals[field.Name] = v
			continue

		case "sourceIPv6Address":
			isIP = true
			v := fmtIPv6(field.Value.(*net.IP))
			tags["src_ip"] = v
			vals[field.Name] = v
			continue
		case "destinationIPv6Address":
			isIP = true
			v := fmtIPv6(field.Value.(*net.IP))
			tags["dst_ip"] = v
			vals[field.Name] = v
			continue

		case "sourceMacAddress":
			v := fmtMAC(field.Value.([]byte))
			if *optKeyMAC { tags["src_mac"] = v }
			vals[field.Name] = v
			continue
		case "destinationMacAddress":
			v := fmtMAC(field.Value.([]byte))
			if *optKeyMAC { tags["dst_mac"] = v }
			vals[field.Name] = v
			continue

		case "flowDirection":
			dir := field.Value.(uint8)

			// drop output reports?
			if *optDropOut {
				if dir == 1 { return nil, nil } // ignore whole record
				continue // ignore field
			}

			// use as key
			switch dir {
			case 0: tags["dir"] = "in"
			case 1: tags["dir"] = "out"
			}

		case "interfaceName":
			if *optKeyInterface {
				tags["ifname"] = field.Value.(string)
			}

		case "observationPointId":       tags["obs_id"] = fmt.Sprintf("%d", field.Value)
		case "protocolIdentifier":       tags["proto"] = fmt.Sprintf("%d", field.Value)
		case "sourceTransportPort":      tags["src_port"] = fmt.Sprintf("%d", field.Value)
		case "destinationTransportPort": tags["dest_port"] = fmt.Sprintf("%d", field.Value)
		}

		// store everything as InfluxDB fields (ordinary, non-indexed values)
		switch v := field.Value.(type) {
		case uint64:
			// for now (01/2019), avoid using the 'u' value suffix (see InfluxDB issue #7801)
			vals[field.Name] = int64(v)
		case []byte:
			vals[field.Name] = hex.EncodeToString(v)
		default:
			vals[field.Name] = v
		}
	}

	// drop non-IP && empty
	if !isIP || len(vals) == 0 { return nil, nil }

	// store template ID, just for fun
	vals["templateId"] = rec.TemplateID

	// rewrite as influx point
	return influx.NewPoint(fmt.Sprintf("domain%d", req.ifxDomainId), tags, vals, req.ifxTime)
}

// read_udp reads UDP packets, interprets them as IPFIX, and rewrites to InfluxDB points
func read_udp(input chan *Req, pool chan *Req, ifx *Ifx, points chan *influx.Point) {
	// process packets
	for req := range input {
		dbg(5, "read_udp", "UDP packet: %d bytes from %s", len(req.udpPayload), req.udpSource)

		// find ipfix stream, parse all messages
		ifx.ifxFind(req)
		err := req.ifxParseAll()
		if err != nil {
			dbg(1, "read_udp", "ifxParseAll(): %s", err)
			continue
		}

		// go through each IPFIX message
		for _, msg := range req.ifxMsgs {
			// interpet the header
			req.ifxHeader(&msg)

			// rewrite each IPFIX record as InfluxDB point
			for j := range msg.DataRecords {
				p, err := req.rewrite(&msg.DataRecords[j])
				if err != nil {
					dbg(1, "read_udp", "rewrite(): %s", err)
					continue
				}
				if p != nil {
					dbg(5, "read_udp", "new point: %s", p.String())
					points <- p
				}
			}
		}
		
		// recycle memory
		pool <- req
	}

	dbg(1, "read_udp", "channel closed, exiting")
}

func main() {
	flag.Parse()
	dbgSet(*optDbg)

	// start ipfix parsers
	ifx, err := NewIfx(*optIpfixCache)
	if err != nil { die("main", "NewIfx", err) }

	// listen on UDP
	udpq, udpmem := udpSource(*optListen, *optRcvSize)

	// rewrite UDP packets into InfluxDB points
	points := make(chan *influx.Point, 2 * *optInfluxBatch)
	go read_udp(udpq, udpmem, ifx, points)

	// prepare influx stuff
	ticker := time.NewTicker(*optInfluxTime)

	cconfig := influx.HTTPConfig{
		Addr: *optInflux,
		Timeout: *optInfluxTimeout,
		InsecureSkipVerify: *optInfluxInsecure,
	}
	if len(*optInfluxUser) > 0 { cconfig.Username = *optInfluxUser }
	if len(*optInfluxPass) > 0 { cconfig.Password = *optInfluxPass }
	client, err := influx.NewHTTPClient(cconfig)
	if err != nil { die("main", "NewHTTPClient", err) }

	bpconfig := influx.BatchPointsConfig{ Database: *optInfluxDB }
	if len(*optInfluxRP) > 0 { bpconfig.RetentionPolicy = *optInfluxRP }
	batch, err := influx.NewBatchPoints(bpconfig)
	if err != nil { die("main", "NewBatchPoints", err) }

	// write to InfluxDB at least once per time interval
	bsize := 0
	for {
		select {
		case p, ok := <-points:
			if !ok { break }

			batch.AddPoint(p)
			bsize++

			if bsize < *optInfluxBatch { continue }

		case <-ticker.C:
			// it's time - anything ready?
			if bsize == 0 { continue }
		}

		// try writing
		err = client.Write(batch)
		if err != nil {
			dbg(0, "main", "InfluxDB Write(): %s", err)
		} else {
			dbg(1, "main", "wrote %d points to InfluxDB", bsize)
		}

		// start new batch
		batch, err = influx.NewBatchPoints(bpconfig)
		if err != nil { die("main", "NewBatchPoints", err) }
		bsize = 0
	}
}
