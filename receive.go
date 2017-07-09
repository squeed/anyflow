package main

import (
	"errors"
	"net"

	"github.com/cloudflare/cfssl/log"
	"github.com/jesk78/anyflow/proto/netflow"
)

type Packet struct {
	Raw   []byte
	Saddr *net.UDPAddr
	Proto string
}

func Parse(b []byte, addr *net.UDPAddr) (*Packet, error) {
	p := new(Packet)
	// parse for flow netflowcol
	switch b[1] {
	case 9:
		*p = Packet{Raw: b, Saddr: addr, Proto: "nf9"}
	default:
		return p, errors.New("No flow packet")
	}
	return p, nil
}

func Receive(c *net.UDPConn, out chan<- *netflow.Record) {
	buf := make([]byte, 9000)

	for {
		n, addr, err := c.ReadFromUDP(buf)
		if err != nil {
			log.Errorf("Error: ", err)
			continue
		}

		packetSourceIP := addr.IP.String()
		packetsTotal.WithLabelValues(packetSourceIP).Inc()
		log.Infof("Packet source: ", packetSourceIP)

		p, err := Parse(buf[:n], addr)
		if err != nil {
			log.Errorf("Error parsing packet: ", err)
			continue
		}

		switch p.Proto {
		case "nf9":
			nf, err := netflow.New(p.Raw, p.Saddr)
			if err != nil {
				log.Errorf("Error parsing netflow nf9 packet: ", err)
				continue
			}

			if !nf.HasFlows() {
				log.Debug("No flows in nf9 packet")
				continue
			}

			records, err := nf.GetFlows()
			if err != nil {
				log.Errorf("Error getting flows from packet: ", err)
				continue
			}

			log.Infof("Number of flow packet records: ", len(records))

			for _, r := range records {
				out <- &r
			}
		}
	}
}
