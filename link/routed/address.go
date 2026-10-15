// Package routed adapts dial/listen transports into independent gossip streams.
// Each link uses one persistent control connection and fresh data connections.
// Routing tokens identify links; they do not authenticate or authorize peers.
// The supplied transport must provide the security required by the deployment.
package routed

import (
	"context"
	"errors"
	"net"
	"net/netip"
)

// EncodeTCPAddr encodes an address in the upstream 18-byte socket format.
// Scoped IPv6 addresses are rejected because this format cannot preserve scope.
func EncodeTCPAddr(addr netip.AddrPort) ([]byte, error) {
	if !addr.IsValid() || addr.Addr().Zone() != "" {
		return nil, errors.New("invalid or scoped advertised address")
	}
	ip := addr.Addr().As16()
	b := make([]byte, 18)
	copy(b, ip[:])
	b[16] = byte(addr.Port() >> 8)
	b[17] = byte(addr.Port())
	return b, nil
}

// DecodeTCPAddr decodes the upstream socket format and normalizes mapped IPv4.
func DecodeTCPAddr(b []byte) (netip.AddrPort, error) {
	if len(b) != 18 {
		return netip.AddrPort{}, errors.New("invalid socket address length")
	}
	var ip [16]byte
	copy(ip[:], b[:16])
	return netip.AddrPortFrom(netip.AddrFrom16(ip).Unmap(), uint16(b[16])<<8|uint16(b[17])), nil
}

// TCPDial is a Dial implementation for ordinary TCP. Applications that require
// authentication should supply their authenticated transport instead.
func TCPDial(ctx context.Context, address []byte) (net.Conn, error) {
	addr, err := DecodeTCPAddr(address)
	if err != nil {
		return nil, err
	}
	var dialer net.Dialer
	return dialer.DialContext(ctx, "tcp", addr.String())
}
