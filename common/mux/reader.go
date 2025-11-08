package mux

import (
	"encoding/binary"
	"io"

	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/serial"
)

// PacketReader is an io.Reader that reads whole chunk of Mux frames every time.
type PacketReader struct {
	reader io.Reader
	eof    bool
	dest   *net.Destination
	largePayload bool
}

// NewPacketReader creates a new PacketReader.
func NewPacketReader(reader io.Reader, dest *net.Destination, largePayload bool) *PacketReader {
	return &PacketReader{
		reader: reader,
		eof:    false,
		dest:   dest,
		largePayload: largePayload,
	}
}

// ReadMultiBuffer implements buf.Reader.
func (r *PacketReader) ReadMultiBuffer() (buf.MultiBuffer, error) {
	if r.eof {
		return nil, io.EOF
	}

	var size uint32
	if r.largePayload {
		nSize, err := serial.ReadUint32(r.reader)
		if err != nil {
			return nil, err
		}
		size = nSize
	} else {
		nSize, err := serial.ReadUint16(r.reader)
		if err != nil {
			return nil, err
		}
		size = uint32(nSize)
	}

	if size > buf.Size {
		return nil, errors.New("packet size too large: ", size)
	}

	b := buf.New()
	if _, err := b.ReadFullFrom(r.reader, int32(size)); err != nil {
		b.Release()
		return nil, err
	}
	r.eof = true
	if r.dest != nil && r.dest.Network == net.Network_UDP {
		b.UDP = r.dest
	}
	return buf.MultiBuffer{b}, nil
}

type StreamReader struct {
	reader      *buf.BufferedReader

	leftOverSize int32
	numChunk     uint32
	largePayload bool
}

// NewStreamReader creates a new StreamReader.
func NewStreamReader(reader io.Reader, largePayload bool) *StreamReader {
	r := &StreamReader{
		largePayload: largePayload,
	}
	if breader, ok := reader.(*buf.BufferedReader); ok {
		r.reader = breader
	} else {
		r.reader = &buf.BufferedReader{Reader: buf.NewReader(reader)}
	}

	return r
}

func (r *StreamReader) readSize() (uint32, error) {
	bufSize := 2
	if r.largePayload {
		bufSize = 4
	}
	buffer :=  make([]byte, bufSize)
	if _, err := io.ReadFull(r.reader, buffer); err != nil {
		return 0, err
	}
	if r.largePayload {
		return binary.BigEndian.Uint32(buffer), nil
	}
	return uint32(binary.BigEndian.Uint16(buffer)), nil
}

func (r *StreamReader) ReadMultiBuffer() (buf.MultiBuffer, error) {
	size := r.leftOverSize
	if size == 0 {
		r.numChunk++
		if r.numChunk > 1 {
			return nil, io.EOF
		}
		nextSize, err := r.readSize()
		if err != nil {
			return nil, err
		}
		if nextSize == 0 {
			return nil, io.EOF
		}
		if nextSize >= 1<<31 {
			return nil, errors.New("stream size too large: ", nextSize)
		}
		size = int32(nextSize)
	}
	r.leftOverSize = size

	mb, err := r.reader.ReadAtMost(size)
	if !mb.IsEmpty() {
		r.leftOverSize -= mb.Len()
		return mb, nil
	}
	return nil, err
}
