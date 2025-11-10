package mux

import (
	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/common/serial"
	"github.com/xtls/xray-core/common/session"
)

type Writer struct {
	dest         net.Destination
	writer       buf.Writer
	id           uint16
	followup     bool
	hasError     bool
	transferType protocol.TransferType
	globalID     [8]byte
	inbound      *session.Inbound
	acceptLargePayload bool
}

func NewWriter(id uint16, dest net.Destination, writer buf.Writer, transferType protocol.TransferType, globalID [8]byte, inbound *session.Inbound) *Writer {
	return &Writer{
		id:           id,
		dest:         dest,
		writer:       writer,
		followup:     false,
		transferType: transferType,
		globalID:     globalID,
		inbound:      inbound,
		acceptLargePayload: true,
	}
}

func NewResponseWriter(id uint16, writer buf.Writer, transferType protocol.TransferType, acceptLargePayload bool) *Writer {
	return &Writer{
		id:           id,
		writer:       writer,
		followup:     true,
		transferType: transferType,
		acceptLargePayload: acceptLargePayload,
	}
}

func (w *Writer) getNextFrameMeta() FrameMetadata {
	meta := FrameMetadata{
		SessionID: w.id,
		Target:    w.dest,
		GlobalID:  w.globalID,
		Inbound:   w.inbound,
	}

	if w.followup {
		meta.SessionStatus = SessionStatusKeep
	} else {
		w.followup = true
		meta.SessionStatus = SessionStatusNew
	}

	if w.acceptLargePayload {
		meta.Option.Set(OptionAcceptLargePayload)
	}

	return meta
}

func (w *Writer) writeMetaOnly() error {
	meta := w.getNextFrameMeta()
	b := buf.New()
	if err := meta.WriteTo(b); err != nil {
		return err
	}
	return w.writer.WriteMultiBuffer(buf.MultiBuffer{b})
}

func (w *Writer) writeKeepAlive() error {
	meta := FrameMetadata{
		SessionID:     w.id,
		SessionStatus: SessionStatusKeepAlive,
	}
	b := buf.New()
	if err := meta.WriteTo(b); err != nil {
		return err
	}
	return w.writer.WriteMultiBuffer(buf.MultiBuffer{b})
}

func writeMetaWithFrame(writer buf.Writer, meta FrameMetadata, data buf.MultiBuffer) error {
	frame := buf.New()
	if len(data) == 1 {
		frame.UDP = data[0].UDP
	}
	dataLen := data.Len()
	if dataLen > 65535 {
		meta.Option.Set(OptionLargePayload)
	}
	if err := meta.WriteTo(frame); err != nil {
		return err
	}

	if dataLen > 65535 {
		if _, err := serial.WriteUint32(frame, uint32(dataLen)); err != nil {
			return err
		}
	} else {
		if _, err := serial.WriteUint16(frame, uint16(dataLen)); err != nil {
			return err
		}
	}

	mb2 := make(buf.MultiBuffer, 0, len(data)+1)
	mb2 = append(mb2, frame)
	mb2 = append(mb2, data...)
	return writer.WriteMultiBuffer(mb2)
}

func (w *Writer) writeData(mb buf.MultiBuffer) error {
	meta := w.getNextFrameMeta()
	meta.Option.Set(OptionData)

	return writeMetaWithFrame(w.writer, meta, mb)
}

// WriteMultiBuffer implements buf.Writer.
func (w *Writer) WriteMultiBuffer(mb buf.MultiBuffer) error {
	defer buf.ReleaseMulti(mb)

	if mb.IsEmpty() {
		return w.writeMetaOnly()
	}

	if w.transferType == protocol.TransferTypeStream && w.acceptLargePayload {
		var chunk buf.MultiBuffer
		for i := range mb {
			chunk = append(chunk, mb[i])
			mb[i] = nil
		}
		mb = mb[:0]
		if err := w.writeData(chunk); err != nil {
			return err
		}
		return nil
	}

	for !mb.IsEmpty() {
		var chunk buf.MultiBuffer
		if w.transferType == protocol.TransferTypeStream {
			mb, chunk = buf.SplitSize(mb, 65535)
		} else {
			mb2, b := buf.SplitFirst(mb)
			mb = mb2
			chunk = buf.MultiBuffer{b}
		}
		if err := w.writeData(chunk); err != nil {
			return err
		}
	}

	return nil
}

// Close implements common.Closable.
func (w *Writer) Close() error {
	meta := FrameMetadata{
		SessionID:     w.id,
		SessionStatus: SessionStatusEnd,
	}
	if w.hasError {
		meta.Option.Set(OptionError)
	}

	frame := buf.New()
	common.Must(meta.WriteTo(frame))

	w.writer.WriteMultiBuffer(buf.MultiBuffer{frame})
	return nil
}
