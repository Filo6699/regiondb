package protocol

import (
	"strconv"
)

type responseKind uint8

const (
	responseOK responseKind = iota
	responseError
	responseBulk
)

type Response struct {
	kind    responseKind
	payload []byte
}

func (r Response) Bytes() []byte {
	switch r.kind {
	case responseOK:
		result := make([]byte, 0, len(r.payload)+6)
		result = append(result, "+OK"...)
		if len(r.payload) != 0 {
			result = append(result, ' ')
			result = append(result, r.payload...)
		}
		return append(result, '\r', '\n')
	case responseError:
		result := make([]byte, 0, len(r.payload)+7)
		result = append(result, "-ERR "...)
		result = append(result, r.payload...)
		return append(result, '\r', '\n')
	case responseBulk:
		length := strconv.Itoa(len(r.payload))
		result := make([]byte, 0, len(length)+len(r.payload)+5)
		result = append(result, '$')
		result = append(result, length...)
		result = append(result, '\r', '\n')
		result = append(result, r.payload...)
		return append(result, '\r', '\n')
	default:
		return []byte("-ERR INTERNAL invalid response\r\n")
	}
}

func okResponse(detail string) Response {
	return Response{kind: responseOK, payload: []byte(detail)}
}

func errorResponse(code, detail string) Response {
	payload := make([]byte, 0, len(code)+len(detail)+1)
	payload = append(payload, code...)
	if detail != "" {
		payload = append(payload, ' ')
		payload = append(payload, detail...)
	}
	return Response{kind: responseError, payload: payload}
}

func bulkResponse(payload []byte) Response {
	return Response{kind: responseBulk, payload: append([]byte(nil), payload...)}
}
