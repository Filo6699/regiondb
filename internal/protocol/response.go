package protocol

import (
	"strconv"
)

type responseKind uint8

const (
	responseOK responseKind = iota
	responseError
	responseBulk
	responseArray
)

type Response struct {
	kind    responseKind
	payload []byte
	items   [][]byte
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
	case responseArray:
		result := make([]byte, 0, 4+len(r.items)*8)
		result = append(result, '*')
		result = strconv.AppendInt(result, int64(len(r.items)), 10)
		result = append(result, '\r', '\n')
		for _, item := range r.items {
			result = append(result, '$')
			result = strconv.AppendInt(result, int64(len(item)), 10)
			result = append(result, '\r', '\n')
			result = append(result, item...)
			result = append(result, '\r', '\n')
		}
		return result
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

func arrayResponse(items [][]byte) Response {
	cloned := make([][]byte, len(items))
	for index, item := range items {
		cloned[index] = append([]byte(nil), item...)
	}
	return Response{kind: responseArray, items: cloned}
}
