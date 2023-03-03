package main

import (
	"fmt"
	"io"
	"strconv"
	"strings"
)

type Item struct {
	reqType     RequestType
	numRequests int
}

type RequestSequence struct {
	items []Item
}

func (i *Item) PrettyPrint(w io.Writer) {
	fmt.Fprintf(w, "\t%d: %s\n", i.numRequests, i.reqType)
}

func (s *RequestSequence) initFromPattern(p string) {
	s.items = nil
	seq := strings.Split(p, ",")
	for _, t := range seq {
		tn := strings.Split(t, ":")
		if len(tn) == 2 {
			n, err := strconv.Atoi(tn[1])
			var rType RequestType
			if err == nil {
				switch strings.ToUpper(tn[0]) {
				case "C":
					rType = kRequestTypeCreate
				case "G":
					rType = kRequestTypeGet
				case "S":
					rType = kRequestTypeSet
				case "U":
					rType = kRequestTypeUpdate
				case "D":
					rType = kRequestTypeDestroy
				default:
					continue
				}
				s.items = append(s.items, Item{rType, n})
			}
		}
	}
}

func (s *RequestSequence) PrettyPrint(w io.Writer) {
	for _, item := range s.items {
		item.PrettyPrint(w)
	}
}
