//  
//  Copyright 2023 PayPal Inc.
//  
//  Licensed to the Apache Software Foundation (ASF) under one or more
//  contributor license agreements.  See the NOTICE file distributed with
//  this work for additional information regarding copyright ownership.
//  The ASF licenses this file to You under the Apache License, Version 2.0
//  (the "License"); you may not use this file except in compliance with
//  the License.  You may obtain a copy of the License at
//  
//     http://www.apache.org/licenses/LICENSE-2.0
//  
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//  
//  Package utility provides the utility interfaces for mux package
//  
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
