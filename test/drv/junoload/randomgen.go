package main

import (
	"math/rand"
	//	"fmt"
)

type (
	RandomGen struct {
		payload       []byte
		randNum       *rand.Rand
		payloadLen    int
		ttl           uint32
		tp            int
		isVariable    bool
		disableGetTTL bool
	}
)

func (p *RandomGen) createPayload() []byte {
	start := rand.Intn(p.payloadLen)
	var end int
	if p.isVariable {
		// Create a ranndom number which follows normal distribution with mean p.payloadLen
		// and standaed deviation of of 30% of p.payloadLen
		length := int(p.randNum.NormFloat64()*float64(p.payloadLen)*0.3 + float64(p.payloadLen))
		if length < 0 {
			length *= -1
		}
		if length > 2*p.payloadLen {
			length = 2 * p.payloadLen
		}
		end = start + length
		//fmt.Println("Variable payload length",length," start",start," end",end," orig payload len",p.payloadLen)
	} else {
		end = start + p.payloadLen
	}
	payload := p.payload[start:end]
	return payload
}

func (p *RandomGen) getThroughPut() int {
	var tp int
	if p.isVariable {
		tp = int(uint32(p.randNum.NormFloat64()*float64(p.tp)*0.3 + float64(p.tp)))
		//fmt.Println("Variable TP",tp)
	} else {
		tp = p.tp
	}
	return tp
}

func (p *RandomGen) getTTL() uint32 {
	var ttl uint32
	if p.isVariable {
		ttl = uint32(p.randNum.NormFloat64()*float64(p.ttl)*0.3 + float64(p.ttl))
		if ttl > 2*p.ttl {
			ttl = 2 * p.ttl
		}
		//fmt.Println("Variable TTL",ttl)
	} else {
		ttl = uint32(p.ttl)
	}
	return ttl
}
