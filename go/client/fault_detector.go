package client

import (
	"time"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
	"github.com/zhaohai666/rocketmq-client-remoting/go/remoting"
)

const (
	DefaultDetectTimeoutMillis  = int64(200)
	DefaultDetectIntervalMillis = int64(2 * 1000)
)

func (p *DefaultMQProducer) SetStartDetectorEnable(enable bool) {
	p.faultStrategy.SetStartDetectorEnable(enable)
}

// IsStartDetectorEnable reports the detector switch.
func (p *DefaultMQProducer) IsStartDetectorEnable() bool {
	return p.faultStrategy.StartDetectorEnable()
}

func (p *DefaultMQProducer) SetDetectTimeout(v int64) {
	p.faultStrategy.setDetectTimeout(v)
}

func (p *DefaultMQProducer) SetDetectInterval(v int64) {
	p.faultStrategy.setDetectInterval(v)
}

// ---------------- lifecycle hooks (called by Start/Shutdown) ----------------

func (p *DefaultMQProducer) startDetector() {
	p.mu.Lock()
	inst := p.mqClient
	p.mu.Unlock()
	if inst == nil {
		return
	}
	p.faultStrategy.latencyFaultTolerance.setDetector(
		func(name string) (string, bool) {
			return inst.FindBrokerAddressInPublish(name)
		},
		p.detectByMaxOffset,
	)
	p.detectorRunning.Store(true)
	go p.detectLoop()
}

func (p *DefaultMQProducer) stopDetector() {
	p.detectorRunning.Store(false)
}

func (p *DefaultMQProducer) detectLoop() {
	tolerance := p.faultStrategy.latencyFaultTolerance
	lastRound := time.Now()
	for p.detectorRunning.Load() {
		if time.Since(lastRound) >= time.Duration(tolerance.tickMillis.Load())*time.Millisecond {
			tolerance.detectByOneRoundIfEnabled()
			lastRound = time.Now()
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// ---------------- ServiceDetector ----------------

func (p *DefaultMQProducer) detectByMaxOffset(addr string, timeoutMillis int64) bool {
	if addr == "" {
		return false
	}
	inst, err := p.requireClient()
	if err != nil {
		return false
	}
	topic := p.detectTopic(inst)
	if topic == "" {
		return false
	}
	header := &remoting.GetMaxOffsetRequestHeader{
		Topic:   remoting.StrPtr(topic),
		QueueID: remoting.I32Ptr(0),
	}
	request := remoting.CreateRequestCommand(remoting.ReqGetMaxOffset, header)
	// Probe the same port the sends will use: the address comes from
	// FindBrokerAddressInPublish, exactly like a send address, so with the VIP
	// channel on the probe is shifted too — otherwise the detector would
	// grade the plain port while the traffic goes to the VIP one.
	response, err := inst.invokeSync(inst.brokerVIPAddr(addr), request, timeoutMillis)
	if err != nil {
		common.LogDebugf("detector: GET_MAX_OFFSET probe to %s failed: %v", addr, err)
		return false
	}
	return inst.checkResponse(response) == nil
}

func (p *DefaultMQProducer) detectTopic(inst *Instance) string {
	topics := inst.topicsInUseSnapshot()
	if len(topics) == 0 {
		return ""
	}
	return topics[0]
}
