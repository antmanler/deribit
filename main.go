package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jszwec/csvutil"
	"github.com/refunc/refunc/pkg/utils/cmdutil"
	"github.com/refunc/refunc/pkg/utils/cmdutil/flagtools"
	"github.com/restis/md-deribit/pkg/deribit"
	"github.com/spf13/pflag"
	"k8s.io/klog"
)

var config struct {
	Key      string
	Secret   string
	TestNet  bool
	Interval string
	DataDir  string
}

func init() {
	pflag.StringVar(&config.Key, "access-key", "", "Access key")
	pflag.StringVar(&config.Secret, "secret-key", "", "Secret access key")
	pflag.BoolVar(&config.TestNet, "testnet", false, "Using test net")
	pflag.StringVar(&config.DataDir, "data", "./data", "Folder to store data")
	pflag.StringVar(&config.Interval, "interval", "raw", "raw or 100ms")
}

var states struct {
	subs map[string]*tickSubs
}

func main() {
	runtime.GOMAXPROCS(func() int {
		if runtime.NumCPU() > 128 {
			return runtime.NumCPU()
		}
		return 128
	}())
	rand.Seed(time.Now().UTC().UnixNano())

	flagtools.InitFlags()
	defer klog.Flush()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cli, err := deribit.NewClient(ctx, config.TestNet)
	if err != nil {
		klog.Exitf("failed to create client, %v", err)
	}

	var version struct {
		Version string `json:"version"`
	}

	checkErr(cli.Call("public/test", nil, &version))
	klog.Infof("Connected to Deribit API v%s", version.Version)

	// checkErr(cli.Authenticate(config.Key, config.Secret))

	var instrSet struct {
		sync.RWMutex
		insts map[string]InstrumentInfo
	}
	instrSet.insts = make(map[string]InstrumentInfo)

	var wg sync.WaitGroup

	stream := cli.NewEventStream()

	// init states
	states.subs = make(map[string]*tickSubs)

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer klog.Info("Dispatching exited")

		for {
			select {
			case <-ctx.Done():
				for _, subs := range states.subs {
					subs.Close()
				}
				return

			case <-stream.Changes():
			}
			event, ok := stream.Next().(*deribit.Event)
			if !ok {
				klog.Error("Failed to convert to deribit.Event")
				continue
			}

			subs, has := states.subs[event.Channel]
			if !has {
				inst := func() InstrumentInfo {
					instrSet.RLock()
					defer instrSet.RUnlock()
					return instrSet.insts[event.Channel]
				}()
				s, err := newTickSubs(event.Channel, inst)
				if err != nil {
					klog.Errorf("Failed to create subs for %q, %v", event.Channel, err)
					continue
				}
				states.subs[event.Channel] = s
				subs = s
			}
			if err := subs.Append(event); err != nil {
				klog.Errorf("Failed to append ticks for %q, %v", event.Channel, err)
			}
		}
	}()

	updateSubscription := func() {
		instrSet.Lock()
		defer instrSet.Unlock()

		instruments, err := getInstruments(cli)
		if err != nil {
			klog.Errorf("Failed to qurey instruments, %v", err)
			cancel()
			return
		}

		klog.Infof("Available instruments: %v\n", len(instruments))
		var channels []string
		for _, instr := range instruments {
			c := fmt.Sprintf("book.%s.%s", instr.InstrumentName, config.Interval)
			if _, has := instrSet.insts[c]; !has {
				instrSet.insts[c] = instr
				channels = append(channels, c)
			}
		}
		if len(channels) > 0 {
			klog.Infof("About to subscribe %d instruments", len(channels))
			var subscribed []string
			if err := cli.Call("public/subscribe", map[string][]string{"channels": channels}, &subscribed); err != nil {
				klog.Errorf("Failed to subscribe, %v", err)
				cancel()
				return
			}
			klog.Infof("Subscribed %d instruments", len(subscribed))

			klog.Info("Saving meta")
			folder := filepath.Join(config.DataDir, "meta")
			if err := os.MkdirAll(folder, 0755); err != nil {
				klog.Errorf("Failed to create folder %q, %v", folder, err)
				return
			}
			bts, err := json.Marshal(instrSet.insts)
			if err != nil {
				klog.Errorf("Failed to save meta, %v", err)
				return
			}

			filename := fmt.Sprintf("instruments.%d.json", time.Now().UnixNano()/int64(time.Millisecond))
			if err := ioutil.WriteFile(filepath.Join(folder, filename), bts, 0755); err != nil {
				klog.Errorf("Failed to save meta %q, %v", filename, err)
				return
			}
		}
	}

	wg.Add(1)
	// query new instruments periodically
	go func() {
		defer wg.Done()

		updateSubscription()

		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Hour):
				updateSubscription()
			case <-ticker.C:
				if err := cli.Call("public/test", nil, &version); err != nil {
					klog.Errorf("Failed to send test request, %v", err)
					cancel()
					return
				}
			}
		}
	}()

	klog.Infof(`Received signal "%v", exiting...`, <-cmdutil.GetSysSig())
	cancel()
	wg.Wait()
}

// InstrumentInfo basic insturment information
type InstrumentInfo struct {
	TickSize            float64 `json:"tick_size"`
	SettlementPeriod    string  `json:"settlement_period"`
	QuoteCurrency       string  `json:"quote_currency"`
	MinTradeAmount      float64 `json:"min_trade_amount"`
	Kind                string  `json:"kind"`
	IsActive            bool    `json:"is_active"`
	InstrumentName      string  `json:"instrument_name"`
	ExpirationTimestamp int64   `json:"expiration_timestamp"`
	CreationTimestamp   int64   `json:"creation_timestamp"`
	ContractSize        float64 `json:"contract_size"`
	BaseCurrency        string  `json:"base_currency"`
	Strike              float64 `json:"strike"`
	OptionType          string  `json:"option_type"`
}

func getInstruments(cli *deribit.Client) ([]InstrumentInfo, error) {
	var instruments []InstrumentInfo
	err := cli.Call("public/get_instruments", map[string]interface{}{
		"currency": "BTC",
		"expired":  false,
	}, &instruments)
	return instruments, err
}

// TickEvent is received tick event
type TickEvent struct {
	Timestamp      int64           `json:"timestamp"`
	InstrumentName string          `json:"instrument_name"`
	Bids           [][]interface{} `json:"bids"`
	Asks           [][]interface{} `json:"asks"`
}

// Tick is a single tick of underlying instrument
type Tick struct {
	Type      string  `json:"type"`
	Trigger   string  `json:"trigger"`
	Timestamp int64   `json:"timestamp"`
	Price     float64 `json:"price"`
	Volume    float64 `json:"volume"`
}

func checkErr(err error) {
	if err != nil {
		klog.Exitf("failed on error, %v", err)
	}
}

type tickSubs struct {
	channel string

	nTicks uint64

	mu      sync.Mutex
	opener  func() error
	encoder *csvutil.Encoder
	closer  io.Closer
}

func newTickSubs(c string, instr InstrumentInfo) (*tickSubs, error) {
	folder := filepath.Join(config.DataDir, instr.Kind, instr.InstrumentName)
	if err := os.MkdirAll(folder, 0755); err != nil {
		return nil, err
	}

	ts := &tickSubs{channel: c}

	ts.opener = func() error {
		filename := fmt.Sprintf("%s.%d.csv", ts.channel, time.Now().UnixNano()/int64(time.Millisecond))
		f, err := os.OpenFile(filepath.Join(folder, filename), os.O_RDWR|os.O_CREATE, 0755)
		if err != nil {
			return err
		}
		ts.closer = f
		ts.encoder = csvutil.NewEncoder(csv.NewWriter(f))
		klog.Infof("Start new recording %q", filename)
		return nil
	}

	return ts, nil
}

func (s *tickSubs) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closer != nil {
		closer := s.closer
		s.closer = nil
		if syncer, ok := closer.(interface {
			Sync() error
		}); ok {
			syncer.Sync() // nolint:errcheck
		}
		if n := atomic.LoadUint64(&s.nTicks); n > 0 {
			atomic.StoreUint64(&s.nTicks, 0)
			klog.Infof("Channel %q saved %d ticks", s.channel, n)
		}
		return closer.Close()
	}
	return nil
}

func (s *tickSubs) Append(e *deribit.Event) error {

	var tevt TickEvent
	if err := json.Unmarshal(e.Data, &tevt); err != nil {
		return err
	}

	if len(tevt.Asks) == 0 && len(tevt.Bids) == 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closer == nil {
		if err := s.opener(); err != nil {
			return err
		}
	}

	for _, ask := range tevt.Asks {
		if err := s.encoder.Encode(Tick{
			Type:      "ask",
			Timestamp: tevt.Timestamp,
			Trigger:   ask[0].(string),
			Price:     ask[1].(float64),
			Volume:    ask[2].(float64),
		}); err != nil {
			return err
		}
		atomic.AddUint64(&s.nTicks, 1)
	}

	for _, bid := range tevt.Bids {
		if err := s.encoder.Encode(Tick{
			Type:      "bid",
			Timestamp: tevt.Timestamp,
			Trigger:   bid[0].(string),
			Price:     bid[1].(float64),
			Volume:    bid[2].(float64),
		}); err != nil {
			return err
		}
		atomic.AddUint64(&s.nTicks, 1)
	}

	return nil
}
