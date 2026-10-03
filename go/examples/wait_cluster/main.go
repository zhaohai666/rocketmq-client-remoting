// wait_cluster blocks until the nameserver reports a non-empty broker table,
// i.e. until the broker has registered. It exists because "boot success" on the
// broker's stdout only means its local store opened — registration with the
// nameserver happens on a separate thread and lags several seconds behind, so
// a test that starts immediately after the port opens sees an empty cluster
// table and fails for the wrong reason.
//
//	go run ./examples/wait_cluster -ns 127.0.0.1:9876 -timeout 60
//
// Exits 0 as soon as a master broker is visible, 1 on timeout.
package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/zhaohai666/rocketmq-client-remoting/go/client"
)

func main() {
	ns := flag.String("ns", "127.0.0.1:9876", "nameserver address")
	timeout := flag.Int("timeout", 60, "seconds to wait")
	flag.Parse()

	admin := client.NewDefaultMQAdminExt(nil)
	admin.SetNamesrvAddr(*ns)
	admin.SetInstanceName("WAIT_CLUSTER")
	if err := admin.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "wait_cluster: admin start: %v\n", err)
		os.Exit(1)
	}
	defer admin.Shutdown()

	deadline := time.Now().Add(time.Duration(*timeout) * time.Second)
	for {
		ci, err := admin.FetchBrokerClusterInfo()
		if err == nil {
			for _, entry := range ci.BrokerAddrTable {
				if entry != nil {
					if addr, ok := entry.BrokerAddrs[0]; ok && addr != "" {
						fmt.Printf("cluster ready: broker=%s addr=%s\n", entry.BrokerName, addr)
						os.Exit(0)
					}
				}
			}
		}
		if time.Now().After(deadline) {
			fmt.Fprintf(os.Stderr, "wait_cluster: no broker after %ds (lastErr=%v)\n", *timeout, err)
			os.Exit(1)
		}
		time.Sleep(time.Second)
	}
}
