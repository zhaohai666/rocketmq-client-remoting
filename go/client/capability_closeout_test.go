// The capability gaps closed against the Java 5.x reference client.
//
// Each test pins one Java facade to this port and pairs the new accessor with
// the behavior it steers, so a refactor that keeps the setter but drops the
// wiring goes red here:
//
//   - ConsumeFromWhere: all six values of the Java enum exist. The three
//     "deprecated" ones are not dead -- Java folds them into the
//     CONSUME_FROM_LAST_OFFSET branch of the offset-init switch
//     (RebalancePushImpl#computePullFromWhereWithException lines 171-173).
//   - LitePull connectBrokerByUser / defaultBrokerId: the only way to read from
//     a slave (PullAPIWrapper#recalculatePullFromWhichNode).
//   - Admin adminExtGroup: default "admin_ext_group" plus the Java accessor pair.
package client

import (
	"testing"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
	"github.com/zhaohai666/rocketmq-client-remoting/go/remoting"
)

// javaConsumeFromWhere is the authoritative list from Java
// common/.../consumer/ConsumeFromWhere.java.
var javaConsumeFromWhere = []string{
	"CONSUME_FROM_LAST_OFFSET",
	"CONSUME_FROM_LAST_OFFSET_AND_FROM_MIN_WHEN_BOOT_FIRST",
	"CONSUME_FROM_MIN_OFFSET",
	"CONSUME_FROM_MAX_OFFSET",
	"CONSUME_FROM_FIRST_OFFSET",
	"CONSUME_FROM_TIMESTAMP",
}

func TestConsumeFromWhereHasAllSixJavaValues(t *testing.T) {
	remotingValues := []string{
		remoting.ConsumeFromWhereLastOffset,
		remoting.ConsumeFromWhereLastOffsetAndFromMinWhenBootFirst,
		remoting.ConsumeFromWhereMinOffset,
		remoting.ConsumeFromWhereMaxOffset,
		remoting.ConsumeFromWhereFirstOffset,
		remoting.ConsumeFromWhereTimestamp,
	}
	clientValues := []string{
		ConsumeFromWhereLastOffset,
		ConsumeFromWhereLastOffsetAndFromMinWhenBootFirst,
		ConsumeFromWhereMinOffset,
		ConsumeFromWhereMaxOffset,
		ConsumeFromWhereFirstOffset,
		ConsumeFromWhereTimestamp,
	}
	for i, want := range javaConsumeFromWhere {
		if remotingValues[i] != want {
			t.Fatalf("remoting[%d] = %q, want %q", i, remotingValues[i], want)
		}
		// The listener.go mirror is what application code imports; it must carry
		// the same strings, or a heartbeat would advertise a value the broker
		// cannot parse.
		if clientValues[i] != want {
			t.Fatalf("client[%d] = %q, want %q", i, clientValues[i], want)
		}
	}
}

func TestLitePullBrokerPinDefaultsMatchJava(t *testing.T) {
	api := newPullAPI(nil, "GID_PinDefault")
	if api.isConnectBrokerByUser() {
		t.Fatal("connectBrokerByUser must default to false")
	}
	if got := api.getDefaultBrokerID(); got != int64(common.MasterID) {
		t.Fatalf("defaultBrokerId = %d, want %d", got, common.MasterID)
	}
}

func TestLitePullBrokerPinOverridesTheSuggestionTable(t *testing.T) {
	api := newPullAPI(nil, "GID_PinOverride")
	mq := common.MessageQueue{Topic: "PinTopic", BrokerName: "broker-a", QueueID: 0}
	// The broker suggested a slave; unpinned rounds follow it.
	api.updatePullFromWhichNode(mq, 2)
	if got := api.recalculatePullFromWhichNode(mq); got != 2 {
		t.Fatalf("unpinned round = %d, want the suggested 2", got)
	}
	api.setConnectBrokerByUser(true)
	api.setDefaultBrokerID(1)
	if got := api.recalculatePullFromWhichNode(mq); got != 1 {
		t.Fatalf("pinned round = %d, want 1 (the pin must beat the table)", got)
	}
	// Unpinning restores the table, it does not latch to the pinned id.
	api.setConnectBrokerByUser(false)
	if got := api.recalculatePullFromWhichNode(mq); got != 2 {
		t.Fatalf("after unpin = %d, want the suggested 2", got)
	}
}

func TestLitePullBrokerPinIsSettableBeforeStart(t *testing.T) {
	c, err := NewDefaultLitePullConsumer("GID_PinPreStart")
	if err != nil {
		t.Fatalf("NewDefaultLitePullConsumer: %v", err)
	}
	c.SetConnectBrokerByUser(true)
	c.SetDefaultBrokerID(4)
	if !c.IsConnectBrokerByUser() {
		t.Fatal("IsConnectBrokerByUser must report true before Start")
	}
	if got := c.DefaultBrokerID(); got != 4 {
		t.Fatalf("DefaultBrokerID = %d, want 4", got)
	}
}

func TestAdminExtGroupMatchesJava(t *testing.T) {
	if DefaultAdminExtGroup != "admin_ext_group" {
		t.Fatalf("DefaultAdminExtGroup = %q", DefaultAdminExtGroup)
	}
	admin := NewDefaultMQAdminExt(nil)
	if got := admin.AdminExtGroup(); got != "admin_ext_group" {
		t.Fatalf("default AdminExtGroup = %q", got)
	}
	admin.SetAdminExtGroup("custom-admin-group")
	if got := admin.AdminExtGroup(); got != "custom-admin-group" {
		t.Fatalf("AdminExtGroup after set = %q", got)
	}
}
