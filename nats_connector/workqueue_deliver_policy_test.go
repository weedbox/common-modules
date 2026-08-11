package nats_connector

import (
	"context"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// DeliverPolicy decides what a durable sees the first time it is created, and
// the answer is not recoverable later: a consumer created at DeliverLast starts
// at the last message matching its filter and everything published before it is
// gone as far as that consumer is concerned. For a consumer whose messages are
// work orders (a teardown request, say) that is silent data loss, hence the
// opt-in — and hence the back-compat care, since jetstream.DeliverAllPolicy is
// the ZERO value and an unset field must keep meaning "last".

func newPolicyRig(t *testing.T) (jetstream.JetStream, *nats.Conn, *nats.StreamInfo) {
	t.Helper()

	opts := natsserver.DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	opts.StoreDir = t.TempDir()

	srv := natsserver.RunServer(&opts)
	if !srv.ReadyForConnections(5 * time.Second) {
		srv.Shutdown()
		t.Fatal("nats test server not ready")
	}
	t.Cleanup(srv.Shutdown)

	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(nc.Close)

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}

	ctx := context.Background()
	if _, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name: "policy", Subjects: []string{"policy.>"},
	}); err != nil {
		t.Fatalf("create stream: %v", err)
	}

	// The legacy nats.JetStreamContext view the config still asks for.
	legacy, err := nc.JetStream()
	if err != nil {
		t.Fatalf("legacy js: %v", err)
	}
	info, err := legacy.StreamInfo("policy")
	if err != nil {
		t.Fatalf("stream info: %v", err)
	}

	return js, nc, info
}

func publishN(t *testing.T, js jetstream.JetStream, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		if _, err := js.Publish(context.Background(), "policy.work", []byte("m")); err != nil {
			t.Fatalf("publish: %v", err)
		}
	}
}

func newPolicyConsumer(t *testing.T, nc *nats.Conn, stream *nats.StreamInfo, name string, policy *jetstream.DeliverPolicy) *WorkQueueConsumer {
	t.Helper()

	config := NewWorkQueueConsumerConfig()
	config.Conn = nc
	config.Stream = stream
	config.ConsumerName = name
	config.Subjects = []string{"policy.work"}
	config.DeliverPolicy = policy

	wqc, err := NewWorkQueueConsumer(config)
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}
	t.Cleanup(wqc.Shutdown)

	return wqc
}

func pending(t *testing.T, wqc *WorkQueueConsumer) uint64 {
	t.Helper()
	info, err := wqc.consumer.Info(context.Background())
	if err != nil {
		t.Fatalf("consumer info: %v", err)
	}
	return info.NumPending
}

// Unset must stay DeliverLast — every caller predating this field relies on it,
// and the zero value of the enum says the opposite.
func TestEnsureConsumer_UnsetPolicyStaysDeliverLast(t *testing.T) {
	js, nc, stream := newPolicyRig(t)
	publishN(t, js, 5)

	wqc := newPolicyConsumer(t, nc, stream, "legacy", nil)

	info, err := wqc.consumer.Info(context.Background())
	if err != nil {
		t.Fatalf("consumer info: %v", err)
	}
	if info.Config.DeliverPolicy != jetstream.DeliverLastPolicy {
		t.Fatalf("unset DeliverPolicy must mean DeliverLastPolicy, got %v", info.Config.DeliverPolicy)
	}
	if got := pending(t, wqc); got != 1 {
		t.Fatalf("DeliverLast must see only the last message, got %d pending", got)
	}
}

// The point of the field: a consumer created after the work was published still
// sees all of it.
func TestEnsureConsumer_DeliverAllSeesTheBacklog(t *testing.T) {
	js, nc, stream := newPolicyRig(t)
	publishN(t, js, 5)

	all := jetstream.DeliverAllPolicy
	wqc := newPolicyConsumer(t, nc, stream, "backlog", &all)

	if got := pending(t, wqc); got != 5 {
		t.Fatalf("DeliverAll must see every published message, got %d pending", got)
	}
}

// The upgrade hazard, and the reason the policy is read off the live consumer
// instead of being sent blindly: the server answers a changed DeliverPolicy
// with "deliver policy can not be updated" (500/10012). A deployment whose
// durable already exists must still start — it simply keeps the policy it has.
func TestEnsureConsumer_ExistingDurableKeepsItsPolicy(t *testing.T) {
	js, nc, stream := newPolicyRig(t)
	publishN(t, js, 5)

	// First boot: the old behaviour, durable created at DeliverLast.
	first := newPolicyConsumer(t, nc, stream, "upgraded", nil)
	if got := pending(t, first); got != 1 {
		t.Fatalf("precondition: DeliverLast should have 1 pending, got %d", got)
	}
	first.Shutdown()

	// Second boot after the code asks for DeliverAll. This must NOT fail.
	all := jetstream.DeliverAllPolicy
	second := newPolicyConsumer(t, nc, stream, "upgraded", &all)

	info, err := second.consumer.Info(context.Background())
	if err != nil {
		t.Fatalf("consumer info: %v", err)
	}
	if info.Config.DeliverPolicy != jetstream.DeliverLastPolicy {
		t.Fatalf("an existing durable must keep its own policy, got %v", info.Config.DeliverPolicy)
	}
}
