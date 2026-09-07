package kvsrv

import (
	"testing"
	"time"

	"6.5840/kvsrv1/rpc"
)

type PutRecord struct {
	Client string

	// 客户端调用 Put 的时间。
	CallTime time.Time

	// Put 返回的时间。
	ReturnTime time.Time

	Value string
	Err   rpc.Err
}

func TestTwoClientsSameVersion(t *testing.T) {
	ts := MakeTestKV(t, true)
	defer ts.Cleanup()

	ts.Begin("two clients compete with the same version")

	// 用单独的客户端初始化 key。
	initClerk := ts.MakeClerk()

	// key 不存在时使用 version=0 创建。
	if err := initClerk.Put("x", "initial", 0); err != rpc.OK {
		t.Fatalf("initialize x failed: %v", err)
	}

	value, version, err := initClerk.Get("x")
	if err != rpc.OK {
		t.Fatalf("Get x failed: %v", err)
	}

	if value != "initial" || version != 1 {
		t.Fatalf(
			"initial state = (%q, %d), want (%q, %d)",
			value,
			version,
			"initial",
			1,
		)
	}

	// 创建两个相互独立的 Clerk。
	ck1 := ts.MakeClerk()
	ck2 := ts.MakeClerk()

	// 两个 goroutine 都在 start 上等待，尽量同时开始。
	start := make(chan struct{})
	results := make(chan PutRecord, 2)

	go func() {
		<-start

		record := PutRecord{
			Client:   "C1",
			CallTime: time.Now(),
			Value:    "A",
		}

		record.Err = ck1.Put("x", "A", 1)
		record.ReturnTime = time.Now()

		results <- record
	}()

	go func() {
		<-start

		record := PutRecord{
			Client:   "C2",
			CallTime: time.Now(),
			Value:    "B",
		}

		record.Err = ck2.Put("x", "B", 1)
		record.ReturnTime = time.Now()

		results <- record
	}()

	// 同时释放两个 goroutine。
	close(start)

	r1 := <-results
	r2 := <-results

	finalValue, finalVersion, getErr := initClerk.Get("x")
	if getErr != rpc.OK {
		t.Fatalf("final Get failed: %v", getErr)
	}

	t.Logf(
		"%s: Put(%q, version=1), call=%s, return=%s, err=%v",
		r1.Client,
		r1.Value,
		r1.CallTime.Format("15:04:05.000000"),
		r1.ReturnTime.Format("15:04:05.000000"),
		r1.Err,
	)

	t.Logf(
		"%s: Put(%q, version=1), call=%s, return=%s, err=%v",
		r2.Client,
		r2.Value,
		r2.CallTime.Format("15:04:05.000000"),
		r2.ReturnTime.Format("15:04:05.000000"),
		r2.Err,
	)

	t.Logf(
		"final state: value=%q version=%d",
		finalValue,
		finalVersion,
	)

	// 检查：只能有一个客户端成功。
	successCount := 0
	versionErrorCount := 0
	successValue := ""

	for _, result := range []PutRecord{r1, r2} {
		switch result.Err {
		case rpc.OK:
			successCount++
			successValue = result.Value

		case rpc.ErrVersion:
			versionErrorCount++

		default:
			t.Fatalf(
				"%s returned unexpected error: %v",
				result.Client,
				result.Err,
			)
		}
	}

	if successCount != 1 {
		t.Fatalf(
			"success count = %d, want 1",
			successCount,
		)
	}

	if versionErrorCount != 1 {
		t.Fatalf(
			"ErrVersion count = %d, want 1",
			versionErrorCount,
		)
	}

	// 初始版本是 1，只有一个 Put 成功，因此最终版本是 2。
	if finalVersion != 2 {
		t.Fatalf(
			"final version = %d, want 2",
			finalVersion,
		)
	}

	if finalValue != successValue {
		t.Fatalf(
			"final value = %q, but successful Put wrote %q",
			finalValue,
			successValue,
		)
	}
}
