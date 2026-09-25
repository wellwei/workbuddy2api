package scheduler

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"workbuddy2api/internal/auth"
	"workbuddy2api/internal/pool"
	"workbuddy2api/internal/upstream"
)

// creditStub 模拟 billing 域：global / CN 两条 get-user-resource 路径分开计数，
// 响应体与 fakeUpstream 同一 schema（global /billing/meter/* 与 CN /v2/* 共用解析）。
type creditStub struct {
	globalCalls atomic.Int32
	cnCalls     atomic.Int32
	remain      int64
}

func (f *creditStub) server() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/billing/meter/get-user-resource":
			f.globalCalls.Add(1)
		case r.URL.Path == "/v2/billing/meter/get-user-resource":
			f.cnCalls.Add(1)
		default:
			http.Error(w, "not found", 404)
			return
		}
		w.Write([]byte(`{"code":0,"data":{"Response":{"Data":{"Accounts":[{"CycleCapacitySize":1000,"CycleCapacityRemain":` +
			jsonI64(f.remain) + `,"CycleCapacityUsed":0}]}}}}`))
	}))
}

// newCreditRefreshScheduler 供对账测试组装：单账号 + 指向 stub 的 upstream client。
// 其余任务全部禁用（这些测试直接调 RunCreditRefreshNow，不跑 Run 主循环）。
func newCreditRefreshScheduler(t *testing.T, p *pool.Pool, srv *httptest.Server, global bool) *Scheduler {
	t.Helper()
	up := &upstream.Client{HTTP: srv.Client(), ChatBaseCN: srv.URL, BillingBaseCN: srv.URL}
	if global {
		up.BillingBaseGlobal = srv.URL
		up.GlobalEnabled = true
	}
	return New(Config{
		Pool:                  p,
		Upstream:              up,
		CheckinDisabled:       true,
		TravelDisabled:        true,
		ActivityDisabled:      true,
		KeepaliveDisabled:     true,
		SchoolDisabled:        true,
		CatDisabled:           true,
		CreditRefreshDisabled: false,
	})
}

// TestCreditRefreshSyncsGlobalDriftedCredits 复盘主线（2026-09-25）：global 账号本地
// 基数漂到 0（上游实查 419/500），对账后必须以权威余额纠正——global 无签到体系，
// 这是它唯一的权威刷新来源。同时断言打到 global 路径、CN base 零调用。
func TestCreditRefreshSyncsGlobalDriftedCredits(t *testing.T) {
	f := &creditStub{remain: 419}
	srv := f.server()
	defer srv.Close()

	p := pool.New("")
	// 手工把本地基数扣漂到 0（模拟 NoteModelCost 内插扣减的漂移终态）。
	p.Add(&auth.Auth{UID: "g1", AccessToken: "at", RefreshToken: "rt", ExpiresAt: 9999999999,
		Domain: "www.workbuddy.ai"})
	p.SetCredits("g1", 0)

	s := newCreditRefreshScheduler(t, p, srv, true)
	s.RunCreditRefreshNow()

	st, ok := p.Status("g1")
	if !ok {
		t.Fatal("account g1 missing from pool")
	}
	if st.Credits != 419 {
		t.Errorf("credits after refresh=%d want 419（global 权威余额必须回写）", st.Credits)
	}
	if n := f.globalCalls.Load(); n != 1 {
		t.Errorf("global billing calls=%d want 1", n)
	}
	if n := f.cnCalls.Load(); n != 0 {
		t.Errorf("cn billing calls=%d want 0（global 账号不应打 CN base）", n)
	}
}

// TestCreditRefreshRevivesHardCooledAccount 余额恢复的硬冷却账号必须解冻：
// 14018 硬冷却到次日 04:00 等签到，但试用包/充值随时可能到账——对账发现 remain>0
// 即解冻，不让已恢复的账号干等到期。
func TestCreditRefreshRevivesHardCooledAccount(t *testing.T) {
	f := &creditStub{remain: 500}
	srv := f.server()
	defer srv.Close()

	p := pool.New("")
	p.Add(&auth.Auth{UID: "u1", AccessToken: "at", RefreshToken: "rt", ExpiresAt: 9999999999})
	p.Cooldown("u1", pool.CoolHard, 12*time.Hour, "余额不足")

	s := newCreditRefreshScheduler(t, p, srv, false)
	s.RunCreditRefreshNow()

	st, ok := p.Status("u1")
	if !ok {
		t.Fatal("account u1 missing from pool")
	}
	if st.Cooling {
		t.Errorf("account still cooling: %+v（remain>0 必须解冻）", st)
	}
	if st.Credits != 500 {
		t.Errorf("credits=%d want 500", st.Credits)
	}
}

// TestCreditRefreshZeroRemainKeepsCooldown remain=0 时只回写基数，冷却必须保留：
// 余额没恢复就不能把 14018 硬冷却的账号放回选号池。
func TestCreditRefreshZeroRemainKeepsCooldown(t *testing.T) {
	f := &creditStub{remain: 0}
	srv := f.server()
	defer srv.Close()

	p := pool.New("")
	p.Add(&auth.Auth{UID: "u1", AccessToken: "at", RefreshToken: "rt", ExpiresAt: 9999999999})
	p.Cooldown("u1", pool.CoolHard, 12*time.Hour, "余额不足")

	s := newCreditRefreshScheduler(t, p, srv, false)
	s.RunCreditRefreshNow()

	st, ok := p.Status("u1")
	if !ok {
		t.Fatal("account u1 missing from pool")
	}
	if !st.Cooling {
		t.Errorf("cooling=%v want true（remain=0 不解冻）", st.Cooling)
	}
	if st.Credits != 0 {
		t.Errorf("credits=%d want 0", st.Credits)
	}
}

// TestCreditRefreshSkipsSystemDisabled 系统禁用账号跳过（不发起任何上游调用），
// 与签到同口径。
func TestCreditRefreshSkipsSystemDisabled(t *testing.T) {
	f := &creditStub{remain: 100}
	srv := f.server()
	defer srv.Close()

	p := pool.New("")
	p.Add(&auth.Auth{UID: "u1", AccessToken: "at", RefreshToken: "rt", ExpiresAt: 9999999999})
	p.Disable("u1", "连续失败")

	s := newCreditRefreshScheduler(t, p, srv, false)
	s.RunCreditRefreshNow()

	if n := f.globalCalls.Load() + f.cnCalls.Load(); n != 0 {
		t.Errorf("billing calls=%d want 0（disabled 账号不参与对账）", n)
	}
}

// TestCreditRefreshManualDisabledStillSynced 手动停用账号照常对账：手动停用是
// 「摘流量」不是「冻结」（setManualDisabledLocked 设计注释），积分状态必须保持
// 鲜活——运维重新启用时看到的是真实余额；manualDisabled 位本身不得被对账解除。
func TestCreditRefreshManualDisabledStillSynced(t *testing.T) {
	f := &creditStub{remain: 419}
	srv := f.server()
	defer srv.Close()

	p := pool.New("")
	p.Add(&auth.Auth{UID: "u1", AccessToken: "at", RefreshToken: "rt", ExpiresAt: 9999999999})
	if _, changed := p.SetManualDisabled("u1", true, "运维摘除"); !changed {
		t.Fatal("SetManualDisabled returned changed=false")
	}

	s := newCreditRefreshScheduler(t, p, srv, false)
	s.RunCreditRefreshNow()

	st, ok := p.Status("u1")
	if !ok {
		t.Fatal("account u1 missing from pool")
	}
	if st.Credits != 419 {
		t.Errorf("credits=%d want 419（手动停用账号余额照常对账）", st.Credits)
	}
	if !st.ManualDisabled {
		t.Errorf("manual_disabled=%v want true（对账不得解除运维停用）", st.ManualDisabled)
	}
}

// TestNextWakeIncludesCreditRefresh 默认 hours 下对账有独立时点（与签到 9/21 错峰）。
func TestNextWakeIncludesCreditRefresh(t *testing.T) {
	s := New(Config{
		CheckinHours:      []int{9, 21},
		TravelDisabled:    true,
		ActivityDisabled:  true,
		KeepaliveDisabled: true,
		SchoolDisabled:    true,
		CatDisabled:       true,
	})
	// 00:30 → 最近时点是 02:00 对账（签到 09:00 更远）。
	at, kinds := s.nextWake(time.Date(2026, 9, 26, 0, 30, 0, 0, time.Local))
	if want := time.Date(2026, 9, 26, 2, 0, 0, 0, time.Local); !at.Equal(want) {
		t.Errorf("next=%v want %v", at, want)
	}
	if len(kinds) != 1 || kinds[0] != taskCreditRefresh {
		t.Errorf("kinds=%v want [creditRefresh]", kinds)
	}
}

// TestCreditRefreshTokenRefreshFailureSkipsBilling token 临近过期且刷新失败 →
// 记 WARN 跳过该号（不打 billing 白跑），不喂 session-dead 计数（死亡判定归
// keepalive/签到），其余账号照常。
func TestCreditRefreshTokenRefreshFailureSkipsBilling(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		switch {
		case strings.HasSuffix(r.URL.Path, "/token/refresh"):
			http.Error(w, `{"code":500,"msg":"upstream down"}`, 500)
		case r.URL.Path == "/v2/billing/meter/get-user-resource":
			w.Write([]byte(`{"code":0,"data":{"Response":{"Data":{"Accounts":[{"CycleCapacitySize":100,"CycleCapacityRemain":77,"CycleCapacityUsed":0}]}}}}`))
		default:
			http.Error(w, "not found", 404)
		}
	}))
	defer srv.Close()

	p := pool.New("")
	p.Add(&auth.Auth{UID: "expired", AccessToken: "old", RefreshToken: "rt", ExpiresAt: 1})
	p.Add(&auth.Auth{UID: "healthy", AccessToken: "at", RefreshToken: "rt", ExpiresAt: 9999999999})

	s := newCreditRefreshScheduler(t, p, srv, false)
	s.RunCreditRefreshNow()

	st, ok := p.Status("healthy")
	if !ok {
		t.Fatal("account healthy missing from pool")
	}
	if st.Credits != 77 {
		t.Errorf("healthy credits=%d want 77（单号失败不得阻断遍历）", st.Credits)
	}
	expiredSt, ok := p.Status("expired")
	if !ok {
		t.Fatal("account expired missing from pool")
	}
	if expiredSt.Credits != 0 {
		t.Errorf("expired credits=%d want 0（billing 未成功不得改基数）", expiredSt.Credits)
	}
}
