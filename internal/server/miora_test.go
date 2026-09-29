package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"workbuddy2api/internal/auth"
	"workbuddy2api/internal/upstream"
)

func TestMioraProxyTempKeyExchangeAndTaskBinding(t *testing.T) {
	var mintCount int32
	var lastAuthHeader string

	mockSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v2/api-keys":
			n := atomic.AddInt32(&mintCount, 1)
			w.Header().Set("Content-Type", "application/json")
			exp := time.Now().Add(15 * time.Minute).Format(time.RFC3339)
			_, _ = w.Write([]byte(`{"code":0,"data":{"key":"ck_t_test_` + string(rune('0'+n)) + `","expires_at":"` + exp + `"}}`))
		case "/api/ai/workbuddy-proxy/video/submit":
			lastAuthHeader = r.Header.Get("Authorization")
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"code":0,"failed":false,"data":{"upstreamTaskId":"v-task-123","model":"minimax-h3-iOA","provider":"copilot-video"},"message":"successful","httpStatus":200}`))
		case "/api/ai/workbuddy-proxy/video/query-task":
			lastAuthHeader = r.Header.Get("Authorization")
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"code":0,"failed":false,"data":{"v-task-123":{"status":"completed","resultFiles":[{"signedUrl":"https://cos.example.com/out.mp4","mimeType":"video/mp4"}]}},"message":"successful","httpStatus":200}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer mockSrv.Close()

	p := testPoolWith(&auth.Auth{
		UID:         "u-cn-1",
		AccessToken: "jwt-token-1",
		ExpiresAt:   time.Now().Add(time.Hour).Unix(),
		Domain:      "www.codebuddy.cn",
	})
	up := upstream.New()
	up.HTTP = mockSrv.Client()
	up.BillingBaseCN = mockSrv.URL

	h := NewHandler(Config{
		Pool:      p,
		Upstream:  up,
		APIKey:    "wb2a-secret",
		MioraBase: mockSrv.URL,
	})

	submitReq := httptest.NewRequest(
		http.MethodPost,
		"/api/ai/workbuddy-proxy/video/submit",
		strings.NewReader(`{"prompt":"sunrise over mountains","duration":5,"resolution":"768P","aspect_ratio":"16:9"}`),
	)
	submitReq.Header.Set("Authorization", "Bearer wb2a-secret")
	submitRec := httptest.NewRecorder()
	h.ServeHTTP(submitRec, submitReq)

	if submitRec.Code != http.StatusOK {
		t.Fatalf("submit status=%d body=%s", submitRec.Code, submitRec.Body.String())
	}
	if lastAuthHeader != "Bearer ck_t_test_1" {
		t.Fatalf("expected temp key header Bearer ck_t_test_1, got %q", lastAuthHeader)
	}

	queryReq := httptest.NewRequest(
		http.MethodPost,
		"/api/ai/workbuddy-proxy/video/query-task",
		strings.NewReader(`{"upstreamTaskIds":["v-task-123"]}`),
	)
	queryReq.Header.Set("Authorization", "Bearer wb2a-secret")
	queryRec := httptest.NewRecorder()
	h.ServeHTTP(queryRec, queryReq)

	if queryRec.Code != http.StatusOK {
		t.Fatalf("query status=%d body=%s", queryRec.Code, queryRec.Body.String())
	}
	if atomic.LoadInt32(&mintCount) != 1 {
		t.Fatalf("expected cached temp key to be reused (mintCount=1), got %d", mintCount)
	}
	var parsed map[string]any
	if err := json.Unmarshal(queryRec.Body.Bytes(), &parsed); err != nil {
		t.Fatalf("unmarshal query response: %v", err)
	}
}
