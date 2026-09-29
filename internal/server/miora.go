package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"workbuddy2api/internal/auth"
	"workbuddy2api/internal/logfmt"
	"workbuddy2api/internal/upstream"
)

const (
	defaultMioraBase      = "https://public.miora.qq.com"
	mioraTempKeyTTL       = 900 * time.Second
	mioraTempKeySkew      = 90 * time.Second
	mioraTaskBindingTTL   = 24 * time.Hour
	mioraTaskBindingLimit = 4096
)

type mioraTempKey struct {
	key       string
	expiresAt time.Time
}

type mioraTaskBinding struct {
	uid       string
	createdAt time.Time
}

type mioraState struct {
	mu           sync.Mutex
	tempKeys     map[string]mioraTempKey
	taskBindings map[string]mioraTaskBinding
}

func (m *mioraState) getCachedKey(uid string, now time.Time) (string, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.tempKeys == nil {
		return "", false
	}
	entry, ok := m.tempKeys[uid]
	if !ok || entry.key == "" {
		return "", false
	}
	if now.Add(mioraTempKeySkew).After(entry.expiresAt) {
		delete(m.tempKeys, uid)
		return "", false
	}
	return entry.key, true
}

func (m *mioraState) setCachedKey(uid, key string, expiresAt time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.tempKeys == nil {
		m.tempKeys = make(map[string]mioraTempKey)
	}
	m.tempKeys[uid] = mioraTempKey{
		key:       key,
		expiresAt: expiresAt,
	}
}

func (m *mioraState) invalidateKey(uid string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.tempKeys != nil {
		delete(m.tempKeys, uid)
	}
}

func (m *mioraState) bindTask(taskID, uid string, now time.Time) {
	taskID = strings.TrimSpace(taskID)
	if taskID == "" || uid == "" {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.taskBindings == nil {
		m.taskBindings = make(map[string]mioraTaskBinding)
	}
	if len(m.taskBindings) >= mioraTaskBindingLimit {
		for k, v := range m.taskBindings {
			if now.Sub(v.createdAt) > mioraTaskBindingTTL {
				delete(m.taskBindings, k)
			}
		}
	}
	m.taskBindings[taskID] = mioraTaskBinding{
		uid:       uid,
		createdAt: now,
	}
}

func (m *mioraState) lookupTaskUID(taskID string, now time.Time) string {
	taskID = strings.TrimSpace(taskID)
	if taskID == "" {
		return ""
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.taskBindings == nil {
		return ""
	}
	entry, ok := m.taskBindings[taskID]
	if !ok {
		return ""
	}
	if now.Sub(entry.createdAt) > mioraTaskBindingTTL {
		delete(m.taskBindings, taskID)
		return ""
	}
	return entry.uid
}

func (h *Handler) mioraBaseURL() string {
	if strings.TrimSpace(h.cfg.MioraBase) != "" {
		return strings.TrimRight(strings.TrimSpace(h.cfg.MioraBase), "/")
	}
	return defaultMioraBase
}

func (h *Handler) mioraBillingBaseURL() string {
	if h.cfg.Upstream != nil && strings.TrimSpace(h.cfg.Upstream.BillingBaseCN) != "" {
		return strings.TrimRight(strings.TrimSpace(h.cfg.Upstream.BillingBaseCN), "/")
	}
	return "https://www.codebuddy.cn"
}

func (h *Handler) mioraHTTPClient() *http.Client {
	if h.cfg.Upstream != nil && h.cfg.Upstream.HTTP != nil {
		return h.cfg.Upstream.HTTP
	}
	return &http.Client{Timeout: 120 * time.Second}
}

func (h *Handler) ensureAccountToken(acct *auth.Auth) error {
	if acct == nil {
		return errors.New("nil account")
	}
	if !acct.NeedsRefresh(h.cfg.RefreshSkew) {
		return nil
	}
	if h.cfg.Upstream == nil {
		return nil
	}
	if err := h.cfg.Upstream.RefreshToken(acct); err != nil {
		var ue *upstream.Error
		if errors.As(err, &ue) && ue.Kind == upstream.ErrSessionDead {
			h.cfg.Pool.NoteSessionDead(acct.UID)
		} else {
			h.cfg.Pool.NoteError(acct.UID)
		}
		return err
	}
	acct.BackfillRealm()
	if err := acct.SaveAtomic(); err != nil {
		log.Printf("ERR: [server] miora refresh acct=%s: save auth failed: %v", logfmt.Label(acct.UID, acct.Nickname), err)
	}
	return nil
}

func (h *Handler) getOrMintMioraTempKey(ctx context.Context, acct *auth.Auth) (string, error) {
	now := time.Now()
	if key, ok := h.miora.getCachedKey(acct.UID, now); ok {
		return key, nil
	}
	if err := h.ensureAccountToken(acct); err != nil {
		return "", fmt.Errorf("refresh token: %w", err)
	}
	reqBody := []byte(`{"name":"workbuddy-connect-cloud-service","expire_in_seconds":900,"temporary":true}`)
	endpoint := h.mioraBillingBaseURL() + "/v2/api-keys"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(reqBody))
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+acct.AccessTokenValue())
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	resp, err := h.mioraHTTPClient().Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", err
	}
	if resp.StatusCode >= 400 {
		return "", fmt.Errorf("mint temp key status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(raw)))
	}
	var env struct {
		Code    int    `json:"code"`
		Msg     string `json:"msg"`
		Message string `json:"message"`
		Data    struct {
			Key       string `json:"key"`
			ExpiresAt string `json:"expires_at"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &env); err != nil {
		return "", fmt.Errorf("parse temp key response: %w", err)
	}
	if env.Code != 0 || strings.TrimSpace(env.Data.Key) == "" {
		msg := env.Message
		if msg == "" {
			msg = env.Msg
		}
		return "", fmt.Errorf("mint temp key code=%d msg=%s", env.Code, msg)
	}
	expiresAt := now.Add(mioraTempKeyTTL)
	if parsed, err := time.Parse(time.RFC3339, strings.TrimSpace(env.Data.ExpiresAt)); err == nil {
		expiresAt = parsed
	}
	h.miora.setCachedKey(acct.UID, env.Data.Key, expiresAt)
	return env.Data.Key, nil
}

func isMioraInvalidTempKey(status int, raw []byte) bool {
	if status == http.StatusUnauthorized {
		return true
	}
	body := string(raw)
	return strings.Contains(body, "AUTH_INVALID_TEMP_KEY") || strings.Contains(body, "invalid temporary key")
}

func (h *Handler) callMioraWithAccount(ctx context.Context, acct *auth.Auth, path string, body []byte) (int, []byte, error) {
	for attempt := 0; attempt < 2; attempt++ {
		tempKey, err := h.getOrMintMioraTempKey(ctx, acct)
		if err != nil {
			return 0, nil, err
		}
		targetURL := h.mioraBaseURL() + path
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, targetURL, bytes.NewReader(body))
		if err != nil {
			return 0, nil, err
		}
		req.Header.Set("Authorization", "Bearer "+tempKey)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept", "application/json")

		resp, err := h.mioraHTTPClient().Do(req)
		if err != nil {
			return 0, nil, err
		}
		raw, readErr := io.ReadAll(io.LimitReader(resp.Body, 10<<20))
		resp.Body.Close()
		if readErr != nil {
			return 0, nil, readErr
		}
		if attempt == 0 && isMioraInvalidTempKey(resp.StatusCode, raw) {
			h.miora.invalidateKey(acct.UID)
			continue
		}
		return resp.StatusCode, raw, nil
	}
	return 0, nil, errors.New("miora temp key retry exhausted")
}

type mioraSubmitEnvelope struct {
	Code       int    `json:"code"`
	Failed     bool   `json:"failed"`
	HttpStatus int    `json:"httpStatus"`
	Message    string `json:"message"`
	Data       struct {
		UpstreamTaskID string `json:"upstreamTaskId"`
	} `json:"data"`
}

func (h *Handler) mioraProxy(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 32<<20))
	if err != nil {
		writeOpenAIError(w, http.StatusBadRequest, "invalid_request", "read body: "+err.Error())
		return
	}

	if strings.HasSuffix(r.URL.Path, "/query-task") {
		h.mioraQueryTask(w, r, body)
		return
	}
	h.mioraSubmitTask(w, r, body)
}

func (h *Handler) mioraQueryTask(w http.ResponseWriter, r *http.Request, body []byte) {
	var peek struct {
		UpstreamTaskIDs []string `json:"upstreamTaskIds"`
	}
	_ = json.Unmarshal(body, &peek)

	now := time.Now()
	var acct *auth.Auth
	for _, taskID := range peek.UpstreamTaskIDs {
		if uid := h.miora.lookupTaskUID(taskID, now); uid != "" {
			acct = h.cfg.Pool.AuthByUID(uid)
			if acct != nil {
				break
			}
		}
	}
	if acct == nil {
		acct = h.cfg.Pool.PickExcludingForRealm(nil, "", "cn")
	}
	if acct == nil {
		writeOpenAIError(w, http.StatusServiceUnavailable, "no_healthy_account", "no available CN account for miora query-task")
		return
	}

	status, raw, err := h.callMioraWithAccount(r.Context(), acct, r.URL.Path, body)
	if err != nil {
		writeOpenAIError(w, http.StatusBadGateway, "miora_upstream_error", err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(raw)
}

func (h *Handler) mioraSubmitTask(w http.ResponseWriter, r *http.Request, body []byte) {
	tried := make(map[string]bool)
	var (
		lastStatus int
		lastRaw    []byte
		lastErr    error
	)

	for i := 0; i < h.cfg.MaxRotate; i++ {
		acct := h.cfg.Pool.PickExcludingForRealm(tried, "", "cn")
		if acct == nil {
			break
		}
		tried[acct.UID] = true

		status, raw, err := h.callMioraWithAccount(r.Context(), acct, r.URL.Path, body)
		if err != nil {
			lastErr = err
			h.cfg.Pool.NoteFailures(acct.UID)
			continue
		}
		lastStatus = status
		lastRaw = raw

		var env mioraSubmitEnvelope
		_ = json.Unmarshal(raw, &env)

		if status == http.StatusOK && env.Code == 0 && !env.Failed {
			if env.Data.UpstreamTaskID != "" {
				h.miora.bindTask(env.Data.UpstreamTaskID, acct.UID, time.Now())
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(raw)
			return
		}

		if status == http.StatusBadRequest || env.HttpStatus == http.StatusBadRequest {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(status)
			_, _ = w.Write(raw)
			return
		}

		h.cfg.Pool.NoteFailures(acct.UID)
	}

	if len(lastRaw) > 0 {
		if lastStatus == 0 {
			lastStatus = http.StatusBadGateway
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(lastStatus)
		_, _ = w.Write(lastRaw)
		return
	}
	if lastErr != nil {
		writeOpenAIError(w, http.StatusBadGateway, "miora_upstream_error", lastErr.Error())
		return
	}
	writeOpenAIError(w, http.StatusServiceUnavailable, "no_healthy_account", "no available CN account for miora submit")
}
