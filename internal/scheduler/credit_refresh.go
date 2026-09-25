// credit_refresh.go — 积分基数对账（第七类排程）。
//
// 背景（2026-09-25 wb2a-global 误判复盘）：CN 账号的权威余额每天随签到（09/21 点）
// 刷新两次，两次之间由 pool.NoteModelCost 按实测扣费内插扣减；**global 账号无签到
// 体系**（CheckinAll 的 D4 门控直接跳过），基数一旦因内插误差漂移——实测一个账号
// 从真实 419/500 漂到 state 里的 0——就永远不会被纠正，运维按面板误判账号死活，
// 14018 硬冷却的账号领了新试用包也没有自动解冻的出口。
//
// 本任务对全量账号（CN + global）只做一件事：查权威余额 → ReenableIfCredits
// （余额恢复则解冻冷却域）+ SetCreditsDetailed（刷新基数与快过期桶）。不做签到/
// 上报等任何写操作；billing 查询是轻量 GET，默认每 4 小时一趟，量级可忽略。
//
// 账号取舍与签到同口径：跳过系统禁用（st.Disabled）；**手动停用不跳**——手动停用
// 是「对话流量摘除」不是「账号冻结」，排程任务照常执行（见 pool.setManualDisabledLocked
// 的设计注释），停用期间的积分状态保持鲜活，运维重新启用时拿到的是真实余额。
package scheduler

import (
	"log"

	"workbuddy2api/internal/logfmt"
)

// RunCreditRefreshNow 立即执行一轮积分基数对账（定时入口与测试共用）。
// 无返回值：单账号失败只记 WARN 不影响遍历（与各任务「单账号失败不阻断」同口径），
// 结果以日志汇总；调用方关心数值时读 pool.List() 的 Credits。
func (s *Scheduler) RunCreditRefreshNow() {
	s.runCreditRefresh()
}

// runCreditRefresh 一趟对账：token 按需补刷新 → 权威余额 → 解冻 + 回写基数。
func (s *Scheduler) runCreditRefresh() {
	statuses := s.cfg.Pool.List()
	var okN, failN, skipN, reviveN int
	for _, st := range statuses {
		if st.Disabled {
			skipN++
			continue
		}
		a := s.cfg.Pool.AuthByUID(st.UID)
		if a == nil || a.RefreshTokenValue() == "" {
			skipN++
			continue
		}
		// token 临近过期先补一次刷新（与签到同窗口 checkinRefreshSkew），否则
		// billing 查询必然 401 白跑。刷新失败不喂 session-dead 计数：账号死亡的
		// 判定归 keepalive/签到这两个以 token 健康为主责的任务，本任务只管余额，
		// 失败记 WARN 等下一趟即可。
		if a.NeedsRefresh(checkinRefreshSkew) {
			if err := s.cfg.Upstream.RefreshToken(a); err != nil {
				log.Printf("credit-refresh %s refresh: %v", logfmt.Label(st.UID, st.Nickname), err)
				if a.NeedsRefresh(0) {
					failN++
					continue
				}
			} else {
				a.BackfillRealm() // 老 auth 空 realm → 落盘前补标识（幂等：已有不动）
				if err := a.SaveAtomic(); err != nil {
					log.Printf("credit-refresh %s save: %v", logfmt.Label(st.UID, st.Nickname), err)
				}
			}
		}
		// 分桶查权威余额：快过期窗口与签到共用 ExpiringSoonWindow，pool 据此优先
		// 消耗快过期积分（同一口径，两处写入口不产生两套语义）。
		remain, buckets, err := s.cfg.Upstream.UserResourceDetailed(a, s.cfg.ExpiringSoonWindow)
		if err != nil {
			log.Printf("credit-refresh %s: %v", logfmt.Label(st.UID, st.Nickname), err)
			failN++
			continue
		}
		// 与签到同序：先解冻（余额恢复时清冷却域并写 credits），再补快过期桶。
		// wasCooling 取自 List 快照（Cooling 含熔断/降权域）：ReenableIfCredits 只清
		// 冷却域不动熔断，故计数语义是「尝试解冻」，日志括注说明边界。
		wasCooling := st.Cooling && remain > 0
		s.cfg.Pool.ReenableIfCredits(st.UID, remain)
		s.cfg.Pool.SetCreditsDetailed(st.UID, remain, buckets.Expiring)
		if wasCooling {
			reviveN++
			log.Printf("credit-refresh %s: remain=%d 余额恢复，已解冻冷却账号（熔断域不受影响）",
				logfmt.Label(st.UID, st.Nickname), remain)
		}
		okN++
	}
	log.Printf("credit-refresh done: total=%d ok=%d fail=%d skip=%d revived=%d",
		len(statuses), okN, failN, skipN, reviveN)
}
