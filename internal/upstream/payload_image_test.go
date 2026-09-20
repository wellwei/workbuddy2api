package upstream

import (
	"bytes"
	"encoding/json"
	"testing"
)

// 提取 body 里所有 image_url part 的 image_url 值形态。
func imageParts(t *testing.T, body []byte) []any {
	t.Helper()
	var obj map[string]any
	if err := json.Unmarshal(body, &obj); err != nil {
		t.Fatalf("输出不可解析: %v", err)
	}
	msgs, _ := obj["messages"].([]any)
	var out []any
	for _, m := range msgs {
		mm, _ := m.(map[string]any)
		parts, _ := mm["content"].([]any)
		for _, p := range parts {
			pp, _ := p.(map[string]any)
			if t, _ := pp["type"].(string); t == "image_url" {
				out = append(out, pp["image_url"])
			}
		}
	}
	return out
}

// TestNormalizeImagePartsForm 锁定字符串形态 image_url 被归一为对象形态。
//
// 背景：new-api 的 responses→chat 转换器对字符串 form 原样透传，上游只认对象形态，
// 收到字符串即 400 code=11101「invalid image_url content」(cannot unmarshal string)。
// responses 客户端带图必失败；此处走 PrepareBodyOptWithEfforts 全链路断言。
func TestNormalizeImagePartsForm(t *testing.T) {
	cases := []struct {
		name    string
		body    string
		wantURL string // 期望 image_url.url；空串表示「应保持字符串形态不包装」
		wantStr bool   // 期望仍为字符串形态
	}{
		{"字符串形态被包装为对象",
			`{"messages":[{"role":"user","content":[{"type":"text","text":"看图"},{"type":"image_url","image_url":"data:image/png;base64,AAA"}]}]}`,
			"data:image/png;base64,AAA", false},
		{"对象形态原样保留（不二次包装）",
			`{"messages":[{"role":"user","content":[{"type":"image_url","image_url":{"url":"https://x/y.png","detail":"high"}}]}]}`,
			"https://x/y.png", false},
		{"http URL 字符串同样包装",
			`{"messages":[{"role":"user","content":[{"type":"image_url","image_url":"https://x/y.png"}]}]}`,
			"https://x/y.png", false},
		{"空字符串不包装（由上游按其语义处理）",
			`{"messages":[{"role":"user","content":[{"type":"image_url","image_url":""}]}]}`,
			"", true},
		{"缺 image_url 字段不 panic",
			`{"messages":[{"role":"user","content":[{"type":"image_url"}]}]}`,
			"", true},
		{"video_url 不被包装（chat 规范本就字符串）",
			`{"messages":[{"role":"user","content":[{"type":"video_url","video_url":"https://v/1.mp4"}]}]}`,
			"", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			out := PrepareBodyOptWithEfforts([]byte(c.body), true, nil)
			got := imageParts(t, out)
			if c.wantURL == "" && c.wantStr {
				// 断言未被包装成对象
				for _, v := range got {
					if _, isObj := v.(map[string]any); isObj {
						t.Fatalf("不应被包装，实为对象: %v", v)
					}
				}
				return
			}
			if len(got) != 1 {
				t.Fatalf("期望 1 个 image_url part，实得 %d", len(got))
			}
			obj, isObj := got[0].(map[string]any)
			if !isObj {
				t.Fatalf("应为对象形态，实为 %T: %v", got[0], got[0])
			}
			if obj["url"] != c.wantURL {
				t.Fatalf("url 内容被改动: got %v want %v", obj["url"], c.wantURL)
			}
		})
	}
}

// TestNormalizeImagePartsPreservesDetail 对象形态的 detail 等其余键不得丢失。
func TestNormalizeImagePartsPreservesDetail(t *testing.T) {
	body := `{"messages":[{"role":"user","content":[{"type":"image_url","image_url":{"url":"https://x/y.png","detail":"low"}}]}]}`
	out := PrepareBodyOptWithEfforts([]byte(body), true, nil)
	got := imageParts(t, out)
	obj := got[0].(map[string]any)
	if obj["detail"] != "low" {
		t.Fatalf("detail 丢失: %v", obj)
	}
}

// TestNormalizeImagePartsStable 图片归一化必须保持 body 重写链的字节级稳定
// （稳定性是 prompt cache 前缀命中的前提，见 stability_test.go）。
// 同输入多次跑必须字节一致；且无字符串形态图片时不得因本步引入变化。
func TestNormalizeImagePartsStable(t *testing.T) {
	bodies := []string{
		`{"model":"glm-5.3","messages":[{"role":"user","content":[{"type":"text","text":"看图"},{"type":"image_url","image_url":"data:image/png;base64,AAA"},{"type":"image_url","image_url":{"url":"https://x/y.png"}}]}]}`,
		`{"model":"glm-5.3","messages":[{"role":"user","content":"纯文本"}]}`,
		`{"model":"cn:glm-4","messages":[{"role":"user","content":[{"type":"image_url","image_url":"https://a/b.png"}]},{"role":"assistant","content":"y"}]}`,
	}
	for i, b := range bodies {
		first := PrepareBodyOptWithEfforts([]byte(b), true, nil)
		for n := 0; n < 5; n++ {
			again := PrepareBodyOptWithEfforts([]byte(b), true, nil)
			if !bytes.Equal(first, again) {
				t.Fatalf("case %d 序列化不稳定（prompt cache 前缀将 miss）\nrun1=%s\nrun%d=%s", i, first, n+2, again)
			}
		}
	}
}
