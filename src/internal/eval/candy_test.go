package eval

import (
	"strings"
	"testing"
)

func TestCandyGuarantee(t *testing.T) {
	var failing [25][18]bool
	worstBlindDraw := 0
	for ar := 0; ar <= 7; ar++ {
		for pr := 0; pr <= 9; pr++ {
			for as := 0; as <= 7; as++ {
				for ps := 0; ps <= 6; ps++ {
					if (ar == 0 || ps == 0) && (pr == 0 || as == 0) {
						for wr := 0; wr <= 8; wr++ {
							for ws := 0; ws <= 4; ws++ {
								failing[ar+pr+wr][as+ps+ws] = true
								worstBlindDraw = max(worstBlindDraw, ar+pr+wr+as+ps+ws)
							}
						}
					}
				}
			}
		}
	}
	minimum := 42
	for circles := range failing {
		for stars, fails := range failing[circles] {
			if !fails {
				minimum = min(minimum, circles+stars)
			}
		}
	}
	if minimum != CandyExpected || failing[9][12] || !failing[9][11] {
		t.Fatalf("shape-selective minimum = %d, grader expects %d", minimum, CandyExpected)
	}
	if CandyExpected != 21 {
		t.Fatal("original benchmark answer changed")
	}
	if worstBlindDraw+1 != 29 {
		t.Fatal("blind draws must not be confused with shape-selective draws")
	}
}

func TestCandyOriginalScoring(t *testing.T) {
	for text, status := range map[string]string{
		"21": "correct", "FINAL_ANSWER=21": "correct",
		"最少取出 21 个糖果，9 颗圆形和 12 颗星形。": "correct",
		"**21颗**": "correct", "21.0": "correct",
		"29": "incorrect", "FINAL_ANSWER=29": "incorrect",
		"121": "incorrect", "210": "incorrect", "2 1": "incorrect",
		// Preserve upstream matching, including its known incidental-number limit.
		"21 不够，正确答案是 29": "correct", "": "uncertain", " \n": "uncertain",
	} {
		if got := GradeCandy(text); got.Status != status {
			t.Errorf("grade %q = %s, want %s", text, got.Status, status)
		}
	}
}

func TestCandyPromptDoesNotChangeOriginalOutputProtocol(t *testing.T) {
	if !strings.HasPrefix(CandyPrompt, "不使用任何外部工具回答以下问题：\n\n") ||
		!strings.Contains(CandyPrompt, "不同的形状靠手感可以分辨") || strings.Contains(CandyPrompt, "FINAL_ANSWER") {
		t.Fatal("original prompt conditions/output protocol changed")
	}
	if CandyRevision == "candy-shapes-v2-final-29" {
		t.Fatal("old incorrect scoring revision reused")
	}
}
