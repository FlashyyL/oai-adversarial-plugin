package eval

import (
	"regexp"
	"strings"
)

// The question is adapted from haowang02/cpa-plugin-codex-candy-eval (MIT).
// Shape-selective draws guarantee a pair with 9 circles and 12 stars.
const CandyRevision = "candy-shapes-v3-original-21"
const CandyExpected = 21
const CandyPrompt = `不使用任何外部工具回答以下问题：

在一个黑色的袋子里放有三种口味的糖果，每种糖果有两种不同的形状（圆形和五角星形，不同的形状靠手感可以分辨）。现已知不同口味的糖和不同形状的数量统计如下表。参赛者需要在活动前决定摸出的糖果数目，那么，最少取出多少个糖果才能保证手中同时拥有不同形状的苹果味和桃子味的糖？（同时手中有圆形苹果味匹配五角星桃子味糖果，或者有圆形桃子味匹配五角星苹果味糖果都满足要求）

        苹果味  桃子味  西瓜味
圆形       7      9      8
五角星形   7      6      4
`

var candyAnswer = regexp.MustCompile(`(^|[^0-9])21([^0-9]|$)`)

type CandyGrade struct {
	Status string `json:"status"`
	Answer *int   `json:"answer,omitempty"`
}

// Match the upstream benchmark's standalone-21 rule, not an added output
// format. Empty responses remain ungraded; request failures never reach here.
func GradeCandy(text string) CandyGrade {
	if strings.TrimSpace(text) == "" {
		return CandyGrade{Status: "uncertain"}
	}
	if candyAnswer.MatchString(text) {
		answer := CandyExpected
		return CandyGrade{Status: "correct", Answer: &answer}
	}
	return CandyGrade{Status: "incorrect"}
}
