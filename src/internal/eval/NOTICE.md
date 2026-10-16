# Evaluation sources and licenses

The ModelTrace scoring algorithm, challenge generator, reference bank and golden
fixtures are adapted from haowang02/cpa-plugin-codex-candy-eval, commit
`976239690451849223c2174ffdd08681019753d1` (MIT, Copyright 2026 Hao Wang).
The original ModelTrace source is xqy2006/ModelTrace, pinned upstream at
`df3a0f9d3e054c0dc02d6d586686db8daf8fa7c8` (MIT, Copyright 2026 xqy2006).

- https://github.com/haowang02/cpa-plugin-codex-candy-eval
- https://github.com/xqy2006/ModelTrace
- Plugin license: `LICENSE.candy-eval`
- ModelTrace license: `data/modeltrace/LICENSE` (also embedded in the plugin)

Changes: standalone Go package and exported adapters; LF normalization for
bundled data. Golden scores verify algorithm parity. Results compare only the
included candidates; probabilities do not prove model identity or capability.
Adding a candidate requires refitting the bank, feature transforms and calibration.

The candy question and standalone-21 scoring rule are adapted from the same
plugin. The prompt includes the no-external-tools instruction used by v0.3.6,
without an added FINAL_ANSWER output format. Shape-selective draws guarantee a
pair with 9 circles and 12 stars, so the answer is 21, not 29. Revision
`candy-shapes-v3-original-21` corrects the previous integration's mistaken
`candy-shapes-v2-final-29` baseline. Old records are preserved without regrading.
The original matcher can accept an incidental 21 in an explanation; this
compatibility benchmark is not a proof of capability. Empty answers are ungraded.
