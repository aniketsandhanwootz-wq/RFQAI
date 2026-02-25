# vision_caption_6line.md
You are analyzing a manufacturing-related image (drawing/photo/screenshot).
Produce EXACTLY 6 lines, each starting with the label below.
If something is not clearly visible, write "UNKNOWN". Do not guess.

Rules:
- No extra lines. Exactly 6 lines.
- Keep it concise but information-dense.
- Preserve symbols, callouts, part numbers, units, tolerances exactly as shown.
- If the image is a technical drawing, prioritize: dimensions, tolerances, GD&T, notes, material, finish, standards.
- If the image is a product photo, prioritize: visible features, defects, markings, labels, scale/measurements if visible.

Output format (exact labels):
1) WHAT: <what the image is>
2) PART: <part name/number/identifier if visible>
3) DIMS_TOL: <key dimensions/tolerances/GD&T if visible>
4) MATERIAL_FINISH: <material/finish/coating/heat treatment if visible>
5) NOTES_STD: <notes/standards/process instructions/warnings if visible>
6) RISKS_QUESTIONS: <ambiguities/missing info/questions to ask>

Example (drawing):
WHAT: Mechanical drawing (2D) with callouts and notes
PART: 10998713A-002-FA01_REVA (visible)
DIMS_TOL: Ø12 H7; 0.05 flatness; 120±0.1 (units mm)
MATERIAL_FINISH: SS304; Ra 1.6 (visible)
NOTES_STD: Deburr all edges; ISO 2768-mK (visible)
RISKS_QUESTIONS: Thread spec unclear; surface treatment not mentioned

Example (photo):
WHAT: Product photo of machined bracket
PART: UNKNOWN
DIMS_TOL: UNKNOWN
MATERIAL_FINISH: Appears anodized (if explicitly labeled); otherwise UNKNOWN
NOTES_STD: Label "QC PASS" visible (if present); else UNKNOWN
RISKS_QUESTIONS: No scale present; cannot confirm dimensions