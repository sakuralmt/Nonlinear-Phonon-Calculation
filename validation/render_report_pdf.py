"""Render the scientific Markdown report with embedded figures and tables.

Requires reportlab; provide an installed Unicode TTF via --font.
"""

import argparse
from pathlib import Path
import re
from xml.sax.saxutils import escape
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.lib import colors
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.pagesizes import A4
from reportlab.platypus import (
    SimpleDocTemplate,
    Paragraph,
    Spacer,
    Table,
    TableStyle,
    Image,
)

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--input", type=Path, required=True)
parser.add_argument("--output", type=Path, required=True)
parser.add_argument(
    "--font",
    type=Path,
    default=Path("/System/Library/Fonts/Supplemental/Arial Unicode.ttf"),
)
args = parser.parse_args()
source = args.input.resolve()
output = args.output.resolve()
output.parent.mkdir(parents=True, exist_ok=True)
pdfmetrics.registerFont(TTFont("Unicode", str(args.font)))
styles = {
    "body": ParagraphStyle(
        "body",
        fontName="Unicode",
        fontSize=9.2,
        leading=14.2,
        spaceAfter=7,
        wordWrap="CJK",
    ),
    "title": ParagraphStyle(
        "title",
        fontName="Unicode",
        fontSize=18,
        leading=25,
        spaceAfter=15,
        textColor=colors.HexColor("#175870"),
    ),
    "h2": ParagraphStyle(
        "h2",
        fontName="Unicode",
        fontSize=12.5,
        leading=18,
        spaceBefore=11,
        spaceAfter=8,
        keepWithNext=True,
        textColor=colors.HexColor("#175870"),
    ),
    "cell": ParagraphStyle(
        "cell", fontName="Unicode", fontSize=8.0, leading=11.5, wordWrap="CJK"
    ),
}


def clean(text):
    text = re.sub(r"\[([^]]+)\]\(([^)]+)\)", r"\1", text)
    text = (
        text.replace("**", "")
        .replace("`", "")
        .replace("amu³ᐟ²", "amu^(3/2)")
        .replace("ᐟ", "/")
    )
    return escape(text)


story = []
lines = source.read_text().splitlines()
i = 0
while i < len(lines):
    line = lines[i]
    i += 1
    if not line.strip():
        continue
    if line.startswith("# "):
        story.append(Paragraph(clean(line[2:]), styles["title"]))
        continue
    if line.startswith("## "):
        story.append(Paragraph(clean(line[3:]), styles["h2"]))
        continue
    if line.startswith("!["):
        path = re.search(r"\]\(([^)]+)\)", line).group(1)
        im = Image(str(source.parent / path))
        ratio = im.imageHeight / im.imageWidth
        im.drawWidth = 475
        im.drawHeight = 475 * ratio
        story.extend([im, Spacer(1, 10)])
        continue
    if line.startswith("|"):
        rows = [line]
        while i < len(lines) and lines[i].startswith("|"):
            rows.append(lines[i])
            i += 1
        raw = [
            [s.strip() for s in row.strip("|").split("|")]
            for row in rows
            if not re.match(r"^\|[\s:|\-]+\|$", row)
        ]
        n = len(raw[0])
        first = 160 if n > 2 else 125
        widths = [first] + [(475 - first) / (n - 1)] * (n - 1)
        table = Table(
            [[Paragraph(clean(x), styles["cell"]) for x in row] for row in raw],
            colWidths=widths,
            repeatRows=1,
            hAlign="LEFT",
        )
        table.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#e6eff3")),
                    ("GRID", (0, 0), (-1, -1), 0.3, colors.HexColor("#c9d4d9")),
                    ("VALIGN", (0, 0), (-1, -1), "TOP"),
                    ("LEFTPADDING", (0, 0), (-1, -1), 5),
                    ("RIGHTPADDING", (0, 0), (-1, -1), 5),
                    ("TOPPADDING", (0, 0), (-1, -1), 5),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
                ]
            )
        )
        story.extend([table, Spacer(1, 10)])
        continue
    story.append(Paragraph(clean(line), styles["body"]))


def footer(canvas, doc):
    canvas.setFont("Unicode", 8)
    canvas.setFillColor(colors.HexColor("#607580"))
    canvas.drawString(48, 27, "NPC 1.0.1 | 2026-09-25 | CPU validation; no new DFT")
    canvas.drawRightString(A4[0] - 48, 27, str(doc.page))


doc = SimpleDocTemplate(
    str(output),
    pagesize=A4,
    leftMargin=48,
    rightMargin=48,
    topMargin=43,
    bottomMargin=43,
    title=lines[0].lstrip("# "),
    author="NPC validation",
)
doc.build(story, onFirstPage=footer, onLaterPages=footer)
print(output)
