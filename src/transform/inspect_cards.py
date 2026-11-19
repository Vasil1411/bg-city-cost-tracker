from pathlib import Path
from bs4 import BeautifulSoup

html_path = Path("data/bronze/fuels/ingestion_date=2026-11-19/snapshot.html")
soup = BeautifulSoup(html_path.read_text(encoding="utf-8"), "html.parser")

# Намери елементи, чийто директен текст съдържа "€/л"
matches = []
for el in soup.find_all(["div", "li", "article", "section", "p", "span"]):
    text = el.get_text(" | ", strip=True)
    if "€/л" in text and len(text) < 500:
        matches.append((el.name, el.get("class"), text))

print("Candidate elements with €/л:", len(matches))
for tag, cls, text in matches[:12]:
    print(f"[{tag} class={cls}] -> {text}")