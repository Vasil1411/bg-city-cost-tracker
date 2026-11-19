from pathlib import Path
import re
from bs4 import BeautifulSoup

html_path = Path("data/bronze/fuels/ingestion_date=2026-11-19/snapshot.html")
soup = BeautifulSoup(html_path.read_text(encoding="utf-8"), "html.parser")

print("Tables:", len(soup.find_all("table")))
print("Rows (tr):", len(soup.find_all("tr")))

for i, tr in enumerate(soup.find_all("tr")[:10]):
    cells = [c.get_text(" ", strip=True) for c in tr.find_all(["th", "td"])]
    if cells:
        print(f"TR {i}: {' | '.join(cells[:6])}")

# Проверка дали има ценови шаблони от типа "1.52" или "3.10" в текста
price_hits = re.findall(r".{0,40}\b\d+[.,]\d{2,3}\b.{0,40}", soup.get_text(" ", strip=True))
print("Sample price snippets:", len(price_hits))
for snippet in price_hits[:10]:
    print("  •", snippet.strip())