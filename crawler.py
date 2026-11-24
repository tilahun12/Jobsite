"""Collects jobs from every site and writes one compact file for the web page.

Run locally:  pip install requests
              python crawler.py --out site/data/jobs.json
"""
import json, re, html, time, sqlite3, threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from urllib.parse import quote_plus, unquote, urlparse
import xml.etree.ElementTree as ET

import requests


# Sites we can read (WordPress-style job boards expose RSS or JSON-LD).
SITES = """
https://www.ethiojobs.net https://www.ethiopianreporterjobs.com https://www.hahujobs.com
https://www.geezjobs.com https://www.ezega.com/Jobs https://www.etcareers.com
https://www.harmeejobs.com https://www.effoysira.com https://www.elelanajobs.com
https://www.addisjobs.net https://www.jobwebethiopia.com https://www.ethioworks.com
https://www.justjobset.com https://www.365ethiojobs.com https://www.fideljobs.com
https://www.jobhabesha.com https://www.ethiojobs.info https://www.ethiopiawork.com
https://www.zionjobs.com https://www.mvacancy.com https://www.srafelagi.com
https://www.palmjobs.et https://www.afriworket.com https://www.kebenajobs.com
https://www.ngojobs.et https://www.devnetjobs.org https://reliefweb.int/jobs
https://unjobs.org https://careers.un.org https://jobs.undp.org https://jobs.unicef.org
https://www.unhcr.org/careers https://careers.au.int https://www.ethiopianairlines.com/careers
https://www.ethiotelecom.et https://www.workethiopia.net
""".split()

# Sites that block scraping: we give the user a direct search link instead.
LINK_ONLY = {
    "LinkedIn": "https://www.linkedin.com/jobs/search/?keywords={q}&location=Ethiopia",
    "Indeed": "https://www.indeed.com/jobs?q={q}&l=Ethiopia",
    "Glassdoor": "https://www.glassdoor.com/Job/jobs.htm?sc.keyword={q}",
    "Google Careers": "https://www.google.com/about/careers/applications/jobs/results/?q={q}",
    # Facebook has no public search API, so these open Facebook's own search (log in to see results).
    "Facebook posts": "https://www.facebook.com/search/posts/?q={fq}",
    "Facebook groups": "https://www.facebook.com/search/groups/?q={fq}",
    "Facebook pages": "https://www.facebook.com/search/pages/?q={fq}",
}

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; JobSearchBot/1.0)"}
TIMEOUT = 8
CACHE, CACHE_TTL = {}, 900  # remember each search for 15 minutes
MAX_AGE_DAYS = 90           # ignore jobs older than about 3 months


def cutoff():
    return datetime.now(timezone.utc) - timedelta(days=MAX_AGE_DAYS)


def too_old(batch):
    """True when every job in a (newest-first) batch is dated and older than the cutoff."""
    ds = [parse_date(j.get("date")) for j in batch]
    return bool(ds) and all(d and d < cutoff() for d in ds)

# ---------------------------------------------------------------------------
# Used when the search box is empty: keep only IT / computing related jobs.
# ---------------------------------------------------------------------------
FIELDS = """
Computer Science, Computer Science and Engineering, Information Technology, Information Systems,
Management Information Systems, Software Engineering, Computer Engineering, Information Science,
Information Systems Engineering, Information Technology Management, Data Science, Artificial Intelligence,
Machine Learning, Cybersecurity, Information Security, Network Engineering, Computer Networking,
Telecommunications Engineering, Software Development, Web Development, Mobile Application Development,
Game Development, Database Management, Database Administration, Data Engineering, Big Data Analytics,
Business Intelligence, Cloud Computing, Cloud Engineering, DevOps Engineering, Computer Systems Engineering,
Systems Administration, Systems Engineering, Embedded Systems Engineering, Robotics Engineering,
Mechatronics Engineering, Hardware Engineering, Electrical and Computer Engineering,
Electronics and Computer Engineering, Computer Architecture, Digital Systems Engineering,
Microprocessor Engineering, Microelectronics Engineering, Semiconductor Engineering, VLSI Design,
Computer Graphics, Multimedia Technology, Human-Computer Interaction, Human-Centered Computing,
Computational Science, Computational Mathematics, Computational Physics, Computational Biology,
Bioinformatics, Health Informatics, Medical Informatics, Biomedical Informatics,
Geographic Information Systems, Geoinformatics, Information Management, Knowledge Management,
Library and Information Science, Information Technology Infrastructure, Network Security,
Application Security, Digital Forensics, Ethical Hacking, Cryptography, Cyber Operations,
Computer Forensics, Artificial Intelligence Engineering, Natural Language Processing, Computer Vision,
Robotics and Artificial Intelligence, Autonomous Systems Engineering, Intelligent Systems,
Machine Intelligence, Computational Linguistics, Software Systems Engineering, Distributed Systems,
Parallel Computing, High-Performance Computing, Quantum Computing, Quantum Information Science,
Blockchain Technology, Financial Technology, Educational Technology, E-Commerce Technology,
Digital Transformation, Internet of Things, Internet Engineering, Wireless Communication Engineering,
Mobile Computing, Wireless Networking, Satellite Communications, Computer and Communication Engineering,
Communication Systems Engineering, Control Systems Engineering, Automation Engineering,
Industrial Informatics, Industrial Automation, Computer-Aided Design, Computer-Aided Engineering,
Simulation and Modeling, Computer Simulation, Game Technology, Virtual Reality, Augmented Reality,
Extended Reality, Digital Media Technology, Digital Image Processing, Signal Processing,
Multimedia Systems, Computer Science Education, Computing Education, Information Technology Education,
Software Quality Assurance, Software Testing, Software Architecture,
Programming Languages and Compilers, Theoretical Computer Science, Algorithms and Data Structures,
Computer Systems, Network and Distributed Systems, Information Retrieval, Data Mining,
Statistical Computing, Applied Computing, Applied Computer Science, Business Computing,
Enterprise Information Systems, Enterprise Resource Planning, E-Government Technology, Digital Business,
Privacy Engineering, Green Computing, Edge Computing, Fog Computing, Cloud Security,
Cybersecurity Engineering, Responsible Artificial Intelligence, Computational Neuroscience,
Neuroinformatics, Brain-Computer Interface Technology, Space Computing, Scientific Computing,
Computational Engineering, Computer Applications, Information Technology and Computing,
Computer and Information Science, Computer and Information Systems, Computing and Information Technology,
Computer Technology, Information and Communication Technology, Information and Communication Engineering,
Computer Science and Information Technology
"""

# Job-title words that real postings use (English and Amharic).
ROOTS = """software, developer, programmer, programming, computer, information technology,
information system, informatics, networking, network engineer, network administrator, cyber, database,
data scientist, data analyst, data engineer, big data, machine learning, artificial intelligence, cloud,
devops, full stack, fullstack, front-end, frontend, back-end, backend, mobile app, android developer,
ios developer, system administrator, systems administrator, sysadmin, it officer, it support,
it specialist, it manager, help desk, helpdesk, technical support, telecommunication, telecom engineer,
embedded, robotic, mechatronic, electronics, semiconductor, blockchain, fintech, e-commerce,
digital transformation, internet of things, geospatial, penetration test, quality assurance,
ኮምፒውተር, ኮምፒተር, ሶፍትዌር, ሃርድዌር, መረጃ ቴክኖሎጂ, ኢንፎርሜሽን, ቴክኖሎጂ, ኔትዎርክ, ኔትወርክ, ዳታ,
ዳታቤዝ, ፕሮግራመር, ሲስተም, ሳይበር, ድረ-ገጽ, ድረ ገጽ, ዌብ, አይሲቲ, አይቲ, ዲጂታል, ኤሌክትሮኒክስ, ቴሌኮም,
መተግበሪያ, ሮቦት, ኢንጂነር ሶፍትዌር"""

PHRASES = sorted({p.strip().lower() for p in (FIELDS + "," + ROOTS).split(",") if p.strip()})
# Short acronyms are matched as whole, upper-case words in the job title only.
ACRONYMS = re.compile(r"\b(IT|ICT|AI|ML|GIS|VLSI|ERP|IoT|IOT|VR|AR|XR|HCI|NLP|QA|UI|UX|SQL|DevOps)\b")

# Search terms sent to each site's own search when the box is empty.
CORE_TERMS = ["software", "developer", "IT", "ICT", "computer", "network", "data", "cyber",
              "ኮምፒውተር", "ሶፍትዌር", "ቴክኖሎጂ", "መረጃ ቴክኖሎጂ"]


def is_tech(job):
    title = job["title"] or ""
    text = (title + " " + (job["summary"] or "")).lower()
    return bool(ACRONYMS.search(title)) or any(p in text for p in PHRASES)


def clean(text):
    return re.sub(r"\s+", " ", html.unescape(re.sub(r"<[^>]+>", " ", text or ""))).strip()


def parse_date(value):
    if not value:
        return None
    try:
        d = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        try:
            d = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    return d if d.tzinfo else d.replace(tzinfo=timezone.utc)


def from_rss(url, source, q):
    r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
    r.raise_for_status()
    root = ET.fromstring(r.content)
    out = []
    for it in root.iter("item"):
        title = clean(it.findtext("title"))
        desc = clean(it.findtext("description"))
        if q and q.lower() not in (title + " " + desc).lower():
            continue
        d = parse_date(it.findtext("pubDate"))
        out.append({"title": title, "url": it.findtext("link"), "source": source,
                    "summary": desc[:220], "date": d.isoformat() if d else None})
    return out


def from_jsonld(url, source):
    r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
    r.raise_for_status()
    r.encoding = "utf-8"
    out = []
    for block in re.findall(r'<script[^>]+ld\+json[^>]*>(.*?)</script>', r.text, re.S):
        try:
            data = json.loads(block)
        except ValueError:
            continue
        for item in (data if isinstance(data, list) else data.get("@graph", [data])):
            if isinstance(item, dict) and item.get("@type") == "JobPosting":
                d = parse_date(item.get("datePosted"))
                out.append({"title": clean(item.get("title")), "url": item.get("url"),
                            "source": source, "summary": clean(item.get("description"))[:220],
                            "date": d.isoformat() if d else None})
    return out


def from_wp_api(base, source, term="", pages=5):
    """WordPress REST API: up to 100 posts per page, with real dates."""
    out = []
    for route in ("posts", "job-listings", "job_listing", "jobs"):
        got, ok = [], False
        for p in range(1, pages + 1):
            url = f"{base}/wp-json/wp/v2/{route}?per_page=100&page={p}&orderby=date&order=desc&_fields=date_gmt,link,title,excerpt"
            if term:
                url += "&search=" + quote_plus(term)
            r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
            if r.status_code == 400:  # asked for a page past the end
                ok = True
                break
            if r.status_code == 404:
                break
            r.raise_for_status()
            data = r.json()
            if not isinstance(data, list):
                break
            ok = True
            for it in data:
                d = parse_date(it.get("date_gmt"))
                got.append({"title": clean((it.get("title") or {}).get("rendered")), "url": it.get("link"),
                            "source": source, "summary": clean((it.get("excerpt") or {}).get("rendered"))[:220],
                            "date": d.isoformat() if d else None})
            if len(data) < 100 or too_old(got[-len(data):]):
                break
        got = [j for j in got if not j["date"] or parse_date(j["date"]) >= cutoff()]
        if ok and not got and route == "posts":
            raise ValueError("no posts")
        out += got
        if route == "posts" and not ok:
            raise ValueError("not WordPress")
    return out


def from_sitemap(base, source, depth=0, limit=8000):
    """Last resort: read the sitemap. Title comes from the URL slug, date from lastmod."""
    out = []
    for path in (("/sitemap.xml", "/wp-sitemap.xml") if depth == 0 else ("",)):
        url = base + path if depth == 0 else base
        try:
            r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
            r.raise_for_status()
            root = ET.fromstring(r.content)
        except Exception:
            continue
        locs = lambda el: [(c.tag.split("}")[-1], (c.text or "").strip()) for c in el]
        if root.tag.endswith("sitemapindex"):
            for sm in list(root)[:40]:
                loc = dict(locs(sm)).get("loc")
                if loc and depth < 1:
                    out += from_sitemap(loc, source, depth + 1, limit)
        else:
            for u in root:
                f = dict(locs(u))
                loc = f.get("loc", "")
                slug = unquote(loc.rstrip("/").split("/")[-1]).replace("-", " ").replace("_", " ")
                d = parse_date(f.get("lastmod"))
                if len(slug) > 8 and "." not in slug and not re.search(r"/(category|tag|author|page|feed|wp-|about|contact|privacy|terms)", loc):
                    out.append({"title": slug.title() if slug.isascii() else slug, "url": loc,
                                "source": source, "summary": "", "date": d.isoformat() if d else None})
        if out:
            break
    return out[:limit]


def search_site(base, q, it_only=True, deep=True):
    source = urlparse(base).netloc.replace("www.", "")
    base = base.rstrip("/")
    deadline = time.time() + 100  # never spend more than ~100s on one site
    live = lambda: time.time() < deadline
    terms = [q] if q else (CORE_TERMS if deep else [])
    P = 100 if deep else 2  # pages to read: full crawl vs quick refresh

    def safe(fn, *a, **k):
        try:
            return fn(*a, **k)
        except Exception:
            return None

    jobs = []
    # 1) WordPress REST API: the most complete source
    recent = None if q else safe(from_wp_api, base, source, "", P)
    if recent is not None or q:
        jobs += recent or []
        for t in terms:
            if not live():
                break
            r = safe(from_wp_api, base, source, t, 100 if q else 10)
            if r is None:
                break
            jobs += r
    # 2) RSS feeds, paged deeper than before
    if not jobs:
        if not q:
            for p in range(1, P + 1):
                r = safe(from_rss, f"{base}/feed/?paged={p}", source, "") if live() else None
                if not r:
                    break
                jobs += r
                if too_old(r):
                    break
        for t in terms:
            for p in range(1, 21):
                r = safe(from_rss, f"{base}/?s={quote_plus(t)}&feed=rss2&paged={p}", source, "") if live() else None
                if r is None:
                    break
                jobs += r
                if len(r) < 8 or too_old(r):
                    break
            if r is None:
                break
    # 3) structured data on the page, 4) sitemap
    if not jobs:
        jobs = safe(from_jsonld, f"{base}/?s={quote_plus(q)}" if q else base + "/", source) or []
    if (not jobs or deep) and live():
        extra = safe(from_sitemap, base, source) or []
        if q:
            words = re.findall(r"\w+", q.lower())
            extra = [j for j in extra if all(w in (j["title"] + " " + j["url"]).lower() for w in words)]
        jobs += extra
    jobs = [j for j in jobs if not j.get("date") or parse_date(j["date"]) >= cutoff()]
    if not q and it_only:
        jobs = [j for j in jobs if is_tech(j)]
    return jobs


# ---------------------------------------------------------------------------
# Build the data file
# ---------------------------------------------------------------------------
import argparse, os
from concurrent.futures import as_completed


def norm(url):
    return (url or "").split("#")[0].rstrip("/")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="site/data/jobs.json")
    ap.add_argument("--previous", help="previous jobs.json, so earlier jobs are kept until they age out")
    args = ap.parse_args()

    jobs = {}  # url -> job
    if args.previous and os.path.exists(args.previous):
        try:
            with open(args.previous, encoding="utf-8") as f:
                old = json.load(f)
            for t, u, si, dm, tech, summ in old["jobs"]:
                d = datetime.fromtimestamp(dm * 60, timezone.utc).isoformat() if dm else None
                jobs[u] = {"title": t, "url": u, "source": old["sources"][si], "date": d,
                           "tech": tech, "summary": summ}
            print(f"kept {len(jobs)} jobs from the previous run")
        except Exception as e:
            print("could not read previous data:", e)

    new = 0
    with ThreadPoolExecutor(max_workers=38) as pool:
        futs = {pool.submit(search_site, s, "", False, True): s for s in SITES}
        for f in as_completed(futs):
            name = urlparse(futs[f]).netloc.replace("www.", "")
            try:
                batch = f.result() or []
            except Exception:
                batch = []
            print(f"{name:35} {len(batch):6}")
            for j in batch:
                u = norm(j.get("url"))
                if u and j.get("title"):
                    new += u not in jobs
                    jobs[u] = {"title": j["title"], "url": u, "source": j["source"], "date": j.get("date"),
                               "tech": int(is_tech(j)), "summary": j.get("summary", "")}

    cut = cutoff()
    rows = []
    for j in jobs.values():
        d = parse_date(j["date"])
        if d and d < cut:
            continue  # older than 3 months
        rows.append((int(d.timestamp() // 60) if d else 0, j))
    rows.sort(key=lambda r: (r[0] == 0, -r[0]))  # newest first, undated last

    sources = sorted({j["source"] for _, j in rows})
    idx = {s: i for i, s in enumerate(sources)}
    empty = sorted(urlparse(s).netloc.replace("www.", "") for s in SITES
                   if urlparse(s).netloc.replace("www.", "") not in idx)
    data = {"updated": int(time.time()), "sources": sources, "empty": empty, "sites_checked": len(SITES),
            "jobs": [[j["title"][:200], j["url"], idx[j["source"]], dm, j["tech"], (j["summary"] or "")[:160]]
                     for dm, j in rows]}
    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, separators=(",", ":"))
    print(f"wrote {len(rows)} jobs ({new} new) from {len(sources)} sites -> {args.out}")


if __name__ == "__main__":
    main()
