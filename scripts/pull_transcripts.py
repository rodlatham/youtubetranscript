import csv
import json
import re
import subprocess
import sys
import unicodedata
from pathlib import Path

from tqdm import tqdm
from youtube_transcript_api import (
    YouTubeTranscriptApi,
    TranscriptsDisabled,
    NoTranscriptFound,
    VideoUnavailable,
)

def safe_filename(name: str, suffix: str, max_len: int = 120) -> str:
    """
    Build a filesystem safe filename from a video title.
    Produces something like: "<Title> [VIDEOID].ext"
    """
    n = unicodedata.normalize("NFKC", name or "")
    n = "".join(ch for ch in n if ch.isprintable())
    n = re.sub(r'[\\/:*?"<>|]+', " ", n)
    n = re.sub(r"\s+", " ", n).strip()
    base_max = max_len - len(suffix)
    if base_max < 1:
        base_max = 1
    if len(n) > base_max:
        n = n[:base_max].rstrip()
    return f"{n}{suffix}"

def run_yt_dlp_list(playlist_url: str):
    """
    Use yt-dlp to pull flat metadata for the playlist.
    Returns a list of dicts with video_id, title, url.
    """
    out = subprocess.check_output(
        ["yt-dlp", "--flat-playlist", "-J", playlist_url],
        text=True
    )
    data = json.loads(out)
    items = []
    for e in data.get("entries", []):
        vid = e.get("id")
        title = (e.get("title") or "").strip()
        url = e.get("webpage_url") or (f"https://www.youtube.com/watch?v={vid}" if vid else "")
        if vid and url:
            items.append({"video_id": vid, "title": title, "url": url})
    return items

def fetch_best_english_transcript(vid: str):
    """
    Returns (chunks, source_label).
    Prefers English, then auto English, else translates if allowed.
    """
    try:
        return YouTubeTranscriptApi.get_transcript(vid, languages=["en"]), "en"
    except Exception:
        pass

    listing = YouTubeTranscriptApi.list_transcripts(vid)

    # Native English
    try:
        t = listing.find_manually_created_transcript(["en"])
        return t.fetch(), "en"
    except Exception:
        try:
            t = listing.find_generated_transcript(["en"])
            return t.fetch(), "en-auto"
        except Exception:
            pass

    # Translate another language to English if possible
    for tr in listing:
        if tr.is_translatable:
            try:
                en_t = tr.translate("en")
                return en_t.fetch(), f"{tr.language_code}->en"
            except Exception:
                continue

    raise NoTranscriptFound(f"No English or translatable transcript for {vid}")

def join_text(chunks):
    return " ".join((x.get("text") or "").replace("\n", " ").strip() for x in chunks if x.get("text"))

def to_srt(chunks):
    def fmt(t):
        h = int(t // 3600)
        m = int((t % 3600) // 60)
        s = int(t % 60)
        ms = int(round((t - int(t)) * 1000))
        return f"{h:02}:{m:02}:{s:02},{ms:03}"

    lines = []
    for i, it in enumerate(chunks, 1):
        start = it["start"]
        end = start + it.get("duration", 0.0)
        text = (it.get("text") or "").replace("\n", " ").strip()
        lines += [str(i), f"{fmt(start)} --> {fmt(end)}", text, ""]
    return "\n".join(lines)

def main():
    if len(sys.argv) < 2:
        print("Usage: python scripts/pull_transcripts.py <PLAYLIST_URL>", file=sys.stderr)
        sys.exit(2)
    playlist_url = sys.argv[1]

    out_dir = Path("transcripts")
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = Path("transcripts.csv")
    jsonl_path = Path("transcripts.jsonl")

    # Reset JSONL on each run to avoid uncontrolled growth
    if jsonl_path.exists():
        jsonl_path.unlink()

    items = run_yt_dlp_list(playlist_url)
    if not items:
        print("No entries returned by yt-dlp", file=sys.stderr)
        sys.exit(1)

    rows = []

    for it in tqdm(items, desc="Processing videos"):
        vid = it["video_id"]
        url = it["url"]
        title = it["title"] or vid

        try:
            chunks, src = fetch_best_english_transcript(vid)
            text = join_text(chunks)

            # Title based filenames with ID to keep unique
            txt_name = safe_filename(title, f" [{vid}].txt")
            srt_name = safe_filename(title, f" [{vid}].srt")

            (out_dir / txt_name).write_text(text, encoding="utf-8")
            (out_dir / srt_name).write_text(to_srt(chunks), encoding="utf-8")

            rows.append({
                "video_id": vid,
                "url": url,
                "title": title,
                "source": src,
                "char_len": len(text),
            })

            with jsonl_path.open("a", encoding="utf-8") as jf:
                jf.write(json.dumps({
                    "video_id": vid,
                    "url": url,
                    "title": title,
                    "source": src,
                    "chunks": chunks
                }, ensure_ascii=False) + "\n")

        except (TranscriptsDisabled, NoTranscriptFound):
            rows.append({
                "video_id": vid,
                "url": url,
                "title": title,
                "source": "none",
                "char_len": 0,
            })
        except VideoUnavailable:
            rows.append({
                "video_id": vid,
                "url": url,
                "title": title,
                "source": "unavailable",
                "char_len": 0,
            })
        except Exception as e:
            rows.append({
                "video_id": vid,
                "url": url,
                "title": title,
                "source": f"error: {type(e).__name__}",
                "char_len": 0,
            })

    with csv_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["video_id", "url", "title", "source", "char_len"])
        w.writeheader()
        w.writerows(rows)

    print(f"Done. Wrote {len(rows)} rows.")
    print("Outputs:")
    print("- transcripts/<Title> [VIDEOID].txt")
    print("- transcripts/<Title> [VIDEOID].srt")
    print("- transcripts.csv")
    print("- transcripts.jsonl")

if __name__ == "__main__":
    main()
