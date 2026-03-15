"""
tag_oblivion.py
Tags the Elder Scrolls IV: Oblivion OST WAV files with correct ID3 metadata.
Modifies files in place. Run from anywhere — just update ALBUM_DIR if needed.
"""

from pathlib import Path
from mutagen.wave import WAVE
from mutagen.id3 import (
    ID3NoHeaderError,
    TIT2,  # Title
    TPE1,  # Artist
    TALB,  # Album
    TDRC,  # Year
    TRCK,  # Track number
    TCON,  # Genre
)

# ── Config ────────────────────────────────────────────────────────────────────

ALBUM_DIR = Path(
    "/Users/rob/Library/CloudStorage/OneDrive-Personal/Music/iTunes/iTunes Media/Music/Jeremy Soule/The Elder Scrolls IV Oblivion Original G"
)

ARTIST = "Jeremy Soule"
ALBUM  = "The Elder Scrolls IV: Oblivion (Original Game Soundtrack)"
YEAR   = "2006"
GENRE  = "Soundtrack"

# Track number → clean title (no leading number in the title tag)
TRACKS: dict[int, str] = {
     1: "Reign of the Septims",
     2: "Through the Valleys",
     3: "Death Knell",
     4: "Harvest Dawn",
     5: "Wind from the Depths",
     6: "King and Country",
     7: "Fall of the Hammer",
     8: "Wings of Kynareth",
     9: "All's Well",
    10: "Tension",
    11: "March of the Marauders",
    12: "Watchmen's Ease",
    13: "Glory of Cyrodiil",
    14: "Defending the Gate",
    15: "Bloody Blades",
    16: "Minstrel's Lament",
    17: "Ancient Sorrow",
    18: "Auriel's Ascension",
    19: "Daedra in Flight",
    20: "Unmarked Stone",
    21: "Bloodlust",
    22: "Sunrise of Flutes",
    23: "Churl's Revenge",
    24: "Deep Waters",
    25: "Dusk at the Market",
    26: "Peace at Akatosh",
}

# ── Helpers ───────────────────────────────────────────────────────────────────

def find_track_number(filename: str) -> int | None:
    """Extract leading track number from filename, e.g. '09 All's Well-GAIN.wav' → 9"""
    stem = Path(filename).stem  # strip .wav
    parts = stem.split(" ", 1)
    try:
        return int(parts[0])
    except (ValueError, IndexError):
        return None


def tag_file(wav_path: Path, track_num: int, title: str) -> None:
    audio = WAVE(wav_path)

    # WAVE uses ID3 tags — create header if missing
    try:
        audio.tags.delall("TIT2")  # ensure clean write
    except (AttributeError, ID3NoHeaderError):
        audio.add_tags()

    audio.tags.add(TIT2(encoding=3, text=title))
    audio.tags.add(TPE1(encoding=3, text=ARTIST))
    audio.tags.add(TALB(encoding=3, text=ALBUM))
    audio.tags.add(TDRC(encoding=3, text=YEAR))
    audio.tags.add(TRCK(encoding=3, text=str(track_num)))
    audio.tags.add(TCON(encoding=3, text=GENRE))

    audio.save()
    print(f"  ✓ {track_num:02d} — {title}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    if not ALBUM_DIR.exists():
        print(f"ERROR: Directory not found:\n  {ALBUM_DIR}")
        return

    wav_files = sorted(ALBUM_DIR.glob("*.wav"))
    if not wav_files:
        print(f"No .wav files found in:\n  {ALBUM_DIR}")
        return

    print(f"Found {len(wav_files)} WAV files in:\n  {ALBUM_DIR}\n")
    print(f"Tagging as: {ARTIST} — {ALBUM} ({YEAR})\n")

    skipped = []

    for wav_path in wav_files:
        track_num = find_track_number(wav_path.name)
        if track_num is None or track_num not in TRACKS:
            print(f"  ⚠ Skipping (unrecognised filename): {wav_path.name}")
            skipped.append(wav_path.name)
            continue
        tag_file(wav_path, track_num, TRACKS[track_num])

    print(f"\nDone. {len(wav_files) - len(skipped)} tagged, {len(skipped)} skipped.")
    if skipped:
        print("Skipped files:")
        for f in skipped:
            print(f"  - {f}")


if __name__ == "__main__":
    main()