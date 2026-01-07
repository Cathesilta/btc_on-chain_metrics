import os
from pathlib import Path
import shutil


BASE_DIR = Path("/data/index/btc/csv/")
SRC_ROOT = Path("./From_800000/from_920000")
DST_ROOT = Path("./From_600000/from_600000")



FILENAME = "tx_inputs.pg.csv"

if __name__ == "__main__":
    src_path = Path(os.path.join(BASE_DIR, SRC_ROOT))
    dst_path = Path(os.path.join(BASE_DIR, DST_ROOT))
    for src in src_path.glob("61*/" + FILENAME):
        rel_dir = src.parent.relative_to(src_path)
        dst_dir = dst_path / rel_dir
        print("from ",src)
        print("to ",dst_dir)
        dst = dst_dir / FILENAME
        # exit(0)
        if not dst.exists():
            shutil.copy2(src, dst)  