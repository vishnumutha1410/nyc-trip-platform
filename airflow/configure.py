"""Point Airflow at this repo's dags folder and turn the examples off.

Editing airflow.cfg by hand is fine once. Doing it from a script means the
setup is reproducible and reviewable, and it is the difference between a
project someone else can run and a project with a README full of "now open
this file and change line 47".
"""
from __future__ import annotations

import os
import pathlib
import re
import sys

AIRFLOW_HOME = pathlib.Path(os.getenv("AIRFLOW_HOME", pathlib.Path.home() / "airflow"))
CFG = AIRFLOW_HOME / "airflow.cfg"
DAGS = pathlib.Path(__file__).resolve().parent / "dags"


def main() -> int:
    if not CFG.exists():
        sys.exit(f"{CFG} not found - run `airflow db migrate` first.")

    text = CFG.read_text()
    text, n_dags = re.subn(r"^dags_folder = .*$", f"dags_folder = {DAGS}",
                           text, count=1, flags=re.M)
    text, n_ex = re.subn(r"^load_examples = .*$", "load_examples = False",
                         text, count=1, flags=re.M)
    CFG.write_text(text)

    print(f"dags_folder  -> {DAGS}   ({'set' if n_dags else 'NOT FOUND'})")
    print(f"load_examples-> False    ({'set' if n_ex else 'NOT FOUND'})")
    if not (n_dags and n_ex):
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
