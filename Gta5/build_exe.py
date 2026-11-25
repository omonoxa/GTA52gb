import os
import shutil
import subprocess
from pathlib import Path

project_dir = Path(__file__).resolve().parent
out_dir = Path(r"C:\BuildGTA")

if out_dir.exists():
    shutil.rmtree(out_dir, ignore_errors=True)

cmd = [
    "npx.cmd" if os.name == "nt" else "npx",
    "electron-packager",
    ".",
    "AplicatieGTA",
    "--platform=win32",
    "--arch=x64",
    f"--out={out_dir}",
    "--overwrite",
]

print("Rulez:", " ".join(cmd))
result = subprocess.run(cmd, cwd=project_dir)

if result.returncode == 0:
    target = out_dir / "AplicatieGTA-win32-x64"
    print("Gata! Folderul este:", target)
    if os.name == "nt" and target.exists():
        os.startfile(target)
else:
    print("A apărut o eroare la build. Cod ieșire:", result.returncode)