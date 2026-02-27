import os
import sys
from pathlib import Path

# Add the parent directory to the Python path so we can import from src
parent_dir = Path(__file__).parent.parent
src_path = parent_dir / "src"
sys.path.insert(0, str(parent_dir))
sys.path.insert(0, str(src_path))

# Import app after modifying the path
if __name__ == "__main__":
    from app import app
    app.run(host='127.0.0.1', port=5000, debug=True)
