#!/bin/bash

# Start JupyterLab
jupyter lab --ip=0.0.0.0 --port=8888 --no-browser --allow-root &

# Start Streamlit
streamlit run /workspace/app/dashboard.py --server.port 8501 --server.address 0.0.0.0

# Keep the container alive
tail -f /dev/null

