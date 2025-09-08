import sys
import os

# Add the parent directory to Python path so we can import the main setup
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import and run the main setup
from setup import *
