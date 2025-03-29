import os
import sys

cwd = os.getcwd()
sys.path.insert(0, os.path.join(cwd, "src"))

print(f'{os.getcwd()=}')
print(f'{sys.path=}')