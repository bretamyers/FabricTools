import sys
import os

package_source_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../src'))
print(f'{package_source_path=}')
sys.path.insert(0, os.path.abspath(package_source_path))
# sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

import FabricWorkspaceFetch
