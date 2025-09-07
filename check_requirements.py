import sys
import os
from packaging.requirements import Requirement
from packaging.version import parse as parse_version

# Try to import version from importlib.metadata (Python 3.8+)
try:
    from importlib.metadata import version, PackageNotFoundError
except ImportError:
    # Fallback for older Python versions (less likely needed in Termux now)
    from pkg_resources import get_distribution, DistributionNotFound
    class PackageNotFoundError(Exception): # Define it for consistency
        pass
    def version(pkg_name):
        try:
            return get_distribution(pkg_name).version
        except DistributionNotFound:
            raise PackageNotFoundError(f"Package '{pkg_name}' not found.")


def check_installed_version(package_name):
    """Checks the version of an installed package."""
    try:
        return version(package_name)
    except PackageNotFoundError:
        return None
    except Exception as e:
        return f"Error: {e}"

def check_requirements_status(req_file_path):
    """
    Checks the status of requirements in a given requirements.txt file
    against the currently active Python environment.
    """
    print(f"\n--- Checking requirements from: {req_file_path} ---")
    print(f"--- In Python environment: {sys.executable} ---")
    print(f"--- Python version: {sys.version.split(' ')[0]} ---")

    if not os.path.exists(req_file_path):
        print(f"Error: requirements file not found at {req_file_path}")
        return

    with open(req_file_path, 'r') as f:
        requirements_lines = f.readlines()

    all_ok = True
    for line_num, line in enumerate(requirements_lines, 1):
        line = line.strip()
        if not line or line.startswith('#'):
            continue # Skip empty lines and comments

        try:
            req = Requirement(line)
            package_name = req.name

            installed_version_str = check_installed_version(package_name)

            if installed_version_str is None:
                print(f"Line {line_num}: {package_name} - NOT INSTALLED")
                all_ok = False
            elif installed_version_str.startswith("Error:"):
                print(f"Line {line_num}: {package_name} - {installed_version_str}")
                all_ok = False
            else:
                installed_version = parse_version(installed_version_str)
                
                if req.specifier: # If there's a version specifier (e.g., Flask==2.3.0)
                    if installed_version in req.specifier:
                        print(f"Line {line_num}: {package_name} - OK ({installed_version_str} satisfies {req.specifier})")
                    else:
                        print(f"Line {line_num}: {package_name} - MISMATCH! Installed {installed_version_str} does not satisfy {req.specifier}")
                        all_ok = False
                else: # No specific version, just check if installed
                    print(f"Line {line_num}: {package_name} - INSTALLED ({installed_version_str})")

        except Exception as e:
            print(f"Line {line_num}: Error parsing requirement '{line}': {e}")
            all_ok = False

    if all_ok:
        print("\nAll specified requirements are met in this environment.")
    else:
        print("\nSome requirements are not met or not installed in this environment.")

if __name__ == "__main__":
    # Path to your requirements.txt file relative to where the script is run
    # Assuming this script is placed in ~/prediction/lottery/
    req_file = 'requirements.txt'

    if not os.path.exists(req_file):
        print(f"Error: '{req_file}' not found in the current directory.")
        print("Please ensure this script is in the same directory as your requirements.txt.")
        sys.exit(1)

    check_requirements_status(req_file)

