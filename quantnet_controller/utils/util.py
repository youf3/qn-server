"""
Utility functions
"""

import os
import importlib.util
import logging

logger = logging.getLogger(__name__)


def import_classes_from_package(directory_path: str = None, package_name: str = "package"):
    """
    import classes from package in the directory path

    Parameters:
    -----------
    directory_path: str
        the path to directory of package
    package_name: str
        the name of package

    Return:
    -------
    classes of package

    """

    # Get the list of Python files (.py) in the specified directory
    python_files = [f[:-3] for f in os.listdir(directory_path) if f.endswith('.py')]
    allclasses = {}

    # Iterate over each Python file and import its contents dynamically
    for file_name in python_files:
        try:
            # Construct the full path to the Python file
            file_path = os.path.join(directory_path, f"{file_name}.py")

            # Create a module spec for the file
            spec = importlib.util.spec_from_file_location(file_name, file_path)

            # Create the module object
            module = importlib.util.module_from_spec(spec)

            # Load the module
            spec.loader.exec_module(module)

            # Iterate over the attributes of the module
            for name, obj in vars(module).items():
                # Check if the attribute is a class and belongs to the package
                if isinstance(obj, type) and obj.__module__ == file_name:
                    # Add the class to the globals() namespace
                    globals()[name] = obj
                    allclasses[name] = obj
                    logger.debug(f"Imported class: {name}")
        except Exception as e:
            print(f"Failed to import classes from {file_name}: {e}")

    return allclasses


def import_module_from_package(directory_path: str = None, package_name: str = "package", module_name: str = None):
    """
    import the module from package in the directory path

    Parameters:
    -----------
    directory_path: str
        the path to directory of package
    package_name: str
        the name of package
    module_name: str
        the name of module

    Return:
    -------
    module

    """

    file_path = os.path.join(directory_path, f"{module_name}.py")
    if not os.path.isfile(file_path):
        raise Exception(f"{directory_path} does not contain {module_name}")

    spec = importlib.util.spec_from_file_location(package_name + '.' + module_name, file_path)

    module = importlib.util.module_from_spec(spec)

    spec.loader.exec_module(module)
    return module
