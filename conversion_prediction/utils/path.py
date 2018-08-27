import os


def relative_to_file(file_path, to_path):
    return os.path.join(os.path.dirname(os.path.abspath(file_path)), to_path)