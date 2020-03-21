import fnmatch
import os


def files_from_wildcard(input_wildcard):
    files_to_import = []

    # wildcards were not expanded by the shell, it happens when called from Pycharm
    last_slash = input_wildcard.rfind('/')

    if last_slash != -1:
        base = input_wildcard[:last_slash]
        wildcard = input_wildcard[last_slash + 1 :]
    else:
        base = '.'
        wildcard = input_wildcard

    for file in os.listdir(base):
        if fnmatch.fnmatch(file, wildcard):
            full_path = os.path.join(base, file)
            files_to_import.append(full_path)

    return files_to_import
