import string


def remove_file_extension(file_name: string):
    """ Get a file name without its extension [file_full_name]
    :param context:
    :param dataset_id:
    :return file_name_without_extension:
    """
    pos = file_name.rfind(".")

    if pos != -1:
        return file_name[:pos]
    return file_name

