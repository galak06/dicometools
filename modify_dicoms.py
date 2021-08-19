import pydicom
import os
import datetime
import json
from Tools.helper_dicom import modify_series


def read_json_file(path: str):
    data = {}
    if os.path.basename(path):
        file_to_read = base_dir_related(path)
        with open(file_to_read) as json_file:
            data = json.load(json_file)
    else:
        raise FileNotFoundError
    return data


def base_dir_related(path: str):
    basedir = os.path.abspath(os.path.dirname(__file__))
    return os.path.join(basedir, path)


def main():
    SOURCE = base_dir_related('Images')
    DEST = '/Users/gilqcohen/Dropbox (Aidoc)/Knowledge transfer/ReleaseToRegulation/LoadTest/'
    print(f'source {SOURCE}')
    path = base_dir_related('const.json')
    const = read_json_file(path)
    _values_to_modify = const["DICOM_FIELDS"]["SET1"]
    for scan in const["SCANS_UIDS"]:
        for alg in const["SCANS_UIDS"][scan]:
            _values_to_modify['StudyDescription'] = "Contains %s" % scan.upper()
            _modify_dicom_list_in_range = modify_series(const["SCANS_UIDS"][scan][alg],
                                                        _values_to_modify,
                                                        SOURCE,
                                                        DEST,
                                                        True)


if __name__ == '__main__':
    main()
    print("Done")
