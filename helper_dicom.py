import os
import datetime
import random
import logging
import string
from concurrent.futures import ThreadPoolExecutor
from itertools import repeat
from tqdm import tqdm
import pydicom
from pydicom.uid import generate_uid
import os

logger = logging.getLogger(__name__)


def random_days_between_date(from_time=None, to_time=None, not_in_range: int = 0):
    current_time = datetime.datetime.now()
    past_time_window = datetime.timedelta(seconds=from_time)
    future_time_window = datetime.timedelta(seconds=to_time)
    future_date = current_time + future_time_window
    past_date = current_time - past_time_window
    random_number_of_days_in_seconds = random.randrange(0, future_time_window.total_seconds())

    if not_in_range is 0:
        random_date = current_time - datetime.timedelta(seconds=random_number_of_days_in_seconds)
    if not_in_range > 0:
        random_date = future_date + datetime.timedelta(days=not_in_range)
    if not_in_range < 0:
        random_date = past_date + datetime.timedelta(days=not_in_range)

    return random_date


def process_file(name, study_instance_uid, dicom_fields_to_modify: {}, path_source: str, path_target: str, minor_flow: bool = False):
    relative_path = os.path.relpath(name, path_source)
    filename = os.path.basename(relative_path)
    with pydicom.read_file(name) as file:
        file.StudyInstanceUID = study_instance_uid
        relative_path = os.path.relpath(name, path_source)
        filename = os.path.basename(relative_path)
        if minor_flow:
            dest_dir = os.path.join(path_target, f'study_{file.StudyInstanceUID}')
        else:
            relative_path_no_filename = os.path.dirname(relative_path)
            dest_dir = os.path.join(path_target, relative_path_no_filename)
        if not os.path.exists(dest_dir):
            create_folder(dest_dir)
        if "StudyDate" in dicom_fields_to_modify:
            file.StudyDate = dicom_fields_to_modify['StudyDate']
            file.StudyTime = dicom_fields_to_modify['StudyTime']
        if "AGE" in dicom_fields_to_modify:
            file.PatientAge = dicom_fields_to_modify['AGE']
        if "PatientBirthDate" in dicom_fields_to_modify:
            file.PatientBirthDate = dicom_fields_to_modify['PatientBirthDate']
        if "StudyDescription" in dicom_fields_to_modify:
            file.StudyDescription = dicom_fields_to_modify['StudyDescription']
            file.SeriesDescription = dicom_fields_to_modify['StudyDescription']
        if "PatientName" in dicom_fields_to_modify:
            file.PatientName = dicom_fields_to_modify['PatientName']
        if "InstitutionName" in dicom_fields_to_modify:
            file.InstitutionName = dicom_fields_to_modify['InstitutionName']
        if "AccessionNumber" in dicom_fields_to_modify:
            file.AccessionNumber = dicom_fields_to_modify['AccessionNumber']
        study_uid = file.StudyInstanceUID
        new_path = os.path.join(dest_dir, f'CT.{file.SOPInstanceUID}.dcm')
        file.save_as(new_path)
        logging.debug("new file path: %s", new_path)
        logging.debug(file)
        return study_uid


def process_file_after_pacs_agent(name, study_date: str, path_target: str):
    with pydicom.read_file(name) as _file:
        study_uid = _file.StudyInstanceUID
        _file.StudyDate = study_date
        dest_dir = os.path.join(path_target, f'study_{study_uid}')
        os.makedirs(dest_dir, exist_ok=True)
        new_path = os.path.join(dest_dir, f"CT_{_file.SOPInstanceUID}.dcm")
        _file.save_as(new_path)


def build_file_list(root, series) -> list:
    file_list = []
    if os.path.exists(root):
        for root, dirs, files in os.walk(root):
            if root.__contains__(series):
                for file in files:
                    if not file.endswith('.dcm'):
                        continue
                    old_path = os.path.join(root, file)
                    file_list.append(old_path)
    else:
        logging.debug(f'{__name__} source: {root} not found')
    return file_list


def modify_series(series_to_modify: list, dicom_fields, path_source: str, path_target: str, minor_flow: bool = False):
    logging.debug('%s source: %s , target: %s, minor_flow: %s', __name__, path_source, path_target, minor_flow)
    modify_dicom_fields = modify_fields(dicom_fields)
    file_and_folder_list = []
    results = []
    if os.path.exists(path_source):
        if series_to_modify is list:
            for item in series_to_modify:
                file_and_folder_list.append(build_file_list(path_source, item))
        else:
            file_and_folder_list.append(build_file_list(path_source, series_to_modify))

        for file_list in file_and_folder_list:
            study_instance_uid = generate_uid()
            with ThreadPoolExecutor(max_workers=10) as executor:
                _id = list(tqdm(executor.map(process_file, file_list, repeat(study_instance_uid), repeat(modify_dicom_fields),
                                             repeat(path_source), repeat(path_target), repeat(minor_flow)),
                                total=len(file_and_folder_list)))

            results.append(_id[0])
    else:
        logging.debug('%s source: %s not found', __name__, path_source)
    return results


def modify_fields(dicom_fields: {}):
    modify_list = {}
    if 'StudyDate' in dicom_fields:
        initial_time_window = dicom_fields['StudyDate']['PACS_POLL_INITIAL_TIME_WINDOW']
        time_window = dicom_fields['StudyDate']['PACS_POLL_TIME_WINDOW']
        not_in_range = 0
        if "NOT_IN_RANGE" in dicom_fields['StudyDate']:
            not_in_range = dicom_fields['StudyDate']['NOT_IN_RANGE']
        study_date_time = random_days_between_date(initial_time_window, time_window, not_in_range)
        modify_list['StudyDate'] = study_date_time.strftime('%Y%m%d')
        modify_list['StudyTime'] = study_date_time.strftime("%H%M%S.%M")
    if 'AGE' in dicom_fields:
        modify_list['AGE'] = dicom_fields['AGE']
    if 'PatientBirthDate' in dicom_fields:
        if dicom_fields['PatientBirthDate'] == "-1":
            values = dicom_fields['AGE'].split('Y')
            dateofbirth = datetime.date.today() - datetime.timedelta(days=int(values[0]) * 365)
            modify_list['PatientBirthDate'] = dateofbirth.strftime('%Y%m%d')
        else:
            modify_list['AGE'] = ""
            modify_list['PatientBirthDate'] = dicom_fields['PatientBirthDate']
    if 'StudyDescription' in dicom_fields:
        modify_list['StudyDescription'] = dicom_fields['StudyDescription']
    if 'PatientName' in dicom_fields:
        modify_list['PatientName'] = dicom_fields['PatientName']
    if 'InstitutionName' in dicom_fields:
        modify_list['InstitutionName'] = dicom_fields['InstitutionName']
    if 'AccessionNumber' in dicom_fields:
        value = ''.join(random.choices(string.digits, k=8))
        modify_list['AccessionNumber'] = value + dicom_fields['AccessionNumber']
    return modify_list


def create_folder(path: str):
    try:
        os.makedirs(path, exist_ok=True)
    except FileExistsError:
        logging.debug('error failed to create folder at: ' + path)
