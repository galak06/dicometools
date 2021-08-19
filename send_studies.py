import os
import random
import subprocess
import time
from concurrent import futures
from concurrent.futures.thread import ThreadPoolExecutor

BATCH_SIZE = 10
STUDIES_PER_HOUR = 1000.0
SECONDS_IN_HOUR = 3600.0
STUDIES_PER_SECOND = STUDIES_PER_HOUR / SECONDS_IN_HOUR
TOLERANCE_SEC = 30
# STUDIES_PER_SECOND = 10

INPUT_DIRS = ['./storage1', './storage2', './storage3']
STORESCP_TARGETS = ['aidocbox@172.31.29.53:4243']  # , 'aidocbox@172.31.29.53:8200', 'aidocbox@172.31.29.53:1642']
DCM4CHE_PATH = os.path.expanduser('~/dcm4che/dcm4che-5.23.3/bin')

pool_executor = ThreadPoolExecutor(max_workers=BATCH_SIZE)


def send_study_to_ahs(study_path: str, storescp_target: str):
    # subprocess.run(["/bin/sh", "-c", f"sleep 0.9; echo {study_path}"], check=True)
    subprocess.run([os.path.join(DCM4CHE_PATH, 'storescu'), "-c", storescp_target, study_path], check=True,
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    print(f'Sent: {study_path}')


def get_storescp_target():
    while True:
        for target in STORESCP_TARGETS:
            yield target


def get_studies_to_send():
    for dir in INPUT_DIRS:
        for file in os.listdir(dir):
            study_path = os.path.abspath(os.path.join(dir, file))
            if os.path.isdir(study_path):
                yield study_path


def main():
    start_time = time.time()
    end_time = time.time()

    studies_to_send = list(get_studies_to_send())
    random.shuffle(studies_to_send)
    sent_counter = 0
    sending = []
    for study_path, storescp_target in zip(studies_to_send, get_storescp_target()):
        future = pool_executor.submit(send_study_to_ahs, study_path, storescp_target)
        sending.append(future)
        if len(sending) >= BATCH_SIZE:
            futures.wait(sending, return_when=futures.FIRST_COMPLETED)

        new_sending = []
        for f in sending:
            if f.done():
                sent_counter += 1
            else:
                new_sending.append(f)
        sending = new_sending

        end_time = time.time()
        passed_time_since_start = end_time - start_time
        expected_time_to_pass_since_start = sent_counter / STUDIES_PER_SECOND
        sleep_duration = expected_time_to_pass_since_start - passed_time_since_start - TOLERANCE_SEC
        if sleep_duration > 0:
            print(f'Sleeping for: {sleep_duration} seconds')
            time.sleep(sleep_duration)

    print(f'Took: {end_time - start_time} seconds')


if __name__ == '__main__':
    main()

