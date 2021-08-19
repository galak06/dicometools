import logging
import os
import subprocess
from concurrent.futures.thread import ThreadPoolExecutor
from subprocess import CalledProcessError

logger = logging.getLogger(__name__)


def decompress_file(gdcm_path, src_file, target_file):
    environment = os.environ.copy()
    environment['LD_LIBRARY_PATH'] = os.path.join(gdcm_path, 'lib')
    cmd = [os.path.join(gdcm_path, 'bin', 'gdcmconv'), '--j2k', src_file, target_file]
    subprocess.run(
        cmd,
        env=environment,
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True
    )


class GDCMDicomDecompressor:
    def __init__(self, gdcm_path: str):
        self._gdcm_path = gdcm_path

    def decompress_dir(self, source_dir: str, dest_dir: str):
        # Decompress every slice since we might have a scan with only some compressed slices

        def decompress(source_full_path: str):
            file_relative_path = os.path.relpath(source_full_path, source_dir)
            dest_full_path = os.path.join(dest_dir, file_relative_path)
            os.makedirs(os.path.dirname(dest_full_path), exist_ok=True)
            decompress_file(self._gdcm_path, source_full_path, dest_full_path)
            # print(f'Source: {source_full_path}, Dest: {dest_full_path}')

        max_workers = self._get_number_of_max_workers()
        logger.debug(f"Creating a thread-pool with {max_workers} workers")
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            try:
                all_dicom_files = list(self._get_all_dicom_files(source_dir))
                logger.debug(f"Will decompress {len(all_dicom_files)} dicom files")
                for i, _ in enumerate(pool.map(decompress, all_dicom_files)):
                    if i % 1000 == 999:
                        print(f'Decompressed: {i + 1} files')
            except CalledProcessError as e:
                logger.exception(
                    'Error while decompressing a file.\nstdout: "{}"\nstderr: "{}"'.format(e.stdout, e.stderr))
                raise

    def _get_all_dicom_files(self, source_dir: str):
        for root, dirs, files in os.walk(source_dir):
            for file in files:
                if file.endswith('.dcm'):
                    yield os.path.join(root, file)

    @staticmethod
    def _get_number_of_max_workers() -> int:
        return (os.cpu_count() or 1) * 2


def main():
    decompressor = GDCMDicomDecompressor('/home/ubuntu/GDCM-2.8.4-Linux-x86_64')
    decompressor.decompress_dir('/home/ubuntu/dcm4che/storage2', '/home/ubuntu/compressed/storage2')
    decompressor.decompress_dir('/home/ubuntu/dcm4che/storage3', '/home/ubuntu/compressed/storage3')


if __name__ == '__main__':
    main()

