# Download zipped files from S3 bucket to local directory
# ---------------------------------------------------------
# TODO:
# - take input as a url (s3 bucket url) and n, number of 
#   files to download
# - create a local directory if it doesn't exist
# - select top n files from S3 bucket, all if n is none
# - check if any of the files exist in the local directory
# - get cumulative size of selected files
# - check if cumulative size is less than available space in 
#   local directory
# - download files to local directory
# - show progress of downloads in terminal
# - create log file with download details and size of files 
#   downloaded and status of download (success/failure)
# ---------------------------------------------------------


import boto3
import os
import sys
import logging
import datetime


class S3Downloader:
    def __init__(self, bucket_name, n):
        self.bucket_name = bucket_name
        self.num_files = n
        self.current_date = datetime.now().strftime('%Y-%m-%d')
        self.logger = self.setup_logger(__name__, f'logs/{self.current_date}/s3_downloader.log')
        self.s3 = boto3.resource('s3')
        self.local_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 's3_downloads')
        
        # create local directory if it doesn't exist
        if not os.path.exists(self.local_dir):
            os.makedirs(self.local_dir)


    # helper method to create a log file
    def setup_logger(self, name, log_file, level=logging.INFO):
        """
        - define a formatter for the log file
        - define path to log file
        - create a log directory if it does not exist
        - create a handler for the log file
        - create a logger
        - add the handler to the logger
        - return the logger
        """
        # logging formatters
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        # log path
        log_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), f'core/logs/{self.current_date}')

        # create log directory if not exists
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)

        # create handlers
        handler = logging.FileHandler(log_file)        
        handler.setFormatter(formatter)

        # create logger
        logger = logging.getLogger(name)
        logger.setLevel(level)
        logger.addHandler(handler)

        # return the logger
        return logger


    # helper method to validate bucket url
    def validate_bucket_url(self, bucket_url):
        """
        - check URL provided
        - check if bucket exists
        """
        # check if bucket url is provided
        if not bucket_url:
            err = 'Bucket url is not provided'
            self.logger.error(err)
            raise Exception(err)

        # check if bucket exists
        try:
            self.s3.meta.client.head_bucket(Bucket=bucket_url)
            return True
        except:
            err = f'Bucket {bucket_url} does not exist'
            self.logger.error(err)
            raise Exception(err)


    # helper method to get list of files in bucket
    def get_bucket_files(self, bucket_url, n=None):
        """
        - use the bucket url to get the top n items in the bucket
        - if n is not provided, get all the files in the bucket
        - return the list of files and their sizes
        """
        # get the bucket
        bucket = self.s3.Bucket(bucket_url)

        # get the list of files in the bucket
        bucket_files = []
        if not n:
            for obj in bucket.objects.all():
                bucket_files.append({'key': obj.key, 'size': obj.size})
        else:
            # return top n files in the bucket
            for obj in bucket.objects.all()[:n]:
                bucket_files.append({'key': obj.key, 'size': obj.size})

        return bucket_files


    # helper method to check if file exists in local directory
    def check_file_exists(self, file_name):
        """
        - check if file exists in local directory
        - return true if file exists
        """
        # check if file exists in local directory
        if os.path.exists(os.path.join(self.local_dir, file_name)):
            return True
        else:
            return False


    # helper method to get cumulative size of files
    def get_cumulative_size(self, bucket_files):
        """
        - get cumulative size of files
        - return the cumulative size
        """
        # get cumulative size of files
        cumulative_size = 0
        for file in bucket_files:
            cumulative_size += file['size']

        return cumulative_size


    # helper method to check if available space is sufficient
    def check_available_space(self, cumulative_size):
        """
        - check if available space is sufficient
        - return true if available space is sufficient
        """
        # check if available space is sufficient
        if cumulative_size < self.get_available_space():
            return True
        else:
            exception = f'Not enough space available in local directory. Available space: {self.get_available_space()}, Required space: {cumulative_size}'
            self.logger.error(exception)
            raise Exception(exception)


    # helper method to get available space in local directory
    def get_available_space(self):
        """
        - get available space in local directory
        - return available space
        """
        # get available space in local directory
        return os.statvfs(self.local_dir).f_bavail * os.statvfs(self.local_dir).f_frsize


    def progress_bar(self, count, total, status=''):
        """
        - create a progress bar
        - print the progress bar
        """
        bar_len = 60
        filled_len = int(round(bar_len * count / float(total)))

        percents = round(100.0 * count / float(total), 1)
        bar = '=' * filled_len + '-' * (bar_len - filled_len)

        sys.stdout.write('[%s] %s%s ...%s\r' % (bar, percents, '%', status))
        sys.stdout.flush()

    
    # helper method to show progress bar for downloads
    def show_progress_bar(self, bucket_files, files_downloaded):
        """
        - show progress bar for downloads
        - return the list of files downloaded
        """
        # show progress bar for downloads
        for file in bucket_files:
            # check if file is downloaded
            if file['key'] in files_downloaded:
                # show progress bar
                self.progress_bar(files_downloaded.index(file['key']), len(files_downloaded), status=f'Downloading {file["key"]}')


    # helper method to download files to local directory
    def download_files(self, bucket_files):
        """
        - download files to local directory
        - return the list of files downloaded
        """
        # download files to local directory
        files_downloaded = []
        for file in bucket_files:
            # check if file exists in local directory
            if not self.check_file_exists(file['key']):
                # download the file while showing progress bar
                try:
                    self.s3.Bucket(self.bucket_url).download_file(file['key'], os.path.join(self.local_dir, file['key']), Callback=self.show_progress_bar([file], files_downloaded))
                    files_downloaded.append(file['key'])
                except:
                    self.logger.error(f'File {file["key"]} download failed')
                    raise Exception('File download failed')

        return files_downloaded


    # helper method to log download details
    def log_download_details(self, files_downloaded, bucket_files):
        """
        - log download details
        - return the list of files downloaded
        """
        # log download details
        self.logger.info(f'Downloaded {len(files_downloaded)} files')
        self.logger.info(f'Total size of files downloaded: {self.get_cumulative_size(files_downloaded)}')
        self.logger.info(f'Total size of files in bucket: {self.get_cumulative_size(bucket_files)}')
        self.logger.info(f'Available space in local directory: {self.get_available_space()}')

        return files_downloaded


    # main method to download files
    def download_files_to_local_directory(self):
        """
        - download files to local directory
        - return the list of files downloaded
        """
        if self.validate_bucket_url(self.bucket_url):
            # get the list of files in the bucket
            bucket_files = self.get_bucket_files(self.bucket_url, self.n)

            # get cumulative size of files
            cumulative_size = self.get_cumulative_size(bucket_files)

            # check if available space is sufficient
            if self.check_available_space(cumulative_size):
                # download files to local directory
                files_downloaded = self.download_files(bucket_files)

                # show progress bar for downloads
                self.show_progress_bar(bucket_files, files_downloaded)

                # log download details
                self.log_download_details(files_downloaded, bucket_files)

                return files_downloaded
            else:
                return []


if __name__=="__main__":
    # take input from command line arguments
    bucket_url = sys.argv[0]
    n = sys.argv[1]

    # create object of class
    s3_downloader = S3Downloader(bucket_url, n)

    # download files to local directory
    s3_downloader.download_files_to_local_directory()