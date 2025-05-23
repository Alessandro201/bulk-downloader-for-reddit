#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import hashlib
import logging.handlers
import os
import time
from collections.abc import Iterable
from datetime import datetime
from multiprocessing import Pool
from pathlib import Path
from time import sleep

import praw
import praw.exceptions
import praw.models
import prawcore

from bdfr import exceptions as errors
from bdfr.configuration import Configuration
from bdfr.connector import RedditConnector
from bdfr.site_downloaders.download_factory import DownloadFactory

logger = logging.getLogger(__name__)


def _calc_hash(existing_file: Path):
    chunk_size = 1024 * 1024
    md5_hash = hashlib.md5()
    with existing_file.open("rb") as file:
        chunk = file.read(chunk_size)
        while chunk:
            md5_hash.update(chunk)
            chunk = file.read(chunk_size)
    file_hash = md5_hash.hexdigest()
    return existing_file, file_hash


class RedditDownloader(RedditConnector):
    def __init__(self, args: Configuration, logging_handlers: Iterable[logging.Handler] = ()):
        super(RedditDownloader, self).__init__(args, logging_handlers)
        if self.args.search_existing:
            self.master_hash_list = self.scan_existing_files(self.download_directory)

    def download(self):
        for generator in self.reddit_lists:
            submission = None
            try:
                for submission in generator:
                    try:
                        self._download_submission(submission)
                    except prawcore.PrawcoreException as e:
                        logger.error(f"Submission {submission.id} failed to download due to a PRAW exception: {e}")
            except prawcore.PrawcoreException as e:
                if submission:
                    logger.error(
                        f"The submission after {submission.id} failed to download due to a PRAW exception: {e}"
                    )
                else:
                    logger.error(f"The first submission failed to download due to a PRAW exception: {e}")
                logger.debug("Waiting 60 seconds to continue")
                sleep(60)

    def _download_submission(self, submission: praw.models.Submission):
        if submission.id in self.excluded_submission_ids:
            logger.debug(f"Object {submission.id} in exclusion list, skipping")
            return
        elif submission.subreddit.display_name.lower() in self.args.skip_subreddit:
            logger.debug(f"Submission {submission.id} in {submission.subreddit.display_name} in skip list")
            return
        elif (submission.author and submission.author.name in self.args.ignore_user) or (
            submission.author is None and "DELETED" in self.args.ignore_user
        ):
            logger.debug(
                f"Submission {submission.id} in {submission.subreddit.display_name} skipped"
                f' due to {submission.author.name if submission.author else "DELETED"} being an ignored user'
            )
            return
        elif self.args.min_score and submission.score < self.args.min_score:
            logger.debug(
                f"Submission {submission.id} filtered due to score {submission.score} < [{self.args.min_score}]"
            )
            return
        elif self.args.max_score and self.args.max_score < submission.score:
            logger.debug(
                f"Submission {submission.id} filtered due to score {submission.score} > [{self.args.max_score}]"
            )
            return
        elif (self.args.min_score_ratio and submission.upvote_ratio < self.args.min_score_ratio) or (
            self.args.max_score_ratio and self.args.max_score_ratio < submission.upvote_ratio
        ):
            logger.debug(f"Submission {submission.id} filtered due to score ratio ({submission.upvote_ratio})")
            return
        elif not isinstance(submission, praw.models.Submission):
            logger.warning(f"{submission.id} is not a submission")
            return
        elif not self.download_filter.check_url(submission.url):
            logger.debug(f"Submission {submission.id} filtered due to URL {submission.url}")
            return

        logger.debug(f"Attempting to download submission {submission.id}")
        try:
            downloader_class = DownloadFactory.pull_lever(submission.url)
            downloader = downloader_class(submission)
            logger.debug(f"Using {downloader_class.__name__} with url {submission.url}")
        except errors.NotADownloadableLinkError as e:
            logger.error(f"Could not download submission {submission.id}: {e}")
            return
        if downloader_class.__name__.lower() in self.args.disable_module:
            logger.debug(f"Submission {submission.id} skipped due to disabled module {downloader_class.__name__}")
            return
        try:
            content = downloader.find_resources(self.authenticator)
        except errors.SiteDownloaderError as e:
            logger.error(f"Site {downloader_class.__name__} failed to download submission {submission.id}: {e}")
            return
        except requests.exceptions.ChunkedEncodingError:
            raise errors.SiteDownloaderError("Failed to retrieve downloadable urls due to a network error")
        for destination, res in self.file_name_formatter.format_resource_paths(content, self.download_directory):
            if destination.exists():
                logger.debug(f"File {destination} from submission {submission.id} already exists, continuing")
                continue
            elif not self.download_filter.check_resource(res):
                logger.debug(f"Download filter removed {submission.id} file with URL {submission.url}")
                continue
            try:
                res.download({"max_wait_time": self.args.max_wait_time})
            except errors.BulkDownloaderException as e:
                logger.error(
                    f"Failed to download resource {res.url} in submission {submission.id} "
                    f"with downloader {downloader_class.__name__}: {e}"
                )
                return
            resource_hash = res.hash.hexdigest()
            destination.parent.mkdir(parents=True, exist_ok=True)
            if resource_hash in self.master_hash_list:
                if self.args.no_dupes:
                    logger.info(f"Resource hash {resource_hash} from submission {submission.id} downloaded elsewhere")
                    return
                elif self.args.make_hard_links:
                    try:
                        destination.hardlink_to(self.master_hash_list[resource_hash])
                    except AttributeError:
                        self.master_hash_list[resource_hash].link_to(destination)
                    logger.info(
                        f"Hard link made linking {destination} to {self.master_hash_list[resource_hash]}"
                        f" in submission {submission.id}"
                    )
                    return
            try:
                with destination.open("wb") as file:
                    file.write(res.content)
                logger.debug(f"Written file to {destination}")
            except OSError as e:
                logger.exception(e)
                logger.error(f"Failed to write file in submission {submission.id} to {destination}: {e}")
                return
            creation_time = time.mktime(datetime.fromtimestamp(submission.created_utc).timetuple())
            os.utime(destination, (creation_time, creation_time))
            self.master_hash_list[resource_hash] = destination
            logger.debug(f"Hash added to master list: {resource_hash}")
        logger.info(f"Downloaded submission {submission.id} from {submission.subreddit.display_name}")

    @staticmethod
    def scan_existing_files(directory: Path) -> dict[str, Path]:
        files = []
        for (dirpath, _dirnames, filenames) in os.walk(directory):
            files.extend([Path(dirpath, file) for file in filenames])
        logger.info(f"Calculating hashes for {len(files)} files")

        pool = Pool(15)
        results = pool.map(_calc_hash, files)
        pool.close()

        hash_list = {res[1]: res[0] for res in results}
        return hash_list
