#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
from collections.abc import Iterable
from time import sleep

import praw.exceptions
import praw.models
import prawcore.exceptions

from bdfr.archiver import Archiver
from bdfr.configuration import Configuration
from bdfr.downloader import RedditDownloader

logger = logging.getLogger(__name__)


class RedditCloner(RedditDownloader, Archiver):
    def __init__(self, args: Configuration, logging_handlers: Iterable[logging.Handler] = ()):
        super(RedditCloner, self).__init__(args, logging_handlers)

    def download(self):
        for generator in self.reddit_lists:
            submission = None
            retry = 0
            max_retries = 3
            try:
                for submission in generator:
                    while retry < max_retries:
                        try:
                            self._download_submission(submission)
                            self.write_entry(submission)
                        except prawcore.exceptions.TooManyRequests:
                            if retry < max_retries:
                                retry += 1
                                logger.debug(f"Received TooManyRequest 429 HTTP response. Waiting {30 * (retry**3)}s before retrying")
                                sleep(30 * (retry**3))
                                continue
                            else:
                                if isinstance(submission, praw.models.Comment):
                                    logger.error(
                                        f"Comment {submission.id} of Submission {submission.submission.id} "
                                        f"failed to be cloned due to to TooManyRequest 429 HTTP response"
                                    )
                                else:
                                    logger.error(
                                        f"Submission {submission.id} failed to be cloned due to TooManyRequest 429 HTTP response"
                                    )
                        except prawcore.PrawcoreException as e:
                            logger.error(
                                f"Submission {submission.id} failed to be cloned due to a PRAW exception: {e}"
                            )
                        except praw.exceptions.ClientException as e:
                            if isinstance(submission, praw.models.Comment):
                                logger.error(
                                    f"Comment {submission.id} of Submission {submission.submission.id} "
                                    f"failed to be cloned due to a PRAW exception: {e}"
                                )
                            else:
                                logger.error(
                                    f"Submission {submission.id} failed to be cloned due to a PRAW exception: {e}"
                                )
                        break
            except prawcore.exceptions.PrawcoreException as e:
                if submission:
                    logger.error(
                        f"The submission after {submission.id} failed to download due to a PRAW exception: {e}"
                    )
                else:
                    logger.error(
                        f"The first submission failed to download due to a PRAW exception: {e}"
                    )
                logger.debug("Waiting 60 seconds to continue")
                sleep(60)
