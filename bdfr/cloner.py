#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
from collections.abc import Iterable
from time import sleep

import praw.exceptions
import praw.models
import prawcore

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
            try:
                for submission in generator:
                    try:
                        self._download_submission(submission)
                        self.write_entry(submission)
                    except prawcore.PrawcoreException as e:
                        logger.error(f"Submission {submission.id} failed to be cloned due to a PRAW exception: {e}")
                    except praw.exceptions.ClientException as e:
                        if isinstance(submission, praw.models.Comment):
                            logger.error(
                                f"Comment {submission.id} of Submission {submission.submission.id} "
                                f"failed to be cloned due to a PRAW exception: {e}"
                            )
                        else:
                            logger.error(f"Submission {submission.id} failed to be cloned due to a PRAW exception: {e}")
            except prawcore.PrawcoreException as e:
                if submission:
                    logger.error(
                        f"The submission after {submission.id} failed to download due to a PRAW exception: {e}"
                    )
                else:
                    logger.error(f"The first submission failed to download due to a PRAW exception: {e}")
                logger.debug("Waiting 60 seconds to continue")
                sleep(60)
