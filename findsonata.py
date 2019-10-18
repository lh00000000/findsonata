import json
import os
from python_speech_features import mfcc
import datetime
from sklearn.metrics.pairwise import cosine_similarity
import dateutil
import scipy.io.wavfile as wav
import numpy as np
import s3wav
import db

import hashlib

from pymongo import MongoClient
import os
import logging

psf_logger = logging.getLogger("python_speech_features")
psf_logger.setLevel(logging.ERROR)

REPEAT_THRES = 0.9
AB_THRES = 0.6


def mean_normalized_mfcc(x, fs):
    mfcc_frames = mfcc(x, fs, nfft=512)
    mean_mfcc = np.mean(mfcc_frames, axis=0)
    normalized = mean_mfcc / np.sqrt(np.sum(np.power(mean_mfcc, 2)))
    return normalized


def push_mfcc(new_s3key):
    # download from s3
    s3wav.s3download(new_s3key)
    filename = new_s3key.split("/")[-1]
    filepath = os.path.join("wav", filename)
    fs, x = wav.read(filepath)
    mean_mfcc = mean_normalized_mfcc(x, fs)

    rec_id = hashlib.sha1(filename.encode("utf-8")).hexdigest()
    created_dt = dateutil.parser.parse(filename.replace(".wav", ""))

    db.new_clip_record(rec_id, created_dt, mean_mfcc)

    return rec_id


def push_new_sims(coll_id):
    right = db.recc(coll_id)
    left = [(l["_id"], l["mean_mfcc"]) for l in db.all_clips()]
    left_ids = [lid for lid, lmfcc in left]
    all_mfcc = np.array([lmfcc for lid, lmfcc in left])
    right_mfcc = np.transpose(np.array(right["mean_mfcc"]))
    sims = np.dot(all_mfcc, right_mfcc)

    bulk_result = db.bulk_write_sims(left_ids, coll_id, sims)
    return bulk_result


def sonata_search(new_clip_id):


    recap_b_id = new_clip_id
    recap_b = db.recc(recap_b_id)
    expo_b_ids = [bid for bid, sim in recap_b["sims"].items() if sim > REPEAT_THRES]
    for expo_b_id in expo_b_ids:
        expo_b = db.recc(expo_b_id)

        expo_a_ids = [aid for aid, sim in recap_b["sims"].items() if sim < AB_THRES]
        for expo_a_id in expo_a_ids:
            expo_a = db.recc(expo_a_id)
            if expo_a["created"] < expo_b["created"]:
                recap_a_ids = [
                    aid for aid, sim in expo_a["sims"].items() if sim > REPEAT_THRES
                ]
                for recap_a_id in recap_a_ids:
                    recap_a = db.recc(recap_a_id)
                    if (
                        expo_b["created"] < recap_a["created"]
                        and recap_a["created"] < recap_b["created"]
                    ):
                        dev_material = db.created_between(expo_b["created"], recap_a["created"])

                        if len(dev_material) > 3:
                            db.new_sonata([
                                        expo_a["created"],
                                        expo_b["created"],
                                        *[d["created"] for d in dev_material[:4]],
                                        recap_a["created"],
                                        recap_b["created"],
                                    ])

if __name__ == '__main__':
    since = db.get_since()
    for new_s3key in s3wav.s3newkeys(since):
        new_record_id = push_mfcc(new_s3key)
        push_new_sims(new_record_id)

        db.move_since(db.recc(new_record_id)["created"])

        sonata_search(new_record_id)