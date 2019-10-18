from pymongo import MongoClient, UpdateOne
import os


MONGO_PW = os.environ["MONGO_PW"]
MONGO_USER = os.environ["MONGO_USER"]
MONGO_HOST = "cluster-ttqqa.mongodb.net"
mc = MongoClient(
    "mongodb+srv://{}:{}@{}/test?retryWrites=true".format(
        MONGO_USER, MONGO_PW, MONGO_HOST
    )
)
clips_coll = mc["findsonata"]["clips"]


def get_since():
    return mc["findsonata"]["state"].find_one({"_id": "LAST"})["last_created"]


def move_since(new_since):
    return mc["findsonata"]["state"].update_one(
        {"_id": "LAST"}, {"$set": {"last_created": new_since}}
    )


def new_clip_record(rec_id, created_dt, mean_mfcc):
    return clips_coll.update_one(
        {"_id": rec_id},
        {"$set": {"created": created_dt, "mean_mfcc": list(mean_mfcc)}},
        upsert=True,
    )


def all_clips():
    return clips_coll.find()


def bulk_write_sims(left_ids, coll_id, sims):

    bulk_ops = [
        UpdateOne({"_id": left_ids[i]}, {"$set": {f"sims.{coll_id}": sim}})
        for i, sim in enumerate(sims)
    ] + [
        UpdateOne(
            {"_id": coll_id}, {"$set": {"sims": {left_ids[i]: sim for i, sim in enumerate(sims)}}}
        )
    ]
    return clips_coll.bulk_write(bulk_ops)


def recc(rid):
    return clips_coll.find_one({"_id": rid})


def created_between(start, end):
    return list(
        clips_coll.find({"created": {"$gt": start["created"], "$lt": end["created"]}})
    )


def new_sonata(clip_ids):
    mc["findsonata"]["sonatas"].insert_one({"ids": clip_ids})
